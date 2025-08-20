import base64
import functools
import json
import logging
import os
from decimal import Context, Decimal
from deprecated import deprecated
from enum import Enum
from pathlib import Path
from typing import Dict, List, Union, Any, Tuple, Optional

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import Column, Row, DataFrame, SparkSession
from pyspark.sql.functions import col, lit, coalesce, udf
from databricks.sdk import WorkspaceClient
from smart_open import open as smart_open
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

logger = logging.getLogger(__name__)


@deprecated(version='0.15.31',
            action='always',
            reason="The 'get_secret' function is deprecated. Please use Databricks secrets instead"
            )
def get_secret(secret: str, region: str = 'us-east-1') -> Dict[str, str]:
    session = boto3.session.Session()  # type: ignore
    client = session.client('secretsmanager', region_name=region)
    try:
        response = client.get_secret_value(SecretId=secret)
        return json.loads(response['SecretString'])
    except ClientError as e:
        raise RuntimeError(f'boto3 client error in get_secret_details: {str(e)}')
    except Exception as e:
        raise RuntimeError(f'Unexpected error in get_secret_details: {str(e)}')


@deprecated(version='0.15.31',
            action='always',
            reason="The 'get_secret_value' function is deprecated. Please use Databricks secrets instead"
            )
def get_secret_value(secret: str, key: str, region: str = 'us-east-1', decode: bool = False) -> str:
    detl = get_secret(secret, region)
    val = detl[key]
    if decode:
        return base64.b64decode(val).decode()
    else:
        return val


# NG Job Root functions for improving usability of Git-based jobs in Databricks
def find_ng_job_root() -> Path:
    cur = Path(os.getcwd())
    last = None
    while cur != last:
        if '.ng-job-root' in [p.stem for p in cur.glob('.ng-*')]:
            return cur
        last = cur
        cur = cur.parent
    return Path(os.getcwd())


def set_ng_job_root(print_path: bool = True) -> None:
    job_root = find_ng_job_root().as_posix()
    os.chdir(job_root)
    if print_path:
        print(f'Setting CWD to {job_root}')
    os.environ['NG_JOB_ROOT'] = job_root


def build_match_expr(columns: List[str], nullable_columns: bool = False) -> Column:
    condition_list = [
        (col(f'tgt.{c}') == col(f'src.{c}')) if not nullable_columns else
        (
                coalesce((col(f'tgt.{c}') == col(f'src.{c}')), lit(False))
                | (col(f'tgt.{c}').isNull() & col(f'src.{c}').isNull())
        )
        for c in columns
    ]
    return functools.reduce(lambda x, y: x & y, condition_list)


def build_not_match_expr(columns: List[str], nullable_columns: bool = False) -> Column:
    return ~build_match_expr(columns, nullable_columns)


def detect_updated_columns(left_row: Row, right_row: Row) -> Union[str, None]:
    if right_row['_exist_flag'] is None:
        return None
    left = left_row.asDict()
    right = right_row.asDict()
    diff = sorted([c for c in left.keys() if left[c] != right[c]])
    return ','.join(diff) or None


# UserDefinedFunctionLike type is not exposed, so use Any instead
def get_binary_to_decimal_udf(precision: int, scale: int, signed: bool = True) -> Any:
    def _get_decimal(binary):
        if binary is None:
            return None
        # Spark automatically decodes prior to sending data to the UDF
        return convert_binary_to_decimal(binary, precision, scale, decode=False, signed=signed)

    return udf(_get_decimal)


def get_decimal_to_binary_udf(scale: int, signed: bool = True) -> Any:
    def _get_binary(d):
        if d is None:
            return None
        return convert_decimal_to_binary(d, scale, decode=True, signed=signed)

    return udf(_get_binary, returnType='string')


detect_updated_columns_udf = udf(detect_updated_columns)


def schema_union(left: DataFrame, right: DataFrame) -> Tuple[DataFrame, DataFrame]:
    left_cols = {f.name.lower(): f for f in left.schema}
    right_cols = {f.name.lower(): f for f in right.schema}

    add_to_left = [
        lit(None).cast(f.dataType).alias(fname)
        for fname, f in right_cols.items()
        if fname not in left_cols
    ]

    add_to_right = [
        lit(None).cast(f.dataType).alias(fname)
        for fname, f in left_cols.items()
        if fname not in right_cols
    ]

    expanded_left = left.select(col('*'), *add_to_left)
    expanded_right = (
        right
        .select(col('*'), *add_to_right)
        .select(*expanded_left.columns)
    )

    return (expanded_left, expanded_right)


def convert_binary_to_decimal(val: bytes, precision: int, scale: int, decode=True, signed=True) -> Decimal:
    ctx = Context()
    ctx.prec = precision
    binary_rep = base64.b64decode(val) if decode else val
    return ctx.create_decimal(
        int.from_bytes(binary_rep, byteorder='big', signed=signed)
    ) / 10 ** scale


def convert_decimal_to_binary(val: Decimal, scale: int, decode: bool = False, signed=True) -> Union[bytes, str]:
    int_rep = int(val * (Decimal('10') ** Decimal(str(scale))))
    int_length = int(int_rep.bit_length() / 8 + 1)
    binary_rep = int_rep.to_bytes(length=int_length, byteorder='big', signed=signed)
    b64_rep = base64.b64encode(binary_rep)
    return b64_rep.decode() if decode else b64_rep


def create_dbutils(spark: Optional[SparkSession] = None) -> Any:
    """
    Creates a `pyspark.dbutils.DBUtils` instance.
    :param spark: An optional SparkSession. If provided, the DBUtils object will be created using this session.
    Otherwise, the DBUtils object will use the SparkSession returned by `SparkSession.builder.getOrCreate()`
    :return: the DBUtils object
    """
    # pyspark.dbutils doesn't exist in Apache PySpark, so your IDE might flag an error on this line.
    # pyspark.dbutils will be present when we run this in Databricks. Unit tests mock up the pyspark.dbutils
    # module
    from pyspark.dbutils import DBUtils
    if spark is None:
        spark = SparkSession.builder.getOrCreate()
    return DBUtils(spark)


UNSET = object()


def get_task_value(dbutils: Any, *, taskKey: str, key: str, debugValue=UNSET, **kwargs) -> str:
    """
    Gets the value of a Databricks taskValue that was set by an upstream task.

    The `debugValue` parameter is set to a sentinel value (`UNSET`) by default. If `dbutils.jobs.taskValues.get()`
    returns the sentinel value, `_get_task_value` raises a `ValueError`.

    :param dbutils: the DBUtils object to use to get the taskValue
    :param taskKey: the name of the task that created the taskValue
    :param key: the name of the taskValue
    :param debugValue: the value of the debugValue argument for `dbutils.jobs.taskValues.get`. Defaults to the  sentinel
    value `UNSET`
    :param kwargs: Additional keyword arguments to pass to `dbutils.jobs.taskValues.get()`
    :return: the taskValues's string value
    :raise ValueError: If `dbutils.jobs.taskValues.get` returns the sentinel value `UNSET`
    """
    logger.info(f"Reading taskValue with taskKey='{taskKey}' and key='{key}'")
    val = dbutils.jobs.taskValues.get(taskKey=taskKey, key=key, debugValue=debugValue, **kwargs)
    if val is UNSET:
        raise ValueError(f"taskValue(taskKey='{taskKey}', key='{key}') does not exist")
    return val


def get_workspace_sdk():
    dbutils = create_dbutils()
    ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    api_token = ctx.apiToken().get()
    api_url = ctx.apiUrl().get()
    return WorkspaceClient(host=api_url, token=api_token)


def get_transport_params(uri):
    if uri.startswith("azure://"):
        credential = DefaultAzureCredential()
        data_lake_storage_name = os.environ["DATA_LAKE_STORAGE_NAME"]

        blob_service_client = BlobServiceClient(
            account_url=f"https://{data_lake_storage_name}.blob.core.windows.net",
            credential=credential
        )

        return {"client": blob_service_client}

    return None


def custom_smart_open(uri, mode="r"):
    transport_params = get_transport_params(uri)

    return smart_open(uri, mode, transport_params=transport_params)


class CloudProviderType(str, Enum):
    AWS = 'aws'
    AZURE = 'azure'
    GCP = 'gcp'


def detect_cloud_provider() -> CloudProviderType:
    workspace_client_config = get_workspace_sdk().config

    if workspace_client_config.is_aws:
        return CloudProviderType.AWS
    elif workspace_client_config.is_azure:
        return CloudProviderType.AZURE
    elif workspace_client_config.is_gcp:
        return CloudProviderType.GCP
    else:
        raise ValueError(f"Could not identify the cloud provider for {workspace_client_config.host}")
