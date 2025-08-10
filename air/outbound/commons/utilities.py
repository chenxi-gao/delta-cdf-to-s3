from air.util import create_dbutils
from typing import Any
from pydantic.v1 import validator, BaseModel
from pyspark.sql.types import ArrayType, MapType, StructType, BinaryType, StringType
from pyspark.sql.functions import to_json, col, udf
from pyspark.sql import DataFrame
import base64


class OutboundUtils(BaseModel):
    dbutils: Any = None

    @validator('dbutils', pre=False, always=True)
    def set_dbutils(cls, v, values, **kwargs):
        if v is None:
            return create_dbutils()
        return v

    @staticmethod
    def get_secret(scope: str, key: str) -> str:
        instance = OutboundUtils()
        return OutboundUtils._secret(instance.dbutils, scope, key)

    @staticmethod
    def _secret(dbutils_instance, scope: str, key: str):
        return dbutils_instance.secrets.get(scope=scope, key=key)

    @staticmethod
    def serialize_complex_types(df: DataFrame) -> DataFrame:
        complex_cols = [
            field.name for field in df.schema if isinstance(field.dataType, (ArrayType, MapType, StructType))
        ]
        for col_name in complex_cols:
            df = df.withColumn(col_name, to_json(col(col_name)))
        return df

    @staticmethod
    def encode_to_base64(binary_data):
        if binary_data is not None:
            return base64.b64encode(binary_data).decode('utf-8')

    encode_base64_udf = udf(encode_to_base64, StringType())

    @staticmethod
    def serialize_binary_type_to_base64(df: DataFrame):
        for field in df.schema.fields:
            if isinstance(field.dataType, BinaryType):
                df = df.withColumn(field.name, OutboundUtils.encode_base64_udf(col(field.name)).cast(StringType()))
        return df

    @staticmethod
    def prepare_staging_data(df, serialize_complex_type: bool = True, serialize_binary_type: bool = False):
        for field in df.schema.fields:
            col_name = field.name
            data_type = field.dataType
            # Check if the column is complex
            if isinstance(data_type, (StructType, ArrayType, MapType)):
                # Use to_json to convert complex types to JSON strings
                if serialize_complex_type:
                    df = df.withColumn(col_name, to_json(col(col_name)))
                else:
                    continue
            elif isinstance(data_type, BinaryType):
                if serialize_binary_type:
                    df = OutboundUtils.serialize_binary_type_to_base64(df)
                else:
                    continue
            else:
                # Convert primitive types to string using cast
                df = df.withColumn(col_name, col(col_name).cast("string"))
        return df

    @staticmethod
    def standardize_name(name: str) -> str:
        return f'"{name.lower().strip()}"'
