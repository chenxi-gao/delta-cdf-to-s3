import logging
import re
from datetime import datetime
from typing import Optional, Dict
from pyspark.sql import DataFrame

from ..commons.destination import DestinationBase
from .s3_helper.data_writer import S3DataWriter
from .s3_helper.filename_generators import FileNameGeneratorFactory


class S3Destination(DestinationBase):
    """Destination implementation for writing DataFrames to S3.

    This destination focuses on writing files to S3 buckets with optional
    filename generation rules. Transactional and DDL-related methods are
    implemented as no-ops.
    """

    def __init__(
        self,
        jdbc_props,
        auth_strategy,
        target_schema,
        destination_type,
        spark,
        s3_options: Dict | None = None,
    ):
        # Initialize base to keep common behaviors (e.g., metadata handling)
        # For S3 we do not use jdbc/auth values beyond base initialization
        super().__init__(jdbc_props, auth_strategy, target_schema, destination_type, spark)
        self.logger = logging.getLogger(self.__class__.__name__)
        self.writer = S3DataWriter()
        self.s3_options: Dict = s3_options or {}
        self.s3_bucket, self.s3_prefix = self._parse_s3_url(self.s3_options.get("url", ""))
        self.output_format = (self.s3_options.get("format") or "parquet").lower()
        self.partition_by = self._parse_partition_by(self.s3_options.get("partition_by"))
        self.coalesce = self._parse_int(self.s3_options.get("coalesce"), default_value=None)
        self.file_generator_type = self.s3_options.get("filename_generator_type")
        self.file_generator_params = self.s3_options.get("filename_generator_params", {}) or {}

    # --- Internal utilities ---
    def _parse_s3_url(self, url: str):
        if not url:
            return None, None
        if not url.lower().startswith("s3://"):
            # Accept raw bucket name as url
            return url.strip("/"), ""
        match = re.match(r"s3://([^/]+)/(.*)$", url)
        if match:
            bucket = match.group(1)
            prefix = match.group(2) or ""
            return bucket, prefix
        return url.replace("s3://", "").strip("/"), ""

    def _parse_int(self, value, default_value: Optional[int] = None) -> Optional[int]:
        try:
            return int(value) if value is not None else default_value
        except Exception:
            return default_value

    def _parse_partition_by(self, value) -> Optional[list]:
        if value is None:
            return None
        if isinstance(value, list):
            return value
        if isinstance(value, str) and value.strip():
            return [v.strip() for v in value.split(",") if v.strip()]
        return None

    def _default_file_name(self, target_table_name: str, suffix: Optional[str] = None) -> str:
        timestamp_str = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        base = f"{target_table_name}_{timestamp_str}"
        return f"{base}_{suffix}" if suffix else base

    def _generate_file_name(self, base_params: Dict, extra_params: Dict, fallback_name: str) -> str:
        if not self.file_generator_type:
            return fallback_name
        try:
            generator = FileNameGeneratorFactory.create_generator(self.file_generator_type)
            return generator.generate_filename(base_params, **(extra_params or {}))
        except Exception as e:
            self.logger.warning(f"Filename generator failed: {e}. Falling back to default name.")
            return fallback_name

    # --- DestinationBase required methods ---
    def _connect(self):
        # No external connection needed for S3 writes
        return None

    def _source_target_mapping(self, spark_type) -> str:
        # Not applicable for S3 file writes
        return ""

    def _target_schema_exists(self, create_on_failure: bool = True):
        # Not applicable for S3; schema concept is not used
        return None

    def _destination_truncate(self, table_name: str):
        # Not applicable for S3; writer mode controls overwrite behavior
        return None

    def _drop_target_table_if_exists(self, qualified_target_table_name: str):
        # Not applicable for S3
        return None

    def _create_target_table_with_schema(self, table_schema: set, qualified_target_table_name: str):
        # Not applicable for S3
        return None

    def _push_historical_load(self, df: DataFrame, target_table_name: str) -> bool:
        raise NotImplementedError("Historical full loads are not supported for S3 destination.")

    def _push_cdc_loads(
        self,
        staging_df: DataFrame,
        target_table_name: str,
        join_condition: str,
        value_columns: list,
        update_columns: str
    ) -> bool:
        # For S3, CDC merge is not applicable. We append a timestamped snapshot for traceability.
        if not self.s3_bucket:
            raise Exception("S3 bucket is not configured for CDC writes.")

        effective_prefix_parts = [p for p in [self.s3_prefix, self.target_schema, target_table_name, "cdc"] if p]
        effective_prefix = "/".join(effective_prefix_parts)
        fallback_name = self._default_file_name(target_table_name, suffix="cdc")
        base_params = {"start_date": datetime.utcnow(), "end_date": datetime.utcnow()}
        file_name = self._generate_file_name(base_params, self.file_generator_params, fallback_name)

        success = self.writer.write_to_s3(
            df=staging_df,
            s3_bucket=self.s3_bucket,
            s3_prefix=effective_prefix,
            file_name=file_name,
            output_format=self.output_format,
            mode="append",
            partition_by=self.partition_by,
            coalesce=self.coalesce,
            skip_access_check=True
        )
        if not success:
            raise Exception("CDC write returned False")
        return True

    def _disable_auto_commit(self):
        return None

    def _enable_auto_commit(self):
        return None

    def _begin_transaction(self):
        return None

    def _rollback(self):
        return None

    def _commit(self):
        return None

    # Explicitly disable historical loads entrypoint
    def handle_all_historical_loads(self, *args, **kwargs):
        raise NotImplementedError("Historical full loads are not supported for S3 destination.")
