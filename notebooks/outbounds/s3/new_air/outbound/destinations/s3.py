import logging
import re
from datetime import datetime
from typing import Optional, Dict
from pyspark.sql import DataFrame

from ..commons.destination_object_storage import DestinationObjectStorageBase
from ..commons.destination import DestinationType
from .s3_helper.data_writer import S3DataWriter
from .s3_helper.filename_generators import FileNameGeneratorFactory


class S3Destination(DestinationObjectStorageBase):
    """S3 destination for writing DataFrames to S3 object storage.

    - Inherits object-storage base class (no JDBC-specific concepts)
    - Supports incremental extraction by version and status recording
    """

    def __init__(
        self,
        spark,
        s3_options: Dict | None = None,
    ):
        super().__init__(spark=spark, destination_type=DestinationType.S3, options=s3_options or {})
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
            logging.warning(f"Filename generator failed: {e}. Falling back to default name.")
            return fallback_name

    # --- DestinationObjectStorageBase required implementations ---
    def write_data(self, df: DataFrame, output_filename: str, **kwargs) -> bool:
        if not self.s3_bucket:
            raise Exception("S3 bucket is not configured for writes.")

        effective_prefix_parts = [p for p in [self.s3_prefix, output_filename] if p]
        effective_prefix = "/".join(effective_prefix_parts)
        fallback_name = self.default_file_name(output_filename)

        base_params = kwargs.get("base_params") or {"start_date": datetime.utcnow(), "end_date": datetime.utcnow()}
        file_name = self._generate_file_name(base_params, self.file_generator_params, fallback_name)

        success = self.writer.write_to_s3(
            df=df,
            s3_bucket=self.s3_bucket,
            s3_prefix=effective_prefix,
            file_name=file_name,
            output_format=self.output_format,
            mode=kwargs.get("mode", "overwrite"),
            partition_by=self.partition_by,
            coalesce=self.coalesce
        )
        if not success:
            raise Exception("S3 write returned False")
        return True

    def generate_file_name(
        self,
        filename_keyword: str,
        suffix: Optional[str] = None,
        base_params: Optional[Dict] = None,
        extra_params: Optional[Dict] = None,
    ) -> str:
        fallback_name = self.default_file_name(filename_keyword, suffix)
        return self._generate_file_name(base_params or {}, extra_params or {}, fallback_name)
