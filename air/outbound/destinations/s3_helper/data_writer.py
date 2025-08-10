import logging
from datetime import datetime
from pyspark.sql import SparkSession

from .access_checker import S3AccessChecker


logger = logging.getLogger(__name__)


class S3DataWriter:
    """S3 Data Writer with Unity Catalog Integration (cdf2 implementation)."""

    def __init__(self, check_access: bool = True):
        self.spark = SparkSession.builder.getOrCreate()
        self.supported_formats = ["json", "parquet", "csv"]
        self.check_access = check_access
        self.access_checker = S3AccessChecker(self.spark) if check_access else None

    def _validate_format(self, output_format: str) -> bool:
        if output_format.lower() not in self.supported_formats:
            logger.error(f"Unsupported format: {output_format}")
            logger.info(f"Supported formats: {', '.join(self.supported_formats)}")
            return False
        return True

    def _build_s3_path(self, s3_bucket: str, s3_prefix: str, file_name: str) -> str:
        if not s3_bucket.lower().startswith("s3://"):
            s3_bucket = f"s3://{s3_bucket}"
        s3_bucket = s3_bucket.rstrip("/")
        if s3_prefix:
            s3_prefix = s3_prefix.strip("/")
            return f"{s3_bucket}/{s3_prefix}/{file_name}"
        return f"{s3_bucket}/{file_name}"

    def _check_s3_access(self, s3_path: str) -> None:
        if not self.check_access or not self.access_checker:
            return
        logger.info("Checking S3 access permissions...")
        access_result = self.access_checker.check_path_access(s3_path)
        for recommendation in access_result['recommendations']:
            logger.info(recommendation)

    def write_to_s3(
        self,
        df,
        s3_bucket: str,
        s3_prefix: str,
        file_name: str,
        output_format: str = "parquet",
        mode: str = "overwrite",
        partition_by=None,
        coalesce: int | None = None,
        skip_access_check: bool = False,
        **kwargs,
    ) -> bool:
        try:
            if df is None:
                logger.error("DataFrame is None, cannot write")
                return False

            df_count = df.count()
            if df_count == 0:
                logger.info("DataFrame is empty, creating empty file")

            if not self._validate_format(output_format):
                return False

            output_format = output_format.lower()
            s3_path = self._build_s3_path(s3_bucket, s3_prefix, file_name)

            if not skip_access_check:
                self._check_s3_access(s3_path)

            logger.info("Writing DataFrame to S3...")
            logger.info(f"Path: {s3_path}")
            logger.info(f"Format: {output_format}")
            logger.info(f"Records: {df_count}")

            if coalesce and isinstance(coalesce, int) and coalesce > 0:
                df = df.coalesce(coalesce)
                logger.info(f"Coalesced to {coalesce} partition(s)")

            writer = df.write.mode(mode)

            if partition_by:
                if isinstance(partition_by, str):
                    partition_by = [partition_by]
                writer = writer.partitionBy(*partition_by)
                logger.info(f"Partitioned by: {', '.join(partition_by)}")

            if output_format == "json":
                json_options = {
                    "compression": kwargs.get("compression", None),
                    "dateFormat": kwargs.get("dateFormat", "yyyy-MM-dd"),
                    "timestampFormat": kwargs.get("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
                    "encoding": kwargs.get("encoding", "UTF-8"),
                }
                json_options = {k: v for k, v in json_options.items() if v is not None}
                writer.options(**json_options).json(s3_path)
            elif output_format == "parquet":
                parquet_options = {"compression": kwargs.get("compression", "snappy")}
                writer.options(**parquet_options).parquet(s3_path)
            elif output_format == "csv":
                csv_options = {
                    "header": kwargs.get("header", True),
                    "delimiter": kwargs.get("delimiter", ","),
                    "quote": kwargs.get("quote", '"'),
                    "escape": kwargs.get("escape", "\\"),
                    "nullValue": kwargs.get("nullValue", ""),
                    "encoding": kwargs.get("encoding", "UTF-8"),
                    "compression": kwargs.get("compression", None),
                }
                csv_options = {k: v for k, v in csv_options.items() if v is not None}
                writer.options(**csv_options).csv(s3_path)

            logger.info(f"Successfully wrote data to {s3_path}")
            self._show_write_summary(s3_path, output_format, df_count)
            return True
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error writing to S3: {error_msg}")
            return False

    def _show_write_summary(self, s3_path: str, format_type: str, record_count: int) -> None:
        logger.info(f"{'='*60}")
        logger.info(f"{'Write Summary':^60}")
        logger.info(f"{'='*60}")
        logger.info(f"Location  : {s3_path}")
        logger.info(f"Format    : {format_type}")
        logger.info(f"Records   : {record_count}")
        logger.info(
            f"Timestamp : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logger.info(f"{'='*60}")
