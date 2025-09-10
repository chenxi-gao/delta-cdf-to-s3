import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils


class S3DataWriter:
    """S3 Data Writer with Unity Catalog Integration (cdf2 implementation)."""

    def __init__(self):
        self.spark = SparkSession.builder.getOrCreate()
        self.supported_formats = ["json", "parquet", "csv"]
        # Initialize DBUtils for Databricks filesystem operations (Databricks environment)
        self.dbutils = DBUtils(self.spark)

    def _validate_format(self, output_format: str) -> bool:
        if output_format.lower() not in self.supported_formats:
            logging.error(f"Unsupported format: {output_format}")
            logging.info(f"Supported formats: {', '.join(self.supported_formats)}")
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

    def write_to_s3(
        self,
        df,
        s3_bucket: str,
        s3_prefix: str,
        file_name: str,
        output_format: str = "parquet",
        mode: str = "overwrite",
        **kwargs,
    ) -> bool:
        try:
            if df is None:
                logging.error("DataFrame is None, cannot write")
                return False

            df_count = df.count()
            if df_count == 0:
                logging.info("DataFrame is empty, creating empty file")

            if not self._validate_format(output_format):
                return False

            output_format = output_format.lower()

            # Build final single-file path and a temporary directory for Spark writer
            final_file_name = f"{file_name}.{output_format}"
            final_file_path = self._build_s3_path(s3_bucket, s3_prefix, final_file_name)
            tmp_dir_name = f"_tmp_{file_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            tmp_output_dir = self._build_s3_path(s3_bucket, s3_prefix, tmp_dir_name)

            # Handle empty DataFrame by creating an empty file directly
            if df_count == 0:
                try:
                    if mode == "overwrite":
                        try:
                            self.dbutils.fs.rm(final_file_path, True)
                        except Exception:
                            pass
                    # Create empty object; dbutils.fs.put expects text, write nothing
                    self.dbutils.fs.put(final_file_path, "", True)
                    logging.info(f"Successfully created empty file at {final_file_path}")
                    self._show_write_summary(final_file_path, output_format, df_count)
                    return True
                except Exception as e:
                    logging.error(f"Failed to create empty file: {e}")
                    return False

            logging.info("Writing DataFrame to S3 via temporary directory for single-file output...")
            logging.info(f"Temp dir: {tmp_output_dir}")
            logging.info(f"Final file: {final_file_path}")
            logging.info(f"Format: {output_format}")
            logging.info(f"Records: {df_count}")

            # Force single output file
            df = df.coalesce(1)

            writer = df.write.mode(mode)

            # Format-specific options
            if output_format == "json":
                json_options = {
                    "compression": kwargs.get("compression", None),
                    "dateFormat": kwargs.get("dateFormat", "yyyy-MM-dd"),
                    "timestampFormat": kwargs.get("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZ"),
                    "encoding": kwargs.get("encoding", "UTF-8"),
                }
                json_options = {k: v for k, v in json_options.items() if v is not None}
                writer.options(**json_options).json(tmp_output_dir)
            elif output_format == "parquet":
                parquet_options = {"compression": kwargs.get("compression", "snappy")}
                writer.options(**parquet_options).parquet(tmp_output_dir)
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
                writer.options(**csv_options).csv(tmp_output_dir)

            # Find the single part file produced by Spark
            tmp_files = self.dbutils.fs.ls(tmp_output_dir)
            part_files = [f.path for f in tmp_files if f.name.startswith("part-")]
            if len(part_files) == 0:
                logging.error("No part file found in temporary output directory. Nothing to move.")
                # Cleanup temp dir
                try:
                    self.dbutils.fs.rm(tmp_output_dir, True)
                except Exception:
                    pass
                return False
            if len(part_files) > 1:
                logging.warning("Multiple part files found; proceeding with the first one due to coalesce(1)")
            src_part_file = part_files[0]

            # Overwrite behavior on final single file
            if mode == "overwrite":
                try:
                    self.dbutils.fs.rm(final_file_path, True)
                except Exception:
                    pass

            # Move and rename to final destination
            self.dbutils.fs.mv(src_part_file, final_file_path)

            # Remove temporary directory
            try:
                self.dbutils.fs.rm(tmp_output_dir, True)
            except Exception as e:
                logging.warning(f"Failed to remove temporary directory {tmp_output_dir}: {e}")

            logging.info(f"Successfully wrote single file to {final_file_path}")
            self._show_write_summary(final_file_path, output_format, df_count)
            return True
        except Exception as e:
            error_msg = str(e)
            logging.error(f"Error writing to S3: {error_msg}")
            return False

    def _show_write_summary(self, s3_path: str, format_type: str, record_count: int) -> None:
        logging.info(f"{'='*60}")
        logging.info(f"{'Write Summary':^60}")
        logging.info(f"{'='*60}")
        logging.info(f"Location  : {s3_path}")
        logging.info(f"Format    : {format_type}")
        logging.info(f"Records   : {record_count}")
        logging.info(
            f"Timestamp : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        logging.info(f"{'='*60}")
