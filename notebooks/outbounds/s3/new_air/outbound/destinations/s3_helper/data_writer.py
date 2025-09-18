import logging
import os
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
        # Ensure /tmp exists and is writable before any local temp usage
        self._validate_tmp_dir()

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

    def _build_local_tmp_path(self, dir_name: str) -> str:
        clean_dir_name = dir_name.strip("/")
        return f"/tmp/{clean_dir_name}"

    def _validate_tmp_dir(self, tmp_dir: str = "/tmp") -> None:
        """Validate that tmp_dir exists and is writable; exit if not."""
        try:
            if not os.path.isdir(tmp_dir):
                logging.error(f"Temporary directory does not exist: {tmp_dir}")
                raise SystemExit(1)
            if not os.access(tmp_dir, os.W_OK | os.X_OK):
                logging.error(f"Temporary directory is not writable/executable: {tmp_dir}")
                raise SystemExit(1)
            logging.info(f"Temporary directory is available and writable: {tmp_dir}")
        except Exception as e:
            if isinstance(e, SystemExit):
                raise
            logging.error(f"Failed to validate temporary directory {tmp_dir}: {e}")
            raise SystemExit(1)

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

            # Drop metadata columns if present
            metadata_columns = ["_change_type", "_commit_version", "_commit_timestamp"]
            columns_to_drop = [c for c in metadata_columns if c in df.columns]
            if columns_to_drop:
                df = df.drop(*columns_to_drop)

            # Build final single-file path and a temporary directory for Spark writer
            final_file_name = f"{file_name}.{output_format}"
            final_file_path = self._build_s3_path(s3_bucket, s3_prefix, final_file_name)
            tmp_dir_name = f"_tmp_{file_name}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
            tmp_output_dir = self._build_local_tmp_path(tmp_dir_name)

            # Handle empty DataFrame
            if df_count == 0:
                try:
                    if mode == "overwrite":
                        try:
                            self.dbutils.fs.rm(final_file_path, True)
                        except Exception:
                            pass
                    # For JSON, write an empty array; for others, create empty file
                    if output_format == "json":
                        self.dbutils.fs.put(final_file_path, "[]", True)
                    else:
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
                # Stream out as a single JSON array with indentation and trailing commas
                def _to_array_lines(iterator):
                    try:
                        first = next(iterator)
                    except StopIteration:
                        yield "[]"
                        return
                    yield "["
                    prev = first
                    for current in iterator:
                        yield "  " + prev + ","
                        prev = current
                    yield "  " + prev
                    yield "]"

                # In PySpark, DataFrame.toJSON() returns an RDD[str], so do not access .rdd again
                json_lines_rdd = df.toJSON().coalesce(1).mapPartitions(_to_array_lines)
                json_lines_df = self.spark.createDataFrame(json_lines_rdd.map(lambda s: (s,)), ["value"])
                json_lines_df.write.mode(mode).text(tmp_output_dir)
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
