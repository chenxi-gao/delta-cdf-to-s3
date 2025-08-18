import os
import logging
from datetime import datetime, date
from typing import Optional, Tuple

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from new_air.outbound.commons.cdc_loader import CDCLoader
from new_air.outbound.commons.load_status_delta import (
    PipelineHelperOutbound,
    PipelineLayerOutbound,
)
from new_air.outbound.commons.destination import DestinationType
from new_air.outbound.destinations.s3_helper.location_finder import LocationFinder
from new_air.outbound.destinations.s3 import S3Destination

# flake8: noqa: F821

# ----------------------------
# Required Parameters
# ----------------------------
# Table Configuration
CATALOG_NAME = "abacus_dev_catalog"
SCHEMA_NAME = "bronze"
TABLE_NAME = "cdf_test_demo_table"

# S3 Configuration
S3_BUCKET_KEYWORD = "data-lake"
S3_PREFIX = "cdf-process-test/oynx-reports"
S3_FILENAME_GENERATOR = "oynx_report"
S3_OUTPUT_FORMAT = "parquet"


def _qualified_table_name(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _table_name_only(full_table: str) -> str:
    return full_table.split(".")[-1]


def _get_version_bounds(
    spark: SparkSession, qualified_table: str
) -> Tuple[Optional[int], Optional[int]]:
    """Return (min_version, max_version) from DESCRIBE HISTORY."""
    history_df: DataFrame = spark.sql(f"DESCRIBE HISTORY {qualified_table}")
    if history_df.head(1):
        row = history_df.agg(
            F.min(F.col("version")).alias("min_ver"),
            F.max(F.col("version")).alias("max_ver"),
        ).collect()[0]
        min_ver = row["min_ver"]
        max_ver = row["max_ver"]
        return (int(min_ver) if min_ver is not None else None, int(max_ver) if max_ver is not None else None)
    return None, None


def _get_ts_bounds_for_versions(
    spark: SparkSession, qualified_table: str, min_version: int, max_version: int
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Return (min_timestamp, max_timestamp) for commits within [min_version, max_version]."""
    history_df: DataFrame = spark.sql(f"DESCRIBE HISTORY {qualified_table}")
    window_df = history_df.where(
        (F.col("version") >= F.lit(int(min_version))) & (F.col("version") <= F.lit(int(max_version)))
    )
    if window_df.head(1):
        row = window_df.agg(
            F.min(F.col("timestamp")).alias("min_ts"),
            F.max(F.col("timestamp")).alias("max_ts"),
        ).collect()[0]
        return row["min_ts"], row["max_ts"]
    return None, None


def _get_last_processed_version(
    helper: PipelineHelperOutbound, source_table_name_only: str
) -> Optional[int]:
    """Fetch last_processed_version for this table and S3 destination from version-based status table."""
    status_df = helper.get_load_status_data_version_based()
    filtered = (
        status_df.where(F.col("table_name") == F.lit(source_table_name_only))
        .where(F.col("destination_type") == F.lit(DestinationType.S3.value))
        .select("last_processed_version")
        .limit(1)
        .collect()
    )
    if filtered:
        last_processed_version = filtered[0]["last_processed_version"]
        return int(last_processed_version) if last_processed_version is not None else None
    return None


def _get_last_run_time(
    helper: PipelineHelperOutbound, source_table_name_only: str
) -> Optional[date]:
    """Fetch last_run_time from version-based status table and return as date (yyyy-mm-dd).
    If no record exists, return None (caller will compute from table history).
    """
    status_df = helper.get_load_status_data_version_based()
    filtered = (
        status_df.where(F.col("table_name") == F.lit(source_table_name_only))
        .where(F.col("destination_type") == F.lit(DestinationType.S3.value))
        .select("last_run_time")
        .limit(1)
        .collect()
    )
    if filtered and filtered[0]["last_run_time"] is not None:
        ts: datetime = filtered[0]["last_run_time"]
        return ts.date()
    return None


def run_workflow():
    logging.basicConfig(level=logging.INFO)

    # Spark session
    spark = SparkSession.builder.getOrCreate()

    # Propagate catalog for load status helper
    os.environ["CATALOG_NAME"] = CATALOG_NAME

    # Build names
    qualified_source_table = _qualified_table_name(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME)
    source_table_only = _table_name_only(qualified_source_table)

    # Prepare load status helper by layer (schema aligned) - version based
    layer = PipelineLayerOutbound[SCHEMA_NAME]
    helper = PipelineHelperOutbound(layer=layer)
    helper.create_version_based_table()

    # Compute version bounds and last_run_time/this_run_time
    last_processed_version_saved = _get_last_processed_version(helper, source_table_only)
    min_version: Optional[int]
    max_version: Optional[int]
    this_run_time: datetime = datetime.utcnow()
    last_run_date: Optional[date] = _get_last_run_time(helper, source_table_only)
    this_run_date: date = this_run_time.date()

    if last_processed_version_saved is None:
        min_version, max_version = _get_version_bounds(spark, qualified_source_table)
        if min_version is None or max_version is None:
            logging.info("No history found for source table; nothing to process.")
            return
        # If no last_run_time recorded, use earliest commit timestamp from history as last_run_date
        if last_run_date is None:
            min_ts, _ = _get_ts_bounds_for_versions(spark, qualified_source_table, int(min_version), int(max_version))
            last_run_date = (min_ts or datetime.utcnow()).date()
    else:
        # Use last processed + 1 as lower bound; upper bound is current latest commit version
        _, current_latest_version = _get_version_bounds(spark, qualified_source_table)
        if current_latest_version is None or int(current_latest_version) <= int(last_processed_version_saved):
            logging.info("No new versions detected; nothing to process.")
            return
        min_version = int(last_processed_version_saved) + 1
        max_version = int(current_latest_version)


    # Prepare CDC loader (version-based)
    cdc_loader = CDCLoader(spark)
    try:
        staging_df: DataFrame = cdc_loader.process_table_changes_by_version(
            table_name=qualified_source_table,
            min_version=int(min_version),
            max_version=int(max_version),
        )
    except Exception as e:
        logging.error(f"CDC extraction by version failed: {e}")
        return

    # Resolve S3 bucket and environment via LocationFinder (Databricks-only)
    try:
        loc_finder = LocationFinder(dbutils_instance=dbutils, spark_instance=spark)  # noqa: F821
        write_location = loc_finder.get_write_location_for_keyword(S3_BUCKET_KEYWORD)
        s3_bucket = write_location["bucket"]
        environment = write_location.get("environment") or loc_finder.get_environment()
    except Exception as e:
        logging.error(f"Failed to resolve S3 location: {e}")
        return

    # Instantiate S3Destination to handle filename generation and writes
    s3_url = f"s3://{s3_bucket}/{S3_PREFIX}" if S3_PREFIX else f"s3://{s3_bucket}"
    s3_dest = S3Destination(
        spark=spark,
        s3_options={
            "url": s3_url,
            "format": S3_OUTPUT_FORMAT,
            "filename_generator_type": S3_FILENAME_GENERATOR,
            "filename_generator_params": {"environment": environment},
        },
    )

    # Filename uses last_run_date (start) and this_run_date (end)
    base_params = {
        "start_date": last_run_date,
        "end_date": this_run_date,
    }
    write_ok = False
    try:
        write_ok = s3_dest.write_data(
            df=staging_df,
            output_filename=source_table_only,
            base_params=base_params,
            subdir="cdc",
            mode="append",
            skip_access_check=False,
        )
    except Exception as e:
        logging.error(f"Write to S3 via destination failed: {e}")
        write_ok = False

    # Update version-based status only on success
    if write_ok:
        try:
            last_processed_version = s3_dest.compute_last_processed_version(staging_df)
            s3_dest.update_version_based_status(
                helper,
                table_name=source_table_only,
                last_processed_version=last_processed_version,
                last_run_time=this_run_time,
            )
            logging.info("Version-based load status updated successfully.")
        except Exception as e:
            logging.error(f"Failed to update version-based load status: {e}")
    else:
        logging.error("Write to S3 failed; version-based load status not updated.")


if __name__ == "__main__":
    run_workflow()


