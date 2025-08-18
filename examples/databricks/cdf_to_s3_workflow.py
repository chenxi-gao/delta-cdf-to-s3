import os
import logging
from datetime import datetime
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
from new_air.outbound.destinations.s3_helper.data_writer import S3DataWriter
from new_air.outbound.destinations.s3_helper.filename_generators import (
    FileNameGeneratorFactory,
)

# flake8: noqa: F821

# ----------------------------
# Required Parameters
# ----------------------------
# Table Configuration
CATALOG_NAME = "abacus_dev_catalog"
SCHEMA_NAME = "bronze"
TABLE_NAME = "cdf_test_demo_table"
MERGE_KEY = "id"

# S3 Configuration
S3_BUCKET_KEYWORD = "data-lake"
S3_PREFIX = "cdf-process-test/oynx-reports"
S3_FILENAME_GENERATOR = "oynx_report"
S3_OUTPUT_FORMAT = "parquet"

# Optional tuning
CDF_HISTORY_QUERY_PARTITION = 200


def _qualified_table_name(catalog: str, schema: str, table: str) -> str:
    return f"{catalog}.{schema}.{table}"


def _table_name_only(full_table: str) -> str:
    return full_table.split(".")[-1]


def _get_commit_time_window(
    spark: SparkSession, qualified_table: str
) -> Tuple[Optional[datetime], Optional[datetime]]:
    """Return (min_timestamp, max_timestamp) from DESCRIBE HISTORY."""
    history_df: DataFrame = spark.sql(f"DESCRIBE HISTORY {qualified_table}")
    if history_df.head(1):
        row = history_df.agg(
            F.min(F.col("timestamp")).alias("min_ts"),
            F.max(F.col("timestamp")).alias("max_ts"),
        ).collect()[0]
        return row["min_ts"], row["max_ts"]
    return None, None


def _get_previous_latest_timestamp(
    helper: PipelineHelperOutbound, source_table_name_only: str
) -> Optional[datetime]:
    """Fetch prior latest_updated_timestamp for this table and S3 destination."""
    status_df = helper.get_load_status_data()
    filtered = (
        status_df.where(F.col("table_name") == F.lit(source_table_name_only))
        .where(F.col("destination_type") == F.lit(DestinationType.S3.value))
        .select("latest_updated_timestamp")
        .limit(1)
        .collect()
    )
    if filtered:
        return filtered[0]["latest_updated_timestamp"]
    return None


def _fmt_ts(ts: datetime) -> str:
    # Delta change feed accepts standard timestamp strings
    return ts.strftime("%Y-%m-%d %H:%M:%S")


def run_workflow():
    logging.basicConfig(level=logging.INFO)

    # Spark session
    spark = SparkSession.builder.getOrCreate()

    # Propagate catalog for load status helper
    os.environ["CATALOG_NAME"] = CATALOG_NAME

    # Build names
    qualified_source_table = _qualified_table_name(CATALOG_NAME, SCHEMA_NAME, TABLE_NAME)
    source_table_only = _table_name_only(qualified_source_table)

    # Prepare load status helper by layer (schema aligned)
    layer = PipelineLayerOutbound[SCHEMA_NAME]
    helper = PipelineHelperOutbound(layer=layer)
    helper.create_table()

    # Compute min/max timestamp bounds
    prev_latest_ts = _get_previous_latest_timestamp(helper, source_table_only)
    if prev_latest_ts is None:
        min_ts, max_ts = _get_commit_time_window(spark, qualified_source_table)
        if min_ts is None or max_ts is None:
            logging.info("No history found for source table; nothing to process.")
            return
    else:
        # Use prior latest as lower bound; upper bound is current latest commit
        _, max_ts = _get_commit_time_window(spark, qualified_source_table)
        if max_ts is None:
            logging.info("No new commits detected; nothing to process.")
            return
        min_ts = prev_latest_ts

    # Prepare CDC loader
    cdc_loader = CDCLoader(spark)
    try:
        staging_df: DataFrame = cdc_loader.process_table_changes(
            table_name=qualified_source_table,
            min_ts=_fmt_ts(min_ts),
            max_ts=_fmt_ts(max_ts),
            cdf_history_query_partition=CDF_HISTORY_QUERY_PARTITION,
            merge_keys=[MERGE_KEY],
        )
    except Exception as e:
        logging.error(f"CDC extraction failed: {e}")
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

    # Build file name via generator
    try:
        generator = FileNameGeneratorFactory.create_generator(S3_FILENAME_GENERATOR)
        file_name = generator.generate_filename(
            {"start_date": min_ts, "end_date": max_ts}, environment=environment
        )
    except Exception as e:
        logging.error(f"Failed to generate file name: {e}")
        return

    # Write to S3
    writer = S3DataWriter(check_access=True)
    write_ok = writer.write_to_s3(
        df=staging_df,
        s3_bucket=s3_bucket,
        s3_prefix=S3_PREFIX,
        file_name=file_name,
        output_format=S3_OUTPUT_FORMAT,
        mode="append",
        skip_access_check=False,
    )

    # Update status only on success
    if write_ok:
        try:
            helper.insert_or_update_load_status(
                composite_keys=("table_name", "destination_type"),
                data={
                    "table_name": source_table_only,
                    "latest_updated_timestamp": max_ts,
                    "destination_type": DestinationType.S3.value,
                },
            )
            logging.info("Load status updated successfully.")
        except Exception as e:
            logging.error(f"Failed to update load status: {e}")
    else:
        logging.error("Write to S3 failed; load status not updated.")


if __name__ == "__main__":
    run_workflow()
