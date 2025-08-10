from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, max as spark_max
from functools import reduce
import logging
from .exceptions import CDCReadException


# CDC Reader Class
class _CDCReader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_changes(
            self, table_name: str, min_ts: str, max_ts: str, cdf_history_query_partition
    ) -> DataFrame:
        try:
            df = self.spark.read.format("delta") \
                .option("readChangeFeed", "true") \
                .option('startingTimestamp', min_ts) \
                .option('endingTimestamp', max_ts) \
                .table(table_name) \
                .filter('_change_type != "update_preimage"') \
                .repartition(cdf_history_query_partition)
            return df
        except Exception as e:
            raise CDCReadException(f"Failed to read CDC changes for source table {table_name} - {e}")


class CDCLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.cdc_reader = _CDCReader(spark)

    def process_table_changes(
        self, table_name: str, min_ts: str, max_ts: str, cdf_history_query_partition,
        merge_keys: list
    ) -> DataFrame:
        """
        Processes CDC changes, deduplicating and preparing them for the load.

        :param table_name: The name of the Delta table.
        :param min_ts: The minimum timestamp to consider for CDC changes.
        :param max_ts: The maximum timestamp to consider for CDC changes.
        :param merge_keys: The list of keys used for merging records.
        :return: A DataFrame ready for merging/upserting into a destination.
        """
        try:
            # Read CDC changes
            logging.info(f"Reading changes for {table_name} from {min_ts} to {max_ts}")
            cdc_changes_df = self.cdc_reader.read_changes(table_name, min_ts, max_ts, cdf_history_query_partition)

            # Order changes and calculate the latest change per key
            ordered_cdc_changes_df = cdc_changes_df.orderBy(*merge_keys, "_commit_timestamp")
            max_commit_timestamp_df = ordered_cdc_changes_df.groupBy(*merge_keys).agg(
                spark_max("_commit_timestamp").alias("max_commit_timestamp"))

            # Join to get only latest record for each key
            join_conditions = [col(f"t.{key}") == col(f"s.{key}") for key in merge_keys]
            final_join_condition = reduce(lambda x, y: x & y, join_conditions)

            joined_data = ordered_cdc_changes_df.alias("t").join(
                max_commit_timestamp_df.alias("s"),
                final_join_condition & (col("t._commit_timestamp") == col("s.max_commit_timestamp")),
                "inner"
            )

            # Drop duplicate merge key columns
            for key in merge_keys:
                joined_data = joined_data.drop(col(f"s.{key}"))

            filtered_data = joined_data.filter(col("_commit_timestamp") == col("max_commit_timestamp"))
            staging_df = (
                filtered_data.filter(col("_change_type") == "delete")
                .union(filtered_data.filter(col("_change_type") == "insert"))
                .union(
                    filtered_data.filter((col("_change_type") == "update_postimage") | (col("_change_type") == "merge"))
                )
            )
            return staging_df
        except CDCReadException:
            raise
        except Exception as other_exceptions:
            raise Exception(f"{other_exceptions}")
