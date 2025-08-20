import datetime
from abc import ABC, abstractmethod
from typing import Optional, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from .cdc_loader import CDCLoader
from .destination import DestinationType
from .load_status_delta import PipelineHelperOutbound


class DestinationObjectStorageBase(ABC):
    """Base class for object-storage destinations (e.g., S3).

    Provides common interfaces for incremental extraction by version,
    filename generation helpers, and status recording using version-based
    load status table. Subclasses must implement write logic.
    """

    def __init__(self, spark: SparkSession, destination_type: DestinationType, options: Optional[Dict] = None):
        self.spark = spark
        self.destination_type = destination_type
        self.options: Dict = options or {}
    # -----------------------------
    # Incremental extraction (version-based)
    # -----------------------------

    def process_table_changes_by_version(self, table_name: str, min_version: int, max_version: int) -> DataFrame:
        """Extract CDC changes for a Delta table between versions.

        :param table_name: The fully qualified Delta table name.
        :param min_version: Inclusive starting version.
        :param max_version: Inclusive ending version.
        :return: DataFrame of CDC changes between versions.
        """
        cdc_loader = CDCLoader(self.spark)
        return cdc_loader.process_table_changes_by_version(table_name, min_version, max_version)

    # -----------------------------
    # Write contract (to be implemented by subclasses)
    # -----------------------------
    @abstractmethod
    def write_data(self, df: DataFrame, output_filename: str, **kwargs) -> bool:
        """Write the extracted data to destination.

        :param df: DataFrame to write
        :param output_filename: Logical filename keyword used to build final filenames
        :return: True if write succeeded
        """
        raise NotImplementedError

    # -----------------------------
    # Filename helpers
    # -----------------------------

    def default_file_name(self, filename_keyword: str, suffix: Optional[str] = None) -> str:
        timestamp_str = datetime.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        base = f"{filename_keyword}_{timestamp_str}"
        return f"{base}_{suffix}" if suffix else base

    @abstractmethod
    def generate_file_name(
        self,
        filename_keyword: str,
        suffix: Optional[str] = None,
        base_params: Optional[Dict] = None,
        extra_params: Optional[Dict] = None,
    ) -> str:
        """Generate a destination-specific filename.

        Subclasses may leverage configured generators; base provides signature only.
        """
        raise NotImplementedError

    # -----------------------------
    # Status recording helpers (version-based)
    # -----------------------------
    def compute_last_processed_version(self, df: DataFrame) -> Optional[int]:
        """Compute the latest commit version from a CDC DataFrame.
        Returns latest_version which can be None if df is empty.
        """
        if df is None:
            return None
        if df.limit(1).count() == 0:
            return None

        agg_row = df.agg(
            F.max(F.col("_commit_version")).alias("last_processed_version"),
        ).collect()[0]

        return (
            agg_row["last_processed_version"]
            if agg_row and agg_row["last_processed_version"] is not None
            else None
        )

    def update_version_based_status(
        self,
        helper: PipelineHelperOutbound,
        table_name: str,
        last_processed_version: Optional[int],
        last_run_time: Optional[datetime.datetime],
    ) -> None:
        """Upsert the latest version and last_run_time to version-based status table.
        If last_processed_version is None, this is a no-op.
        """
        if last_processed_version is None:
            return
        helper.create_version_based_table()
        helper.insert_or_update_load_status_version_based(
            composite_keys=("table_name", "destination_type"),
            data={
                "table_name": table_name,
                "destination_type": self.destination_type.value,
                "last_run_time": last_run_time,
                "last_processed_version": int(last_processed_version),
            },
        )

    # -----------------------------
    # Orchestration (optional convenience)
    # -----------------------------
    def run_incremental_extract_and_write_by_version(
        self,
        source_table_name: str,
        output_filename: str,
        min_version: int,
        max_version: int,
        status_helper: PipelineHelperOutbound,
        write_kwargs: Optional[Dict] = None,
    ) -> bool:
        """Convenience method to extract CDC by version, write to destination,
        and record latest version in status table.
        """
        cdc_df = self.process_table_changes_by_version(source_table_name, min_version, max_version)
        write_ok = self.write_data(cdc_df, output_filename, **(write_kwargs or {}))
        last_processed_version = self.compute_last_processed_version(cdc_df)
        self.update_version_based_status(
            status_helper,
            table_name=output_filename,
            last_processed_version=last_processed_version,
            last_run_time=datetime.datetime.utcnow(),
        )
        return write_ok
