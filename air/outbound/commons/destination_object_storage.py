import datetime
import logging
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
    load status table. Subclasses must implement access checks and write logic.
    """

    def __init__(self, spark: SparkSession, destination_type: DestinationType, options: Optional[Dict] = None):
        self.spark = spark
        self.destination_type = destination_type
        self.options: Dict = options or {}
        self.logger = logging.getLogger(self.__class__.__name__)

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
    # Access check (to be implemented by subclasses)
    # -----------------------------
    @abstractmethod
    def access_check(self) -> Dict:
        """Check whether the destination is accessible.

        :return: A dictionary describing the accessibility status and any hints
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
    def compute_latest_version_and_timestamp(self, df: DataFrame) -> tuple[Optional[int], Optional[datetime.datetime]]:
        """Compute the latest commit version and timestamp from a CDC DataFrame.
        Returns (latest_version, latest_timestamp) which can be None if df is empty.
        """
        if df is None:
            return None, None
        # Avoid df.isEmpty for compatibility
        if df.limit(1).count() == 0:
            return None, None

        agg_row = df.agg(
            F.max(F.col("_commit_version")).alias("latest_version"),
            F.max(F.col("_commit_timestamp")).alias("latest_ts"),
        ).collect()[0]

        latest_version = agg_row["latest_version"] if agg_row and agg_row["latest_version"] is not None else None
        latest_ts = agg_row["latest_ts"] if agg_row and agg_row["latest_ts"] is not None else None
        return latest_version, latest_ts

    def update_version_based_status(
        self,
        helper: PipelineHelperOutbound,
        table_name: str,
        latest_version: Optional[int],
        latest_updated_timestamp: Optional[datetime.datetime],
    ) -> None:
        """Upsert the latest version/timestamp to version-based status table.
        If latest_version is None, this is a no-op.
        """
        if latest_version is None:
            return
        helper.create_version_based_table()
        helper.insert_or_update_load_status_version_based(
            composite_keys=("table_name", "destination_type"),
            data={
                "table_name": table_name,
                "destination_type": self.destination_type.value,
                "latest_updated_timestamp": latest_updated_timestamp,
                "latest_version": int(latest_version),
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
        latest_version, latest_ts = self.compute_latest_version_and_timestamp(cdc_df)
        self.update_version_based_status(
            status_helper,
            table_name=output_filename,
            latest_version=latest_version,
            latest_updated_timestamp=latest_ts,
        )
        return write_ok
