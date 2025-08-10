import pytest
from unittest.mock import patch
from air.outbound.commons.cdc_loader import CDCLoader
from air.outbound.commons.exceptions import CDCReadException  # adjust import path accordingly
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.master("local[1]").appName("CDCLoaderTest").getOrCreate()


def test_process_table_changes_raises_cdc_exception_on_read_failure(spark):
    loader = CDCLoader(spark)

    # Patch the read_changes method to raise CDCReadException
    with patch.object(loader.cdc_reader, 'read_changes', side_effect=CDCReadException("Simulated read failure")):
        with pytest.raises(CDCReadException, match="Simulated read failure"):
            loader.process_table_changes(
                table_name="test_table",
                min_ts="2024-01-01T00:00:00.000Z",
                max_ts="2024-01-02T00:00:00.000Z",
                cdf_history_query_partition=1,
                merge_keys=["id"]
            )
