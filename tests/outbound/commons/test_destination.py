import unittest
from unittest.mock import MagicMock, patch
from pyspark.sql import DataFrame
from air.outbound.commons.destination import DestinationBase, DestinationType
from air.outbound.commons.utilities import OutboundUtils
from pyspark.sql.types import StructType, StructField, StringType


# Concrete class for testing abstract base class
class DestinationConcrete(DestinationBase):
    def _connect(self):
        return MagicMock()

    def _source_target_mapping(self, spark_type):
        return "dummy_target_type"

    def _target_schema_exists(self, create_on_failure: bool = True):
        return True

    def _destination_truncate(self, table_name: str):
        return MagicMock()

    def _drop_target_table_if_exists(self, qualified_target_table_name: str):
        return MagicMock()

    def _create_target_table_with_schema(self, table_schema: set, qualified_target_table_name: str):
        return MagicMock()

    def _push_historical_load(self, df: DataFrame, target_table_name: str) -> bool:
        return True

    def _push_cdc_loads(
        self, staging_df: DataFrame,
        target_table_name: str,
        join_condition: str,
        value_columns: list,
        update_columns: str
    ) -> bool:
        return True

    def _disable_auto_commit(self):
        return MagicMock()

    def _enable_auto_commit(self):
        return MagicMock()

    def _begin_transaction(self):
        return MagicMock()

    def _rollback(self):
        return MagicMock()

    def _commit(self):
        return MagicMock()


class DestinationBaseTest(unittest.TestCase):

    @patch('pyspark.sql.SparkSession.builder.getOrCreate', return_value=MagicMock())
    def setUp(self, mock_spark):
        jdbc_props = {"url": "test_jdbc_url", "driver": "test_driver", "jar_path": "test_jar_path"}
        auth_strategy = MagicMock()
        self.dest = DestinationConcrete(
            jdbc_props, auth_strategy, 'test_schema', DestinationType.SQL_SERVER, mock_spark
        )

    @patch.object(OutboundUtils, 'serialize_complex_types', return_value=MagicMock())
    @patch.object(OutboundUtils, 'serialize_binary_type_to_base64', return_value=MagicMock())
    @patch.object(DestinationConcrete, '_process_df_before_write', return_value=MagicMock())
    @patch.object(DestinationConcrete, '_create_table_with_schema_information', return_value=MagicMock())
    @patch.object(DestinationConcrete, '_commit', return_value=MagicMock())
    def test_prepare_for_historical_load(
        self,
        mock_commit,
        mock_create_table,
        mock_process_df,
        mock_serialize_binary,
        mock_serialize_complex
    ):
        # Mock data
        schema = StructType([StructField("col1", StringType(), True)])
        mock_df = MagicMock()
        mock_df.schema = schema
        mock_df = self.dest._prepare_for_historical_load(
            source_df=mock_df,
            source_table_name="source_table",
            target_table_name="target_table",
            create_table_auto=True,
            serialize_complex_type=True,
            serialize_binary_type=True
        )

        mock_create_table.assert_called_once()
        mock_commit.assert_called_once()
        mock_serialize_complex.assert_called_once()
        mock_serialize_binary.assert_called_once()
        mock_process_df.assert_called_once()

    @patch('pyspark.sql.SparkSession.builder.getOrCreate', return_value=MagicMock())
    @patch('air.outbound.commons.destination.CDCLoader')
    @patch.object(OutboundUtils, 'prepare_staging_data', return_value=MagicMock())
    @patch.object(DestinationConcrete, '_begin_transaction', return_value=MagicMock())
    @patch.object(DestinationConcrete, '_disable_auto_commit', return_value=MagicMock())
    @patch.object(DestinationConcrete, '_push_cdc_loads', return_value=True)
    @patch.object(DestinationConcrete, '_commit', return_value=MagicMock())
    def test_handle_all_cdc_loads(
        self, mock_commit, mock_push_cdc_loads, mock_disable_auto_commit,
        mock_begin_transaction, mock_prepare_staging_data, MockCDCLoader, mock_spark
    ):

        # Mock the CDC loader to process table changes.
        mock_cdc_loader_instance = MockCDCLoader.return_value
        mock_staging_df = MagicMock(count=lambda: 1000)
        mock_cdc_loader_instance.process_table_changes.return_value = mock_staging_df

        # Mock the Spark table method
        mock_spark_session = mock_spark.return_value
        mock_schema = [StructField("id", StringType(), True)]

        mock_table = MagicMock()
        mock_table.schema.fields = mock_schema
        mock_spark_session.table.return_value = mock_table

        # Mock DataFrame and Row
        mock_df = MagicMock()
        mock_df.count.return_value = 1
        mock_row = {"source_table": "source_table_name",
                    "target_table": "target_table_name",
                    "min_ts": "2023-01-01",
                    "max_ts": "2023-01-02",
                    "merge_keys": ["id"]}
        mock_df.collect.return_value = [mock_row]

        destination = DestinationConcrete({}, MagicMock(), 'test_schema', DestinationType.SQL_SERVER, mock_spark)
        destination.spark = mock_spark_session

        destination.handle_all_cdc_loads(
            table_info_df=mock_df,
            pipeline_helper=MagicMock(),
            cdf_history_query_partition=MagicMock(),
            rollback_on_error=True,
            serialize_complex_type=True,
            serialize_binary_type=False
        )

        # Assert the mocked methods were called as expected
        mock_disable_auto_commit.assert_called_once()
        mock_begin_transaction.assert_called_once()
        mock_push_cdc_loads.assert_called_once()
        mock_commit.assert_called_once()
        mock_prepare_staging_data.assert_called_once()
