import unittest
from unittest.mock import patch, MagicMock
import pyspark.sql.types as T
from pyspark.sql import DataFrame
from air.outbound.destinations.sqlserver import SQLServerDestination
from air.outbound.commons.authentication import AuthenticationStrategy
from air.outbound.commons.exceptions import SyncFailure
from tenacity import RetryError
import jaydebeapi


class TestSQLServerDestination(unittest.TestCase):

    @patch('jaydebeapi.connect')
    @patch('pyspark.sql.SparkSession.builder.getOrCreate', return_value=MagicMock())
    def setUp(self, mock_spark, mock_connect):
        self.jdbc_props = {
            "url": "jdbc:sqlserver://localhost:1433;databaseName=testdb",
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "jar_path": "jars/mssql-jdbc-driver.jar"
        }
        self.auth_strategy = MagicMock(spec=AuthenticationStrategy)
        self.destination = SQLServerDestination(self.jdbc_props, self.auth_strategy, 'dbo', 'sqlserver', False)

    def test_connect_method_retries(self):
        with patch(
            'jaydebeapi.connect',
            side_effect=[
                jaydebeapi.DatabaseError(),
                jaydebeapi.DatabaseError(),
                MagicMock()
            ]
        ) as mock_connect:
            self.destination._connect()
            self.assertEqual(mock_connect.call_count, 3)

    def test_source_target_mapping(self):
        self.assertEqual(self.destination._source_target_mapping(T.IntegerType()), "INT")

    def test_push_historical_load_retries_on_failure(self):
        mock_df = MagicMock(spec=DataFrame)
        with patch.object(self.destination, '_push_historical_load', side_effect=SyncFailure("Test Failure")):
            with self.assertRaises(SyncFailure):
                self.destination._push_historical_load(mock_df, "test_table")

    @patch.object(SQLServerDestination, '_write_to_staging', return_value=True)
    @patch.object(SQLServerDestination, '_merge_to_target', return_value=True)
    def test_push_cdc_loads_successful(self, mock_merge, mock_write_staging):
        mock_df = MagicMock(spec=DataFrame)
        # Assume method returns True to indicate success
        self.destination._push_cdc_loads(
            mock_df, 'test_table', 's.id = t.id', ['col1', 'col2'], 't.col1 = s.col1'
        )
        mock_write_staging.assert_called_once()
        mock_merge.assert_called_once()

    def test_push_cdc_loads_throws_sync_failure(self):
        mock_df = MagicMock(spec=DataFrame)
        with patch.object(self.destination, '_write_to_staging', side_effect=Exception("Write failure")):
            with self.assertRaises(SyncFailure):
                self.destination._push_cdc_loads(
                    mock_df,
                    'test_table',
                    's.id = t.id',
                    ['col1', 'col2'],
                    't.col1 = s.col1'
                )

    def test_connection_failure_logs_and_raises(self):
        with patch('jaydebeapi.connect', side_effect=[jaydebeapi.DatabaseError()] * 3) as mock_connect:
            with self.assertLogs(level='WARNING'):
                with self.assertRaises(RetryError) as cm:
                    self.destination._connect()
                    self.assertIsInstance(cm.exception.last_attempt.exception(), jaydebeapi.DatabaseError)
                self.assertEqual(mock_connect.call_count, 3)
