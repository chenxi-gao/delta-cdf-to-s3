from unittest.mock import MagicMock, patch
from air.outbound.commons.destination import DestinationType
from air.outbound.commons.destination_factory import DestinationFactory
import pytest


@patch('pyspark.sql.SparkSession.builder.getOrCreate', return_value=MagicMock())
@patch('air.outbound.destinations.sqlserver.SQLServerDestination')
def test_create_destination_sqlserver(mock_sqlserver_dest, mock_spark):
    mock_sqlserver_dest.return_value = MagicMock()

    auth_strategy = MagicMock()
    dest_factory = DestinationFactory()

    # Test SQL Server creation
    dest = dest_factory.create_destination(
        DestinationType.SQL_SERVER,
        auth_strategy,
        'test_jdbc_url',
        'test_target_schema',
        mock_spark
    )

    assert dest is not None
    mock_sqlserver_dest.assert_called_once_with(
        {
            "url": 'test_jdbc_url',
            "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "jar_path": "jars/mssql-jdbc-driver.jar"
        },
        auth_strategy,
        'test_target_schema',
        DestinationType.SQL_SERVER,
        mock_spark
    )


@patch('pyspark.sql.SparkSession.builder.getOrCreate', return_value=MagicMock())
def test_create_destination_unsupported(mock_spark):
    dest_factory = DestinationFactory()

    # Test unsupported destination type, expecting ValueError
    with pytest.raises(ValueError, match=r"Unsupported destination type: .*"):
        dest_factory.create_destination(
            'unsupported_destination',
            MagicMock(),
            'test_jdbc_url',
            'test_target_schema',
            mock_spark
        )
