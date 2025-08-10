from .destination import DestinationType
from .authentication import AuthenticationStrategy
from pyspark.sql import SparkSession


class DestinationFactory:
    @staticmethod
    def create_destination(
        dest_type: DestinationType, auth_strategy: AuthenticationStrategy, jdbc_url: str, target_schema: str,
        spark: SparkSession, **kwargs
    ):
        if dest_type == DestinationType.SQL_SERVER:
            from ..destinations.sqlserver import SQLServerDestination
            jdbc_props = {
                "url": jdbc_url,
                "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                "jar_path": "jars/mssql-jdbc-driver.jar"
            }
            return SQLServerDestination(jdbc_props, auth_strategy, target_schema, dest_type, spark)
        elif dest_type == DestinationType.S3:
            from ..destinations.s3 import S3Destination
            # For S3, do not use jdbc_url/jdbc_props; accept explicit s3_options via kwargs
            s3_options = kwargs.get("s3_options", {}) or {}
            return S3Destination({}, auth_strategy, target_schema, dest_type, spark, s3_options=s3_options)
        else:
            raise ValueError(f"Unsupported destination type: {dest_type}")
