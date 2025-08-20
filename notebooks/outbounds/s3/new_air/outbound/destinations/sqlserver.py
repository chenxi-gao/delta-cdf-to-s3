from ..commons.destination import DestinationBase
from pyspark.sql import DataFrame
import jaydebeapi
import logging
import pyspark.sql.types as T
from ..commons.exceptions import SyncFailure, TableException, SchemaNotFoundError, DatabaseOperationException
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential


# SQL Server Destination
class SQLServerDestination(DestinationBase):

    # Max length allowed for SQL Server Identifiers
    allowable_table_name_length = 128

    @retry(
        retry=retry_if_exception_type(jaydebeapi.DatabaseError),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=10)
    )
    def _connect(self):
        try:
            logging.info("Attempting to connect to SQL Server")
            self.conn = jaydebeapi.connect(
                self.jdbc_props['driver'],
                self.jdbc_props['url'],
                self.auth_strategy.get_connection_props_dict(),
                self.jdbc_props['jar_path']
            )
            logging.info("Connection to SQL Server Successful")
            return self.conn.cursor()
        except jaydebeapi.DatabaseError as e:
            logging.warning(f"Source Database error occurred. Error: {e}.")
            raise

    def _source_target_mapping(self, spark_type):
        if isinstance(spark_type, T.BooleanType):
            return "BIT"
        elif isinstance(spark_type, T.ByteType):
            return "TINYINT"
        elif isinstance(spark_type, T.ShortType):
            return "SMALLINT"
        elif isinstance(spark_type, T.IntegerType):
            return "INT"
        elif isinstance(spark_type, T.LongType):
            return "BIGINT"
        elif isinstance(spark_type, T.FloatType):
            return "REAL"
        elif isinstance(spark_type, T.DoubleType):
            return "FLOAT"
        elif isinstance(spark_type, T.DecimalType):
            return f"DECIMAL({spark_type.precision},{spark_type.scale})"
        elif isinstance(spark_type, T.StringType):
            return "VARCHAR(MAX)"
        elif isinstance(spark_type, T.BinaryType):
            return "VARBINARY(MAX)"
        elif isinstance(spark_type, T.DateType):
            return "DATE"
        elif isinstance(spark_type, T.TimestampType):
            return "DATETIMEOFFSET"
        elif isinstance(spark_type, T.TimestampNTZType):
            return "DATETIME2"
        elif isinstance(spark_type, (T.ArrayType, T.MapType, T.StructType)):
            return "NVARCHAR(MAX)"
        else:
            return "VARCHAR(MAX)"

    def _target_schema_exists(self, create_on_failure: bool = True):
        self.cursor.execute(
            f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{self.target_schema}'"
        )
        rows = self.cursor.fetchall()

        # If no rows are returned, then schema doesn't exist, so create it
        if len(rows) == 0:
            logging.info(f"Creating schema '{self.target_schema}' in SQL Server")
            if create_on_failure:
                self.cursor.execute(f"CREATE SCHEMA [{self.target_schema}]")
                self.conn.commit()
            else:
                raise SchemaNotFoundError(f"Cannot Sync. {self.target_schema} Schema doesn't exist")
        else:
            logging.info(f"Schema '{self.target_schema}' exists in SQL Server")

    def _destination_truncate(self, table_name: str):
        try:
            truncate_query = f"TRUNCATE TABLE {table_name}"
            self.cursor.execute(truncate_query)
            self.commit()
        except Exception as e:
            raise DatabaseOperationException(f"Truncate Failed {e}")

    def _drop_target_table_if_exists(self, qualified_target_table_name: str):

        try:
            exists_and_drop_sql = f"""
                IF OBJECT_ID(N'{qualified_target_table_name}', 'U') IS NOT NULL
                BEGIN
                    DROP TABLE {qualified_target_table_name};
                END
            """
            self.cursor.execute(exists_and_drop_sql)
        except Exception as e:
            raise DatabaseOperationException(f"Failed to drop target table {e}")

    def _create_target_table_with_schema(self, table_schema: set, qualified_target_table_name: str):
        try:
            create_table_sql = f"""
                CREATE TABLE {qualified_target_table_name} (
                    {', '.join(table_schema)}
                );
                """
            self.cursor.execute(create_table_sql)
        except Exception as e:
            raise TableException(f"Cannot Create Table {qualified_target_table_name}in target {e}")

    @retry(
        retry=retry_if_exception_type(SyncFailure),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=10)
    )
    def _push_historical_load(
        self, df: DataFrame,
        target_table_name: str
    ) -> bool:

        qualified_target_table_name = f"[{self.target_schema}].[{target_table_name}]"
        connection_props = {
            **self.jdbc_props,
            **self.auth_strategy.get_connection_props_dict()
        }
        try:
            df.write \
                .format("jdbc") \
                .mode("append") \
                .options(**connection_props) \
                .option("dbtable", qualified_target_table_name) \
                .option("spark.sql.jsonGenerator.ignoreNullFields", "false") \
                .save()
            return True
        except Exception as e:
            raise SyncFailure(f"Problems loading full refresh data to {target_table_name} - {e}")

    @retry(
        retry=retry_if_exception_type(SyncFailure),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=10)
    )
    def _write_to_staging(self, staging_df: DataFrame, table_name: str) -> bool:
        # Write Data using Spark Connector
        connection_props = {
                **self.jdbc_props,
                **self.auth_strategy.get_connection_props_dict()
        }
        try:
            staging_df.write \
                .format("jdbc") \
                .mode("append") \
                .options(**connection_props) \
                .option("dbtable", f"[{self.target_schema}].[{table_name}]") \
                .option("spark.sql.jsonGenerator.ignoreNullFields", "false") \
                .save()
            return True
        except Exception as e:
            raise SyncFailure(f"Sync failed to write to staging table {e}")

    @retry(
        retry=retry_if_exception_type(SyncFailure),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=1, max=10)
    )
    def _merge_to_target(
        self,
        source_table_name: str,
        target_table_name: str,
        join_condition: str,
        value_columns: list,
        update_columns: str
    ) -> bool:
        try:
            # Strip double quotes from column names if they exist
            value_columns = [col.replace('"', '') for col in value_columns]
            # Use brackets for SQL Server
            value_columns_sql = [f'[{col}]' for col in value_columns]

            # Override update_columns for sqlserver. Create new
            update_columns_sql = ', '.join([f't.[{col}] = s.[{col}]' for col in value_columns])

            merge_query = f"""
                MERGE INTO [{self.target_schema}].[{target_table_name}] t
                USING [{self.target_schema}].[{source_table_name}] s
                ON {join_condition}
                WHEN MATCHED AND s._change_type = 'delete' THEN DELETE
                WHEN MATCHED THEN UPDATE SET {update_columns_sql}
                WHEN NOT MATCHED AND s._change_type != 'delete' THEN INSERT ({', '.join(value_columns_sql)})
                VALUES ({', '.join([f's.[{col}]' for col in value_columns])});
            """
            logging.info(f"Merge Query {merge_query}")
            self.cursor.execute(merge_query)
            return True
        except Exception as e:
            raise SyncFailure(f"Cannot Merge into Target {e}")

    def _push_cdc_loads(
        self,
        staging_df: DataFrame, target_table_name: str,
        join_condition: str,
        value_columns: list,
        update_columns: str
    ) -> bool:
        staging_table_name = ""
        try:
            temp_table_name = f"{target_table_name}_temp_load"
            if len(target_table_name) > self.allowable_table_name_length:
                truncated_table_name = target_table_name[:self.allowable_table_name_length-10]
                temp_table_name = f'{truncated_table_name}_temp_load'

            logging.info(f"Writing into temporary table for merge {temp_table_name}")
            staging_table_name = temp_table_name
            self._write_to_staging(staging_df, staging_table_name)
            logging.info(f"Data Written to staging table {staging_table_name}. Now Merging")
            self._merge_to_target(
                source_table_name=staging_table_name,
                target_table_name=target_table_name,
                join_condition=join_condition,
                value_columns=value_columns,
                update_columns=update_columns
            )
            return True
        except Exception as sync_failure:
            raise SyncFailure(f"Failed CDC load for {target_table_name} - {sync_failure}")
        finally:
            self._drop_target_table_if_exists(f"[{self.target_schema}].[{staging_table_name}]")

    def _disable_auto_commit(self):
        try:
            self.conn.jconn.setAutoCommit(False)
        except Exception as e:
            raise DatabaseOperationException(f"Failed to disable Auto Commit {e}")

    def _enable_auto_commit(self):
        try:
            self.conn.jconn.setAutoCommit(True)
        except Exception as e:
            raise DatabaseOperationException(f"Failed to stop the Transaction {e}")

    def _begin_transaction(self):
        logging.info("Starting CDC Transaction")
        try:
            self.cursor.execute("BEGIN TRANSACTION")
        except Exception as e:
            raise DatabaseOperationException(f"Failed to Begin the Transaction {e}")

    def _rollback(self):
        logging.info("Rolling back changes")
        try:
            self.conn.rollback()
        except Exception as e:
            raise DatabaseOperationException(f"Cannot roll back changes {e}")

    def _commit(self):
        self.conn.commit()

    def close_destination(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
