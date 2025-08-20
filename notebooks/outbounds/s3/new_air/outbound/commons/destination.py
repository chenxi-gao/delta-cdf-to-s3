from abc import ABC, abstractmethod
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from .authentication import AuthenticationStrategy
import logging
from enum import Enum
from .exceptions import SyncFailure, TableException
from .utilities import OutboundUtils
from .cdc_loader import CDCLoader
import re


# Destination Type Enum
class DestinationType(Enum):
    SQL_SERVER = "sqlserver"
    S3 = "s3"


# Base Destination Class
class DestinationBase(ABC):
    def __init__(
        self,
        jdbc_props: dict,
        auth_strategy: AuthenticationStrategy,
        target_schema: str,
        destination_type: str,
        spark: SparkSession
    ):
        self.target_schema = target_schema
        self.jdbc_props = jdbc_props
        self.auth_strategy = auth_strategy
        self.destination_type = destination_type
        self.spark = spark
        try:
            self.cursor = self._connect()
        except Exception as e:
            logging.error(f"Failed to connect to the source {e}")
            raise

    def get_session_cursor(self):
        return self.cursor

    @abstractmethod
    def _connect(self):
        """
        Method that connects to the target destination using appropriate driver/library
        and assigns the connection to self.conn object

        Raises: DatabaseError or other exceptions based on library use.
        Method can be annotated with retry package for the raised exception

        Returns a self.conn.cursor object that can be used to execute queries.
        """
        pass

    @abstractmethod
    def _source_target_mapping(self, spark_type) -> str:
        """
        This method provides a source to target schema mapping.
        Add all the spark type to destination type mapping in this method.
        Format: if isinstance(spark_type, T.<some spark type>):
                    return "<destination equivalent type>"

        Args:
            spark_type: spark data type i.e schema.fields.dataType

        returns string value for target datatype
        """
        pass

    @abstractmethod
    def _target_schema_exists(self, create_on_failure: bool = True):
        """
        This method checks if the required self.target_schema exists on target database.
        Args:
        create_on_failure: If set to true, this should create a schema in target database
        if it doesn't exist

        returns None if schema exists
        raises SchemaNotFound Exception if schema doesn't exist and create_on_failure = False
        """
        pass

    @abstractmethod
    def _destination_truncate(self, table_name: str):
        """
        This method truncates the specified table at the destination

        Args:
            table_name (str): Name of the table to truncate

        Raises:
            DatabaseOperationException: If truncation operation fails

        """
        pass

    @abstractmethod
    def _drop_target_table_if_exists(self, qualified_target_table_name: str):
        """
        This method drops the specified table at the destination

        Args:
            qualified_target_table_name (str): Full name of the table to truncate

        Raises:
            DatabaseOperationException: If drop operation fails

        """
        pass

    @abstractmethod
    def _create_target_table_with_schema(self, table_schema: set, qualified_target_table_name: str):
        """
        This method creates the specified table at the destination with table_schema

        Args:
            table_schema (str): a set with "{column_name} {destination_data_type}"
            qualified_target_table_name: Full table name including schema.<table_name>
            for the target table

        Raises:
            TableException: If fails to create table

        """
        pass

    @abstractmethod
    def _push_historical_load(
        self, df: DataFrame,
        target_table_name: str
    ) -> bool:
        """
        Method for pushing historical load from a source to a target table.

        Args:
        df (DataFrame): The dataframe containing data to be loaded.
        target_table_name (str): The name of the target table where data is to be loaded.

        Returns:
        bool: Returns True if data load is successful.

        Raises:
        SyncFailure: If the write operation fails.
        Method can be annonated with retry package on SyncFailure Exception
        """
        pass

    @abstractmethod
    def _push_cdc_loads(
        self,
        staging_df: DataFrame, target_table_name: str,
        join_condition: str,
        value_columns: list,
        update_columns: str
    ) -> bool:
        """
        Method for pushing cdc load from a source to a target table.

        Args:
        staging_df (DataFrame): The dataframe prepared to be loaded to staging.
        This is what gets merged to the target table.
        target_table_name (str): The name of the target table where data is to be loaded.

        join_condition: Join condition to join staging and target table on merge keys
        Should be in the format: ' AND '.join([f"t.{key} = s.{key}" for key in merge_keys])

        value_columns: it is a list of source/target field names.
        Better to use standardization utility for names. Currently converts everything to lower case

        update_columns: String concatenated to map s.col = t.col for the columns that needs to be updated
        should be in the format: ', '.join([f"t.{col} = s.{col}" for col in value_columns])

        Returns:
        bool: Returns True if data load is successful.

        Raises:
        SyncFailure: If the write operation fails.
        SyncFailure is a retriable exception and method can be annonated with retry package.
        Depending on the CDC implementation, the retry annotation should be properly applied
        where database transaction occurs.
        """
        pass

    @abstractmethod
    def _disable_auto_commit(self):
        pass

    @abstractmethod
    def _enable_auto_commit(self):
        self.conn.jconn.setAutoCommit(True)

    @abstractmethod
    def _begin_transaction(self):
        pass

    @abstractmethod
    def _rollback(self):
        pass

    @abstractmethod
    def _commit(self):
        pass

    def _process_df_before_write(self, df) -> DataFrame:
        """
        This method can be used to do necessary transformation before
        writing to the target table if required

        Args: df : Dataframe to be transformed.

        Returns a processed dataframe that can directly be supplied to target table.

        Raise: Should Raise non-retriable exception
        """
        return df

    def _update_metadata_bulk(self, pipeline_helper, composite_keys: set, metadata_dicts: list):
        for metadata_dict in metadata_dicts:
            composite_keys = ("table_name", "destination_type")
            pipeline_helper.insert_or_update_load_status(composite_keys, metadata_dict)
        logging.info(f"Load Status Metadata saved for {len(metadata_dicts)} syncs")

    def _get_source_schema(self, qualified_source_table_name: str) -> DataFrame:
        return self.spark.sql(f"DESCRIBE {qualified_source_table_name}").collect()

    def _create_table_with_schema_information(
        self,
        df: DataFrame,
        source_table_name: str,
        qualified_target_table_name: str
    ):
        source_schema = self._get_source_schema(source_table_name)
        df_cols_lower = {OutboundUtils.standardize_name(field.name) for field in df.schema.fields}

        # Need a column_name: datatype mapping
        name_to_type_mapping = {
            OutboundUtils.standardize_name(field.name): field.dataType for field in df.schema.fields
        }
        processed_columns = set()

        column_definitions = []
        for row in source_schema:
            column_name = OutboundUtils.standardize_name(row["col_name"])

            # Exclude potential extra columns in source schema
            if column_name not in df_cols_lower:
                logging.info(f"Metadata Column {column_name}. --Excluded")
                continue

            if column_name in processed_columns:
                continue

            processed_columns.add(column_name)
            data_type = row["data_type"]

            if data_type.startswith('varchar') or data_type.startswith('char'):
                column_definitions.append(f"{column_name} {data_type.lower()}")
            else:
                spark_data_type = name_to_type_mapping[column_name]
                sqlserver_data_type = self._source_target_mapping(spark_data_type)
                column_definitions.append(f"{column_name} {sqlserver_data_type}")
            self._drop_target_table_if_exists(qualified_target_table_name)
            self._create_target_table_with_schema(column_definitions, qualified_target_table_name)

    def _prepare_for_historical_load(
        self,
        source_df: DataFrame,
        source_table_name: str,
        target_table_name: str,
        create_table_auto: bool,
        serialize_complex_type: bool,
        serialize_binary_type: bool
    ) -> DataFrame:

        qualified_target_table_name = f"[{self.target_schema}].[{target_table_name}]"

        if create_table_auto:
            logging.info("Create auto table is set to True. Creating a new table in sqlserver")
            try:
                self._create_table_with_schema_information(
                    source_df, source_table_name, qualified_target_table_name
                )
                self._commit()
            except Exception as e:
                raise TableException(f"Table Creation Failed - {e}")
        else:
            logging.info(f"Auto table creation disabled. {target_table_name} should exist in sqlserver")
            logging.info(f"Truncating table {qualified_target_table_name}")
            self._destination_truncate(qualified_target_table_name)

        if serialize_complex_type:
            source_df = OutboundUtils.serialize_complex_types(source_df)

        if serialize_binary_type:
            source_df = OutboundUtils.serialize_binary_type_to_base64(source_df)

        source_df = self._process_df_before_write(source_df)
        return source_df

    def handle_all_cdc_loads(
        self, table_info_df: DataFrame, pipeline_helper, cdf_history_query_partition, rollback_on_error: bool = True,
        serialize_complex_type: bool = True,
        serialize_binary_type: bool = False
    ):
        sync_failed = False
        if table_info_df.count() == 0:
            return

        metadata_dicts = []
        cdc_loader = CDCLoader(self.spark)
        try:
            # Starting a transaction
            # Disable auto commit so we can roll back transactions
            self._disable_auto_commit()
            self._begin_transaction()
            for row in table_info_df.collect():
                source_table_name = row["source_table"]
                target_table_name = row['target_table']
                min_ts = row["min_ts"]
                max_ts = row["max_ts"]
                merge_keys = row["merge_keys"]

                staging_df = cdc_loader.process_table_changes(
                    source_table_name, min_ts, max_ts, cdf_history_query_partition,
                    merge_keys
                )

                # Repartition for > 1 mil records
                if staging_df.count() > 1000000:
                    num_partitions = staging_df.count() // 10000
                    staging_df = staging_df.repartition(num_partitions)

                # Prepare Staging Data
                staging_df = OutboundUtils.prepare_staging_data(
                    staging_df,
                    serialize_complex_type=serialize_complex_type,
                    serialize_binary_type=serialize_binary_type
                )

                run_success = False
                join_condition = ' AND '.join([f"t.{key} = s.{key}" for key in merge_keys])
                value_columns = [
                    f'{OutboundUtils.standardize_name(field.name)}'
                    for field in self.spark.table(source_table_name).schema.fields
                ]
                update_columns = ', '.join([f"t.{col} = s.{col}" for col in value_columns])
                try:
                    run_success = self._push_cdc_loads(
                        staging_df=staging_df,
                        target_table_name=target_table_name,
                        join_condition=join_condition,
                        value_columns=value_columns,
                        update_columns=update_columns
                    )
                except SyncFailure as failure:
                    logging.error(f"Sync failed for {source_table_name} - {failure}")
                    logging.info(f"Discarding failure and Moving On {rollback_on_error}")
                    # Continue for other tables if rollback_on_error = False
                    # Don't need to update load status update
                    sync_failed = True
                    if rollback_on_error:
                        self._rollback()
                        raise SyncFailure(f"{failure}")

                if run_success:
                    just_the_table_name = re.split(r"\.", source_table_name)[-1]
                    metadata_dicts.append({
                        'table_name': just_the_table_name,
                        'latest_updated_timestamp': max_ts,
                        'destination_type': self.destination_type.value
                    })
            self._update_metadata_bulk(
                pipeline_helper,
                composite_keys=("table_name", "destination_type"),
                metadata_dicts=metadata_dicts
            )

            # Committing the transaction if all merges are successful
            logging.info(f"Load Status Metadata saved for {len(metadata_dicts)} syncs")
            logging.info("All merges were successful. Committing the transaction.")
            self._commit()
            if sync_failed:
                raise SyncFailure("Some of the syncs have failed. Workflow Failed.")
        except Exception as other_exceptions:
            if rollback_on_error:
                self._rollback()
            else:
                self._commit()
                self._update_metadata_bulk(
                    pipeline_helper,
                    composite_keys=("table_name", "destination_type"),
                    metadata_dicts=metadata_dicts
                )
            raise Exception(other_exceptions)
        finally:
            self._enable_auto_commit()

    def handle_all_historical_loads(
        self, table_info_df: DataFrame, pipeline_helper, create_table_auto: bool = True,
        serialize_complex_type: bool = True,
        serialize_binary_type: bool = False
    ):
        sync_failed = False
        if table_info_df.count() == 0:
            return

        metadata_dicts = []
        # Check if target schema exists:
        self._target_schema_exists(create_on_failure=create_table_auto)

        try:
            for row in table_info_df.collect():
                source_table_name = row["source_table"]
                target_table_name = row["target_table"]
                max_timestamp = row["max_ts"]
                source_df = self.spark.read.table(source_table_name)
                source_df = self._prepare_for_historical_load(
                    source_df,
                    source_table_name,
                    target_table_name,
                    create_table_auto,
                    serialize_complex_type,
                    serialize_binary_type
                )

                # Repartition for > 1 mil records. Seems to have helped.
                if source_df.count() > 1000000:
                    logging.info(f'{source_table_name} has over 1,000,000 rows. Repartitioning...')
                    num_partitions = source_df.count() // 10000
                    source_df = source_df.repartition(num_partitions)

                run_success = False
                try:
                    run_success = self._push_historical_load(
                        source_df, target_table_name
                    )

                except SyncFailure as failed_sync:
                    logging.error(f"Sync failed for {source_table_name} - {failed_sync}")
                    logging.info("Discarding failure and Moving On ...")
                    sync_failed = True
                    continue
                    # Continue for other tables
                just_the_table_name = re.split(r"\.", source_table_name)[-1]
                if run_success:
                    metadata_dicts.append({
                            'table_name': just_the_table_name,
                            'latest_updated_timestamp': max_timestamp,
                            'destination_type': self.destination_type.value
                    })
                logging.info(f"Historical Load Completed for {source_table_name}")

            # Update Load Status Metadata for successful syncs
            for metadata_dict in metadata_dicts:
                composite_keys = ("table_name", "destination_type")
                pipeline_helper.insert_or_update_load_status(composite_keys, metadata_dict)

            logging.info("All Syncs were successful")
            if sync_failed:
                raise SyncFailure("Some of the sync have failed. Workflow Failed")

        except Exception as other_exceptions:
            # Raise exception and stop the process
            raise Exception(f"Sync Process Failed due to exception {other_exceptions}")

        finally:
            # Successful syncs should be saved
            for metadata_dict in metadata_dicts:
                composite_keys = ("table_name", "destination_type")
                pipeline_helper.insert_or_update_load_status(composite_keys, metadata_dict)
            logging.info(f"Load Status Metadata saved for {len(metadata_dicts)} syncs")
