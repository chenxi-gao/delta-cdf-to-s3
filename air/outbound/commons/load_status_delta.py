import datetime
import logging
from abc import ABC
from enum import Enum
from os import environ
from typing import Optional

from pydantic.v1 import Extra, BaseModel
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.utils import AnalysisException
from .destination import DestinationType

ENV_CATALOG = 'CATALOG_NAME'
db_to_outbound_status: str = 'abacus_load_status_outbound'


class LoadStatusOutbound(BaseModel, ABC):
    table_name: str
    latest_updated_timestamp: Optional[datetime.datetime] = None
    destination_type: DestinationType

    class Config:
        validate_all = True
        validate_assignment = True
        extra = Extra.forbid

    @staticmethod
    def construct_load_status(row: Row):
        if not row:
            return None
        else:
            return LoadStatusOutbound(
                table_name=row.table_name,
                latest_updated_timestamp=row.latest_updated_timestamp,
                destination_type=row.destination_type
            )

    @staticmethod
    def get_load_status_schema():
        return StructType([
            StructField('table_name', StringType(), False),
            StructField('latest_updated_timestamp', TimestampType(), True),
            StructField('destination_type', StringType(), True)
        ])


# create enum class for Layer
class PipelineLayerOutbound(str, Enum):
    bronze = 'bronze'
    silver = 'silver'
    gold = 'gold'


class PipelineHelperOutbound(BaseModel, ABC):
    _spark: SparkSession = SparkSession.builder.getOrCreate()
    layer: PipelineLayerOutbound

    class Config:
        validate_all = True
        validate_assignment = True
        extra = Extra.forbid

    @property
    def catalog(self):
        return environ[ENV_CATALOG]

    @property
    def qualified_load_status(self):
        return f"`{self.catalog}`.`{self.layer}`.`{db_to_outbound_status}`"

    def _create_table(self):
        logging.info(f'Creating the load status table for outbounds : {self.qualified_load_status}')
        self._execute_dml(f"""
           CREATE TABLE IF NOT EXISTS {self.qualified_load_status} (
                    table_name STRING NOT NULL,
                    latest_updated_timestamp TIMESTAMP,
                    destination_type STRING NOT NULL,
                    PRIMARY KEY (table_name, destination_type)
                )
                """)

    def create_table(self):
        self._create_table()

    @classmethod
    def _new_pipeline_helper(cls, layer):
        return PipelineHelperOutbound(layer=layer)

    def _execute_dml(self, sql: str):
        try:
            logging.info("Executing DML {sql}")
            return self._spark.sql(sql)
        except AnalysisException as e:
            # use error class for 14.3 LTS or error message for backwards compatibility with 11.3 LTS
            if e.getErrorClass() == 'TABLE_OR_VIEW_NOT_FOUND' or 'Table or view not found' in str(e):
                logging.warning(f'Load status table {self.qualified_load_status} \
                                does not exist in {self.catalog}.{self.layer}')
                self._create_table()
                return self._spark.sql(sql)

    def get_load_status_data(self):
        table_df = self._execute_dml(f"select * from {self.qualified_load_status}")
        return table_df

    def insert_or_update_load_status(self, composite_keys: tuple, data: dict):
        """
        Insert or update a record using Spark SQL with an existence check.
        :param unique_key: The column name that is used to determine if a conflict occurs.
        :param data: A dictionary where keys are column names and values are the data for those columns.
        """

        # Convert dictionary values to a safe format for SQL statements
        # It is important to properly escape strings to avoid SQL injection
        def format_sql_value(value):
            if isinstance(value, str):
                escaped_value = value.replace("'", "''")  # Do the replace outside the f-string
                return f"'{escaped_value}'"
            elif isinstance(value, datetime.datetime):
                return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'"
            else:
                return str(value)

        data_sql_values = {k: format_sql_value(v) for k, v in data.items()}

        where_clause = ' AND '.join([f"{key} = {format_sql_value(data[key])}" for key in composite_keys])
        exists_sql = f"SELECT 1 FROM {self.qualified_load_status} WHERE {where_clause}"
        logging.info(f"Checking current load status {exists_sql}")
        exists = self._execute_dml(exists_sql).count() > 0

        if exists:
            # Record exists, perform update
            # Build SET clause for update by assigning values from data_sql_values dictionary
            set_clause = ', '.join([f"{k} = {v}" for k, v in data_sql_values.items() if k not in composite_keys])
            update_sql = f"""
            UPDATE {self.qualified_load_status}
            SET {set_clause}
            WHERE {where_clause}
            """
            self._execute_dml(update_sql)
        else:
            # Record does not exist, perform insert
            # Prepare columns and values clauses for insert statement
            columns = ', '.join(data.keys())
            values = ', '.join(data_sql_values.values())
            insert_sql = f"""
            INSERT INTO {self.qualified_load_status} ({columns})
            VALUES ({values})
            """
            self._execute_dml(insert_sql)
