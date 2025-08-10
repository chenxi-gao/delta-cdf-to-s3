from .load_status_delta import PipelineHelperOutbound, LoadStatusOutbound
import logging
import psycopg2
from psycopg2 import sql

db_to_outbound_status: str = 'abacus_load_status_outbound'


class PipelineHelperAuroraOutbound(PipelineHelperOutbound):

    @property
    def qualified_load_status(self):
        return f"{self.layer}.{db_to_outbound_status}"

    def _create_table(self):

        logging.info(f'Creating the outbound load status table : {self.qualified_load_status}')
        conn = self._make_connection()
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {self.layer}")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.qualified_load_status} (
                    table_name TEXT NOT NULL,
                    latest_updated_timestamp TIMESTAMP,
                    destination_type TEXT NOT NULL,
                    PRIMARY KEY (table_name, destination_type)
                )
                """)

    def get_load_status_data(self, create_table_on_failure: bool = True):
        conn = None
        try:
            conn = self._make_connection()
            with conn.cursor() as cur:
                sql_string = f"select * from {self.qualified_load_status}"
                cur.execute(sql_string)
                rows = cur.fetchall()
                return self._spark.createDataFrame(rows, LoadStatusOutbound.get_load_status_schema())
        except psycopg2.errors.UndefinedTable as e:
            if create_table_on_failure:
                self._create_table()
                # retry the DML but don't create the table again to avoid infinite recursion
                return self.get_load_status_data(sql_string, create_table_on_failure=False)
            raise e
        except Exception as ex:
            raise RuntimeError(f'Failed to execute SQL: {sql_string}') from ex
        finally:
            if conn:
                conn.close()

    def insert_or_update_load_status(self, composite_keys: tuple, data: dict):
        """
        Insert or update a record in a table, based on a conflict with the unique key.
        :param table_name: The name of the table where the upsert will be performed.
        :param unique_key: The column name that is used to determine if a conflict occurs.
        :param data: A dictionary where keys are column names and values are the data for those columns.
        """
        keys = list(data.keys())
        # Convert the composite keys tuple to SQL identifiers
        conflict_target = sql.SQL(', ').join(sql.Identifier(k) for k in composite_keys)

        insert_columns = sql.SQL(', ').join(sql.Identifier(k) for k in keys)
        insert_values_placeholders = sql.SQL(', ').join(sql.Placeholder(k) for k in keys)

        update_assignment = sql.SQL(', ').join(
            sql.SQL("{key} = EXCLUDED.{key}").format(key=sql.Identifier(k))
            for k in keys if k not in composite_keys
        )
        sql_string = sql.SQL(
            """
            INSERT INTO {table} ({insert_columns})
            VALUES ({insert_values_placeholders})
            ON CONFLICT ({conflict_target})
            DO UPDATE SET {update_assignment}
            """
        ).format(
            table=sql.Identifier(self.layer, db_to_outbound_status),
            insert_columns=insert_columns,
            insert_values_placeholders=insert_values_placeholders,
            conflict_target=conflict_target,
            update_assignment=update_assignment,
        )
        logging.info(f"Upsert query {sql_string}")
        conn = None
        try:
            conn = self._make_connection()
            with conn.cursor() as cur:
                cur.execute(sql_string, data)

        except Exception as ex:
            raise RuntimeError(f'Failed to execute SQL: {sql_string.as_string(conn)}') from ex
        finally:
            if conn:
                conn.close()
