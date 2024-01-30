from typing import List, Tuple, Optional

import psycopg2
import logging
from pyspark.sql import DataFrame, SparkSession


class PostgresHelper:
    """
    A helper class for connecting to and interacting with PostgreSQL databases.
    """

    def __init__(self, spark_session: SparkSession, logger: logging.Logger):
        """
        Initialize the helper class with connection parameters.

        Args:
            spark_session (SparkSession) : SparkSession object 
            logger (logging.Logger): Logger object for logging messages.
        """
        self._logger = logger
        self._database_name = None
        self._user_name = None
        self._password = None
        self._host = None
        self._port = None
        self._auto_commit = None
        self._schema_name = None
        self._connection = None
        self._spark_session = spark_session

    def get_all_parameters(self) -> dict:
        """
        Get all connection parameters as a dictionary.

        Returns:
            dict: Connection parameters.
        """
        return {
            'database_name': self._database_name,
            'schema_name': self._schema_name,
            'host': self._host,
            'port': self._port,
            'user_name': self._user_name,
            'password': self._password,
            'auto_commit': self._auto_commit
        }

    def set_all_parameters(self, host: str, port: int, user_name: str, password: str, database_name: str,
                           schema_name: str,
                           auto_commit=False):
        """
        Set all connection parameters at once.

        Args:
            host (str): The host of the database server.
            port (int): The port number of the database server.
            user_name (str): The username for accessing the database.
            password (str): The password for accessing the database.
            database_name (str): The database to be connected.
            schema_name (str): The schema to be connected.
            auto_commit (bool, optional): Whether to automatically commit changes after each query. Defaults to False.
        """
        self._host = host
        self._port = port
        self._user_name = user_name
        self._password = password
        self._database_name = database_name
        self._schema_name = schema_name
        self._auto_commit = auto_commit

    def __check_all_connection_parameters_set(self):
        """
        Check all connection parameters are set to connect to the PostgreSQL database.

        Raises:
            Exception: If any connection parameter is None.
        """
        if not all([self._database_name, self._schema_name, self._user_name, self._password, self._host, self._port]):
            raise Exception("PostgreSQL Connection parameters cannot be None.")
    
            
    def connect(self) -> None:
        """
        Establishes a connection to the PostgreSQL database.

        Raises:
            Exception: If connection already exists or connection parameters are invalid.
        """

        self._logger.info("Connecting to PostgreSQL database...")

        if self._connection is not None:
            raise Exception("PostgreSQL Connection already exists.")

        self.__check_all_connection_parameters_set()
        
        try:
            self._connection = psycopg2.connect(
                database= self._database_name,
                user=self._user_name,
                password=self._password,
                host=self._host,
                port=self._port
            )
            self._connection.autocommit = self._auto_commit
            self._logger.info("Successfully connected to PostgreSQL database.")
        except Exception as e:
            raise Exception(f"Failed to connect to database: {e}")
 
    def get_connection(self):
        return self._connection
    
    def __check_postgresql_connection(self):
        """
        Check if the PostgreSQL connection is open.

        Raises:
            Exception: If PostgreSQL connection is not open.
        """
        if self._connection is None:
            raise Exception("PostgreSQL Connection is not open.")

    def disconnect(self) -> None:
        """
        Closes the connection to the PostgreSQL database.

        Raises:
            Exception: If connection is not open.
        """
        if self._connection is not None:
            self._logger.info("Disconnecting from PostgreSQL database...")

            try:
                self._connection.close()
                self._connection = None
                self._logger.info("Successfully disconnected from PostgreSQL database.")
            except Exception as e:
                raise Exception(f"Failed to close connection: {e}")
        else:
            self._logger.info("PostgreSQL Connection is already closed.")

    def commit(self) -> None:
        """
        Commits any pending changes to the PostgreSQL database.

        Raises:
            Exception: If connection is not open.
        """

        self._logger.info("Committing pending changes to PostgreSQL database...")
    
        self.__check_postgresql_connection()
        try:
            self._connection.commit()
            self._logger.info("Successfully committed changes to PostgreSQL database.")
        except Exception as e:
            self.disconnect()
            raise Exception(f"Failed to commit changes: {e}")

    def select_rows(self, table_name: str, columns: List[str], condition: Optional[str] = None,
                    fetch_all: bool = False) -> List[Tuple]:
        """
        Selects rows from a PostgreSQL table.

        Args:
            table_name (str): The name of the table.
            columns (List[str]): List of column names to be selected.
            condition (str, optional): The condition to be applied in the WHERE clause. Defaults to None.
            fetch_all (bool, optional): If True, selects all columns. Defaults to False.

        Returns:
            List[tuple]: A list of tuples representing the selected rows.
        """
        table_name = '.'.join((self._schema_name,table_name))
        cols = '*' if fetch_all else ','.join(columns)
        self._logger.info(f"Selecting rows for {cols} columns from table '{table_name}' with condition: {condition}")
        self.__check_postgresql_connection()
        try:
            with self._connection.cursor() as cursor:
                query = f"SELECT {cols} FROM {table_name}"
                if condition:
                    query += f" WHERE {condition}"
                
                query += ';'
                self._logger.info("Running query::"+query)
                cursor.execute(query)
                rows = cursor.fetchall()
                self._logger.info(
                    f"Successfully selected rows for {cols} columns from table '{table_name}' with condition: {condition}")
                return rows
        except Exception as e:
            self.disconnect()
            self._logger.error(f"Error selecting rows from table '{table_name}': {str(e)}")
            raise

    def select_distinct_values(self, table_name: str, col: str, condition: Optional[str] = None) -> List[Tuple]:
        """
        Selects distinct values from a column in a PostgreSQL table.

        Args:
            table_name (str): The name of the table.
            col (str): The name of the column for which distinct values are to be selected.
            condition (str, optional): The condition to be applied in the WHERE clause. Defaults to None.

        Returns:
            List[tuple]: A list of tuples representing the selected distinct values.
        """
        table_name = '.'.join((self._schema_name,table_name))
        self._logger.info(f"Selecting distinct values for {col} from table '{table_name}' with condition: {condition}")
        self.__check_postgresql_connection()
        try:
            with self._connection.cursor() as cursor:
                query = f"SELECT DISTINCT({col}) FROM {table_name}"
                if condition:
                    query += f" WHERE {condition}"
                
                query += ';'
                self._logger.info("Running query::"+query)
                cursor.execute(query)
                rows = cursor.fetchall()
                self._logger.info(
                    f"Successfully selected distinct values for {col} from table '{table_name}' with condition: {condition}")
                return rows
        except Exception as e:
            self.disconnect()
            self._logger.error(f"Error selecting distinct values from table '{table_name}': {str(e)}")
            raise


    def insert_row(self, table_name: str, column_name: str, value) -> None:
        """
        Inserts a single row into a specified table.

        Args:
            table_name (str): The name of the table to insert the value into.
            column_name (str): The name of the column to insert the value into.
            value: The value to insert.
        """
        table_name = '.'.join((self._schema_name,table_name))
        self._logger.info(f"Inserting value '{value}' into table '{table_name}'...")
        self.__check_postgresql_connection()
        try:
            with self._connection.cursor() as cursor:
                query = f"INSERT INTO {table_name} ({column_name}) VALUES (%s)"
                
                query += ';'
                self._logger.info("Running query::"+query)
                cursor.execute(query, (value,))
                self.commit()
                self._logger.info(f"Successfully inserted value '{value}' into table '{table_name}'.")
        except Exception as e:
            self.disconnect()
            self._logger.error(f"Error inserting value into table '{table_name}': {str(e)}")
            raise

    def insert_rows(self, table_name: str, columns: List[str], values: List[Tuple]) -> None:
        """
        Inserts multiple rows into a specified table.

        Args:
            table_name (str): The name of the table to insert the rows into.
            columns (List[str]): A list of column names.
            values (List[tuple]): A list of values, where each value is a tuple corresponding to the columns.
        """
        table_name = '.'.join((self._schema_name,table_name))
        self._logger.info(f"Inserting values into table '{table_name}'...")
        self.__check_postgresql_connection()
        try:
            query = f"INSERT INTO {table_name} ({','.join(columns)}) VALUES %s"
            with self._connection.cursor() as cursor:
                query += ';'
                self._logger.info("Running query::"+query)
                cursor.executemany(query, values)
                self.commit()
                self._logger.info(f"Successfully inserted {len(values)} rows into table '{table_name}'.")
        except Exception as e:
            self.disconnect()
            self._logger.error(f"Error inserting rows into table '{table_name}': {str(e)}")
            raise

    def remove_row(self, table_name: str, condition: str) -> None:
        """
        Removes a row from a specified table based on a condition.

        Args:
            table_name (str): The name of the table to remove the row from.
            condition (str): A SQL condition expression.
        """
        table_name = '.'.join((self._schema_name,table_name))
        self._logger.info(f"Removing row from table '{table_name}' as condition '{condition}'...")
        self.__check_postgresql_connection()
        try:
            query = f"DELETE FROM {table_name} WHERE {condition}"
            with self._connection.cursor() as cursor:
                query += ';'
                self._logger.info("Running query::"+query)
                cursor.execute(query)
                self.commit()
                self._logger.info(f"Successfully removed row from table '{table_name}' with condition: {condition}")
        except Exception as e:
            self.disconnect()
            self._logger.error(f"Error removing row from table '{table_name}': {str(e)}")
            raise

    def drop_table(self, table_name: str) -> None:
        """
        Drops a specified table from the database.

        Args:
            table_name (str): The name of the table to drop.
        """
        table_name = '.'.join((self._schema_name,table_name))
        self.__check_postgresql_connection()
        try:
            self._logger.info(f"Dropping table '{table_name}'...")
            with self._connection.cursor() as cursor:
                query = f"DROP TABLE IF EXISTS {table_name}"
                query += ';'
                self._logger.info("Running query::"+query)
                cursor.execute(query)
                self.commit()
                self._logger.info(f"Successfully dropped table '{table_name}'.")
        except Exception as e:
            self.disconnect()
            self._logger.error(f"Error dropping table '{table_name}': {str(e)}")
            raise

    def update_row(self, table_name: str, columns: List[str], new_values: Tuple, condition: str) -> None:
        """
        Updates a single row in a specified table based on a condition.

        Args:
            table_name (str): The name of the table to update the row in.
            columns (List[str]): A list of column names.
            new_values (Tuple): A tuple of new values corresponding to the columns.
            condition (str): A SQL condition expression.
        """
        table_name = '.'.join((self._schema_name,table_name))
        self.__check_postgresql_connection()
        try:
            self._logger.info(f"Updating row in table '{table_name}' with condition: {condition}")
            
            with self._connection.cursor() as cursor:
                query = f"UPDATE {table_name} SET {','.join(columns)} = %s WHERE {condition}"
                query += ';'
                self._logger.info("Running query::"+query)
                cursor.execute(query, new_values)
                self.commit()
                self._logger.info(f"Successfully updated row in table '{table_name}' with condition: {condition}")
        except Exception as e:
            self.disconnect()
            self._logger.error(f"Error updating row in table '{table_name}': {str(e)}")
            raise

    def update_rows(self, table_name: str, columns: List[str], new_values: List[Tuple], condition: str) -> None:
        """
        Updates multiple rows in a specified table based on specific conditions.

        Args:
            table_name (str): The name of the table to update the rows in.
            columns (List[str]): A list of column names to update.
            new_values (List[Tuple]): A list of tuples of new values corresponding to the columns.
            condition (str): A SQL condition expression.
        """
        table_name = '.'.join((self._schema_name,table_name))
        self.__check_postgresql_connection()
        try:
            self._logger.info(f"Updating {len(new_values)} rows in table '{table_name}' with specific conditions...")
            with self._connection.cursor() as cursor:
                for new_values_row in new_values:
                    query = f"UPDATE {table_name} SET {','.join(columns)} = %s WHERE {condition}"
                    query += ';'
                    self._logger.info("Running query::"+query)
                    cursor.execute(query, new_values_row)
                self.commit()
                self._logger.info(
                    f"Successfully updated {len(new_values)} rows in table '{table_name}' with specific conditions.")
        except Exception as e:
            self.disconnect()
            self._logger.error(f"Error updating rows in table '{table_name}': {str(e)}")
            raise

    def truncate_table(self, table_name: str) -> None:
        """
        Truncates a specified table, removing all data from the table.

        Args:
            table_name (str): The name of the table to truncate.
        """
        table_name = '.'.join((self._schema_name,table_name))
        self.__check_postgresql_connection()
        try:
            self._logger.info(f"Truncating table '{table_name}'...")
            with self._connection.cursor() as cursor:
                query = f"TRUNCATE TABLE {table_name}"
                query += ';'
                self._logger.info("Running query::"+query)
                cursor.execute(query)
                self.commit()
                self._logger.info(f"Successfully truncated table '{table_name}'.")
        except Exception as e:
            self.disconnect()
            self._logger.error(f"Error truncating table '{table_name}': {str(e)}")
            raise

    def set_spark_session(self, spark_session: SparkSession) -> None:
        """
        Set SparkSession for PostgreSQL.

        Args:
            spark_session (SparkSession): The SparkSession instance.
        """
        self._spark_session = spark_session
        self._logger.info("Successfully set SparkSession for PostgreSQL ...")

    def read_data_from_postgres_by_spark(self, query: str) -> DataFrame:
        """
        Read data from PostgreSQL database using PySpark and SQL query.

        Args:
            query (str): Running PostgreSQL Query.

        Returns:
            DataFrame: PySpark DataFrame.
        """
        if self._spark_session is None:
            raise Exception("SparkSession is None.")

        try:
            self._logger.info(f"Running query by Spark: '{query}'...")

            postgre_df = (
                self._spark_session.read.format("jdbc")
                .option("url", f"jdbc:postgresql://{self._host}:{self._port}/{self._database_name}")
                .option("user", f"{self._user_name}")
                .option("password", f"{self._password}")
                .option("driver", "org.postgresql.Driver")
                .option("query", f"{query}")
                .load()
            )

            self._logger.info(f"Successfully ran query by Spark: '{query}'...")
            return postgre_df
        except Exception as e:
            self._logger.error(f"Error running PostgreSQL query by Spark: {str(e)}")
            raise
