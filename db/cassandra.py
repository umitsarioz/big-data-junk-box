import logging
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, ConsistencyLevel
from datetime import datetime
from typing import List


class CassandraHelper:
    """
    A helper class for interacting with Cassandra using PySpark and Cassandra Python Driver.
    """

    def __init__(self, spark_session: SparkSession, logger: logging.Logger):
        """
        Initializes the CassandraHelper class.

        Args:
            spark_session (SparkSession): The Spark session.
            logger (logging.Logger): Logger object for logging messages.
        """
        self._logger = logger
        self._spark_session = spark_session
        self._contact_points, self._host, self._port = None, None, None
        self._user_name, self._password = None, None
        self._auth_provider, self._cassandra_cluster = None, None
        self._cassandra_session, self._cluster_keyspace = None, None

    def __set_auth_provider(self):
        if self._user_name and self._password:
            self._auth_provider = PlainTextAuthProvider(username=self._user_name, password=self._password)
        else:
            raise Exception("Username or password is None")

    def __set_cassandra_cluster(self):
        if self._auth_provider and self._contact_points and self._port:
            self._cassandra_cluster = Cluster(self._contact_points,
                                              port=self._port,
                                              auth_provider=self._auth_provider) # lbp = None, 4 olark eklendi
        else:
            raise Exception("Contact points or port is None")

    def get_all_parameters(self) -> dict:
        """Get all connection parameters as a dictionary"""
        return {
            'cluster_keyspace': self._cluster_keyspace,
            'host': self._host,
            'port': self._port,
            'user_name': self._user_name,
            'password': self._password,
            'contact_points': self._contact_points
        }

    def set_all_parameters(self, host: str, port: int, user_name: str, password: str, contact_points: List[str],
                           cluster_keyspace: str = "conferences"):
        """
        Set all parameters individually.

        Args:
            host (str): Cassandra host.
            port (int): Cassandra port.
            user_name (str): Cassandra username.
            password (str): Cassandra password.
            contact_points (list): List of Cassandra contact points.
            cluster_keyspace (str): Cassandra cluster keyspace.
        """
        self._host = host
        self._port = port
        self._user_name = user_name
        self._password = password
        self._contact_points = contact_points
        self._cluster_keyspace = cluster_keyspace
        self.__set_auth_provider()
        self.__set_cassandra_cluster()

    def to_initialize_cluster(self, timeout=30, fetch_size=None) -> None:
        """
        Initializes the Cassandra cluster and session.

        Args:
            timeout (int, optional): The timeout for the Cassandra session. Defaults to 30 seconds.
            fetch_size (int, optional): The fetch size for the Cassandra session. Defaults to None.

        Raises:
            Exception: If the Cassandra cluster session is already started.
        """
        if self._cassandra_session is None:
            self._cassandra_session = self._cassandra_cluster.connect(self._cluster_keyspace)
            # Session Settings
            self._cassandra_session.row_factory = lambda cols, rows: pd.DataFrame(rows, columns=cols)
            self._cassandra_session.default_timeout = timeout
            self._cassandra_session.default_fetch_size = fetch_size
            self._logger.info("Cassandra cluster is initialized...")
        else:
            self._logger.info("Cassandra cluster is already initialized...")

    def to_terminate_cluster(self) -> bool:
        """
        Terminates the Cassandra cluster.

        Returns:
            bool: True if termination is successful, False otherwise.
        """
        res = False
        try:
            self._cassandra_cluster.shutdown()
            self._logger.info("Cassandra cluster is terminated.")
            res = True
        except:
            self._logger.warning("Cassandra cluster is still open!")
        finally:
            return res

    def execute_query(self, query: str) -> pd.DataFrame:
        """
        Executes a query on the Cassandra cluster.

        Args:
            query (str): The CQL query to execute.

        Returns:
            pd.DataFrame: The result of the query as a DataFrame.
        """
        if self._cassandra_session is None:
            raise Exception(
                "Cassandra cluster session is not started. You need to initialize before executing a query!")

        try:
            self._logger.info("Running query: %s", query)
            query = SimpleStatement(query, consistency_level=ConsistencyLevel.QUORUM)
            result = self._cassandra_session.execute(query)
            dataframe = result._current_rows
            return dataframe
        except Exception as e:
            self._logger.error("Exception is occured when executing query: " + query + "\nThrowback: "+str(e))
            raise

    def truncate_table(self, keyspace: str, table: str) -> None:
        """
        Truncates a table in the Cassandra cluster.

        Args:
            keyspace (str): The keyspace of the table.
            table (str): The table to truncate.
        """
        query = f"TRUNCATE {keyspace}.{table};"
        _ = self.execute_query(query)
        self._logger.info("Table truncated.")

    def delete_from_table(self, keyspace: str, table: str, where_clause: str) -> None:
        """
        Deletes rows from a table in the Cassandra cluster.

        Args:
            keyspace (str): The keyspace of the table.
            table (str): The table to delete from.
            where_clause (str): The WHERE clause for the delete operation.
        """
        query = f"delete from {keyspace}.{table} where {where_clause}"
        _ = self.execute_query(query)
        self._logger.info("Rows deleted.")

    def select_from_table(self, keyspace: str, table: str, cols: list, where_clause=None, limit=None) -> pd.DataFrame:
        """
        Selects rows from a table in the Cassandra cluster.

        Args:
            keyspace (str): The keyspace of the table.
            table (str): The table to select from.
            cols (list): The columns to select.
            where_clause (str, optional): The WHERE clause for the select operation. Defaults to None.
            limit (int, optional): The LIMIT clause for the select operation. Defaults to None.

        Returns:
            pd.DataFrame: The result of the select query as a DataFrame.
        """
        cols = ','.join(cols)
        query = f"select {cols} from {keyspace}.{table}"

        if where_clause is not None:
            query += f" where {where_clause} ALLOW FILTERING;"

        if limit is not None:
            query += f' limit {limit}'

        return self.execute_query(query)

    def __check_spark_session_is_started(self):
        """
        Check if the Spark session is started.
        """
        self._logger.info("Spark session status is checking...")

        if not self._spark_session:
            raise Exception("Spark session is None. Set a running spark session.")

    def set_spark_session(self, spark_session: SparkSession):
        """
        Set the SparkSession for HDFS operations.

        Parameters:
        - spark_session (SparkSession): The SparkSession to be set.
        """
        self._spark_session = spark_session

    def read_table_by_spark(self, keyspace: str, table: str) -> DataFrame:
        """
        Reads a table from the Cassandra cluster into a Spark DataFrame.

        Args:
            keyspace (str): The keyspace of the table.
            table (str): The table to read.

        Returns:
            DataFrame: The Spark DataFrame containing the data from the Cassandra table.
            bool: False if an exception occurs.
        """
        self.__check_spark_session_is_started()
        try:
            self._logger.info(f"Reading {keyspace}.{table}")
            df = self._spark_session.read.format("org.apache.spark.sql.cassandra") \
                .options(table=table, keyspace=keyspace) \
                .load()

            self._logger.info("Read successfully!")
            return df
        except Exception as e:
            self._logger.error(
                f"Exception when reading data from Cassandra (Source: {keyspace}.{table})\nError:{str(e)}")
            raise

    def write_table_by_spark(self, df: DataFrame, keyspace: str, table: str, mode="append") -> bool:
        """
        Writes data to a table in the Cassandra cluster using a Spark DataFrame.

        Args:
            df (DataFrame): The Spark DataFrame to be written.
            keyspace (str): The keyspace of the table.
            table (str): The table to write to.
            mode (str, optional): The writing mode, 'append' or 'overwrite'. Defaults to 'append'.

        Returns:
            bool: True if write is done successfully, False otherwise.
        """
        self.__check_spark_session_is_started()
        res = False

        try:
            self._logger.info(f"Writing data to {keyspace}.{table} as {mode} mode.")
            if mode == 'append':
                df.write.format("org.apache.spark.sql.cassandra") \
                    .options(table=table, keyspace=keyspace) \
                    .save(mode='append')

                res = True

            elif mode == 'overwrite':
                df.write.format("org.apache.spark.sql.cassandra") \
                    .option("confirm.truncate", "true") \
                    .options(table=table, keyspace=keyspace) \
                    .save(mode='overwrite')

                res = True
            else:
                self._logger.warning("Undefined mode! Mode should be 'append' or 'overwrite'. It cannot be %s", mode)
        except Exception as e:
            self._logger.error(
                f"Exception when writing data to Cassandra (Destination:{keyspace}.{table}).\nError:{str(e)}")
            raise
        finally:
            if res:
                self._logger.info("Write successfully!")
            return res

    def check_data_is_already_exist_in_db(self, table: str, keyspace: str, time_limit='24 hours', debug=False) -> bool:
        """
        Check if data already exists in the specified table within the given time limit.

        :param table: The name of the table to check.
        :param keyspace: The keyspace of the table.
        :param time_limit: Time limit for considering data as 'already exist', default is '24 hours'.
        :param debug: If True, debug information will be logged.
        :return: True if data already exists within the time limit, False otherwise.
        """
        time_limit_els = time_limit.split()  # limit sayısı, limit tipi
        limit_rate = int(time_limit_els[0])
        limit_suffix = time_limit_els[1]

        dt_now = datetime.now()
        is_already_exist = False

        query_result_pd = self.select_from_table(keyspace=keyspace, table=table, cols=["created_at"], limit=1)
        if not query_result_pd.empty:
            created_at = query_result_pd.created_at.iloc[0]
            if created_at:
                if debug:
                    self._logger.info("Datetime Now: %s\nCreated At: %s", dt_now, created_at)

                if limit_suffix in ('day', 'days'):
                    curr_rate = abs((dt_now - created_at).days)
                    limit_rate = limit_rate * 1
                    is_already_exist = curr_rate < limit_rate
                    if debug:
                        self._logger.info("Limit: %s | curr_rate < limit_rate: %s < %s == %s",
                                          time_limit, curr_rate, limit_rate, is_already_exist)
                elif limit_suffix in ('second', 'seconds'):
                    curr_rate = abs((dt_now - created_at).total_seconds())
                    is_already_exist = curr_rate < limit_rate
                    limit_rate = limit_rate * 1
                    if debug:
                        self._logger.info("Limit: %s | curr_rate < limit_rate: %s < %s == %s",
                                          time_limit, curr_rate, limit_rate, is_already_exist)
                elif limit_suffix in ('minute', 'minutes'):
                    curr_rate = abs((dt_now - created_at).total_seconds())
                    limit_rate = limit_rate * 60
                    is_already_exist = curr_rate < limit_rate
                    if debug:
                        self._logger.info("Limit: %s | curr_rate < limit_rate: %s < %s == %s",
                                          time_limit, curr_rate, limit_rate, is_already_exist)
                elif limit_suffix in ('hour', 'hours'):
                    curr_rate = abs((dt_now - created_at).total_seconds())
                    limit_rate = limit_rate * 60 * 60
                    is_already_exist = curr_rate < limit_rate
                    if debug:
                        self._logger.info("Limit: %s | curr_rate < limit_rate: %s < %s == %s",
                                          time_limit, curr_rate, limit_rate, is_already_exist)
                else:
                    self._logger.error("Undefined time limit error :: given time limit is ", time_limit)
                    raise Exception("Undefined time limit error :: given time limit is ", time_limit)

        self._logger.info("Table: %s.%s | Time Limit: %s || Is Exist :: %s", keyspace, table, time_limit,
                          is_already_exist)
        return is_already_exist
