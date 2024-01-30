from pyspark.sql import SparkSession, DataFrame
import logging
from typing import Union
import time


class SparkHelper:
    """
    A utility class for managing and interacting with Spark sessions.
    """

    def __init__(self, logger: logging.Logger):
        """
        Initialize SparkHelper.

        Args:
            logger (Logger, optional): Logger object for logging. Defaults to None.
        """
        self._logger = logger
        self._spark_session = None
        self._parameters = {
            'executor.instances': None,
            'executor.memory': None,
            'executor.cores': None,
            'driver.memory': None,
            'driver.maxResultSize': None,
            'hadoop.fs.defaultFS': None,
            'sql.legacy.timeParserPolicy': None,
            'cassandra.connection.host': None,
            'cassandra.connection.port': None,
            'cassandra.auth.username': None,
            'cassandra.auth.password': None,
            'delimiter': ';',
            'header': 'true',
            'tout': 'df',
        }

    def get_parameter(self, key: str):
        return self._parameters.get(key)

    def __check_parameters_set_properly(self):
        """
        Check if all parameters are set with defined proper types.

        Raises:
            Exception: If the parameter set definition is wrong.
        """
        type_dct = {'executor.cores': int, 'executor.memory': str, 'executor.instances': int,
                    'driver.memory': str, 'driver.maxResultSize': str, 'hadoop.fs.defaultFS': str,
                    'cassandra.connection.host': str, 'cassandra.connection.port': int,
                    'cassandra.auth.username': str, 'cassandra.auth.password': str
                    }

        for k, v in type_dct.items():
            val = self.get_parameter(k)
            if not isinstance(val, v):
                raise Exception(f"Parameter type error: {k} = {val}. It should be {v} type.")

    def set_all_parameters(self, executor_core: int, executor_instance: int, executor_memory: str,
                           driver_memory: str, driver_max_result_size: str, hadoop_fs: str,
                           cassandra_host: str, cassandra_port: int, cassandra_user_name: str, cassandra_password: str,
                           additional_params: dict = None):
        """
        Set all parameters using a dictionary.

        Notes:
            additional_params may be :{'sql.shuffle.partitions': '300', # for dataframe
                                        'default.parallelism': '300'} # for rdd
        """
        if additional_params is None:
            additional_params = dict()

        self._logger.info('Spark parameters are set.')
        params = {
            'executor.cores': executor_core,
            'executor.memory': executor_memory,
            'executor.instances': executor_instance,
            'driver.memory': driver_memory,
            'driver.maxResultSize': driver_max_result_size,
            'hadoop.fs.defaultFS': hadoop_fs,
            'sql.legacy.timeParserPolicy': 'LEGACY',
            'cassandra.connection.host': cassandra_host,
            'cassandra.connection.port': cassandra_port,
            'cassandra.auth.username': cassandra_user_name,
            'cassandra.auth.password': cassandra_password,
        }

        if additional_params:
            params.update(additional_params)

        self._parameters.update(params)


    def get_all_parameters_as_dict(self) -> dict:
        """
        Get all parameters as a dictionary.

        Returns:
            dict: A dictionary containing all parameters and their values.
        """
        return self._parameters.copy()

    def __is_spark_session_started(self):
        """
        Check if the Spark session is None and raise an exception if it is.
        """
        if self._spark_session is None:
            raise Exception("Spark session is not initialized. Please start the Spark session first.")

    def start_spark_session(self, debug=False):
        """
        Start a Spark session with the given initialization values.
        """
        st = time.time()
        self._logger.info('Spark session is starting..')
        self.__check_parameters_set_properly()

        spark_builder = SparkSession.builder

        for key, value in self._parameters.items():
            spark_builder = spark_builder.config(f"spark.{key}", value)

        self._spark_session = spark_builder.getOrCreate()
        self._logger.info(f'Spark session is started in {time.time() - st:.0f} seconds.')

        if debug:
            self._logger.info("Spark Session Initialization Parameters:\n", self._spark_session.sparkContext.getConf().getAll())

    def get_spark_session_object(self) -> SparkSession:
        """
        Get the SparkSession object.

        Returns:
            SparkSession: The SparkSession object.
        """
        return self._spark_session

    def clear_cache(self):
        """
        Clear the cache of the Spark session.
        """
        if self._spark_session:
            self._spark_session.catalog.clearCache()
        else:
            self._logger.info("SparkSession is already closed.")

    def stop_spark_session(self):
        """
        Stop the Spark session.
        """
        if self._spark_session:
            self._logger.info('Spark session is stopping..')
            self._spark_session.stop()
            self._logger.info('Spark session is stopped..')
        else:
            self._logger.info("SparkSession is already closed.")

    def clear_cache_and_stop_spark_session(self):
        """
        Clear Cache and Stop Spark Session.
        """
        if self._spark_session:
            self.clear_cache()
            self.stop_spark_session()
        else:
            self._logger.info("SparkSession is already closed.")

    def set_file_reading_parameters(self, delimiter=';', header='true', tout='df') -> None:
        """
        Set parameters for reading CSV.

        Args:
            delimiter (str, optional): CSV file delimiter. Defaults to ';'.
            header (str, optional): Boolean indicating whether the CSV file has a header. Defaults to 'true'.
            tout (str, optional): Output type ('df' for DataFrame or 'rdd' for Resilient Distributed Dataset). Defaults to 'df'.
        """
        self._logger.info(f"Set parameters delimiter:{delimiter} | header:{header} | tout:{tout}...")
        params = {'delimiter': delimiter, 'header': header, 'tout': tout}
        self._parameters.update(params)

    def __read_csv_as_dataframe(self, csv_file: str, encoding=None) -> DataFrame:
        """
        Read CSV file into DataFrame.

        Args:
            csv_file (str): Path to the CSV file.
            encoding (str, optional): Encoding of the CSV file.

        Returns:
            DataFrame: The resulting DataFrame.
        """
        spark = self._spark_session

        if encoding:
            return spark.read.format("csv").option("header", self.get_parameter('header')) \
                .option("mode", "DROPMALFORMED").option("delimiter", self.get_parameter('delimiter')) \
                .option('encoding', encoding).load(csv_file)
        else:
            return spark.read.format("csv").option("header", self.get_parameter('header')) \
                .option("mode", "DROPMALFORMED").option("delimiter", self.get_parameter('delimiter')).load(csv_file)

    def __read_csv_as_rdd(self, csv_file: str):
        """
        Read CSV file into RDD.

        Args:
            csv_file (str): Path to the CSV file.

        Returns:
            RDD: The resulting RDD.
        """
        self.__is_spark_session_started()
        return self._spark_session.textFile(csv_file).map(lambda x: x.split(self.get_parameter('delimiter')))

    def read_csv(self, csv_file: str, encoding=None) -> Union[DataFrame, None]:
        """
        Read CSV file into DataFrame or RDD.

        Args:
            csv_file (str): Path to the CSV file.
            encoding (str, optional): Encoding of the CSV file.

        Returns:
            Union[DataFrame, None]: DataFrame or RDD depending on the specified output type.
        """
        self._logger.info("Reading csv :: %s", csv_file)
        self.__is_spark_session_started()

        if self.get_parameter('tout') == 'df':
            return self.__read_csv_as_dataframe(csv_file, encoding)
        elif self.get_parameter('tout') == 'rdd':
            return self.__read_csv_as_rdd(csv_file)
        else:
            self._logger.error("Invalid output type! Type:%s", self.get_parameter('tout'))
            return None

    def write_df_to_local_csv(self, dataframe: DataFrame, output_path: str, options=None) -> None:
        """
        Write a PySpark DataFrame to a local CSV file with customizable options.

        Args:
            dataframe (DataFrame): PySpark DataFrame to be written.
            output_path (str): Local path where the CSV file will be saved.
            options (dict, optional): A dictionary of additional options for DataFrame write.
                Defaults to None.

        Example Usage:
            spark = SparkSession.builder.appName("example").getOrCreate()
            get_csv_data = GetCSVData(spark)
            dataframe = get_csv_data.read_csv("path/to/input.csv")
            write_options = {"delimiter": ";", "mode": "append"}
            get_csv_data.write_df_to_local_csv(dataframe, "path/to/output.csv", options=write_options)
        """
        self._logger.info("Writing DataFrame to local CSV :: %s", output_path)
        self.__is_spark_session_started()

        default_options = {"header": True, "delimiter": ",", "mode": "overwrite"}

        if options:
            default_options.update(options)

        dataframe.write.options(**default_options).csv(output_path)
