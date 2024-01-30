import logging
from datetime import datetime, timedelta

import pandas as pd
from pyspark.sql import SparkSession, DataFrame

from hdfs import InsecureClient


class HDFSHelper:
    def __init__(self, spark_session: SparkSession, logger: logging.Logger):
        """
        Initializes the HDFSHelper with a SparkSession.

        Parameters:
        - spark_session (SparkSession): The SparkSession to be used for HDFS operations.
        """
        self._hdfs_user = None
        self._spark_session = spark_session
        self._hdfs_uri = None
        self._hdfs_client = None

        self._logger = logger

    # ------------------------------------ #
    #         Checking Operations          #
    # ------------------------------------ #
    def __check_hadoop_is_setup(self):
        """
        Check if Hadoop is properly set up by verifying the HDFS URI.
        """
        # self._logger.info("Hadoop setup is checking...")
        if not self._hdfs_uri:
            raise Exception("Hadoop is not set up. Set Hdfs Gateway URI and run setup_hadoop().")

    def __check_spark_session_is_started(self):
        """
        Check if the Spark session is started.
        """
        # self._logger.info("Spark session status is checking...")

        if not self._spark_session:
            raise Exception("Spark session is None. Set a running spark session.")

    def __check_hdfs_client_is_set(self):
        """
        Check if the HDFS client is set.
        """
        # self._logger.info("HDFS Client is checking...")

        if not self._hdfs_client:
            raise Exception("Hdfs Client is None. Set an HDFS client.")

    # ------------------------------------ #
    #           Set Operations             #
    # ------------------------------------ #
    def set_spark_session(self, spark_session: SparkSession):
        """
        Set the SparkSession for HDFS operations.

        Parameters:
        - spark_session (SparkSession): The SparkSession to be set.
        """
        self._spark_session = spark_session

    def set_hdfs_client_using_insecure(self, uri: str, user: str):
        """
        Set the HDFS client using the configured HDFS URI and user.
        """
        self._hdfs_uri, self._hdfs_user = uri, user
        self._hdfs_client = InsecureClient(self._hdfs_uri, user=self._hdfs_user)

    def set_hdfs_client_using_spark(self, uri: str):
        """
        Set up Hadoop with required configurations using the SparkContext.
        """
        self._hdfs_uri = uri
        if not self._spark_session:
            raise Exception("SparkSession is None.")

        self._sc = self._spark_session.sparkContext
        self._gateway_URI = self._sc._gateway.jvm.java.net.URI
        self._hadoop_path = self._sc._gateway.jvm.org.apache.hadoop.fs.Path
        self._hadoop_fs = self._sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        self._hadoop_conf = self._sc._gateway.jvm.org.apache.hadoop.conf.Configuration

        self._hadoop_active_fs = self._hadoop_fs.get(self._gateway_URI(self._hdfs_uri), self._hadoop_conf())

    # ------------------------------------ #
    #   Hdfs operations by SparkContext    #
    # ------------------------------------ #
    def is_file_exist(self, hdfs_filepath: str) -> bool:
        return self._hadoop_active_fs.exists(self._hadoop_path(hdfs_filepath))

    def delete_file(self, hdfs_filepath: str):
        if self.is_file_exist(hdfs_filepath):
            self._hadoop_active_fs.delete(self._hadoop_path(hdfs_filepath))
            self._logger.info(f"File is removed from hdfs :: {hdfs_filepath}")
        else:
            self._logger.warning(f"File is not exist in hdfs :: {hdfs_filepath}.")

    def __get_status_list(self, hdfs_filepath: str) -> list:
        """
        Get the status list of files in the specified HDFS path.

        Parameters:
        - hdfs_filepath (str): The HDFS file path.

        Returns:
        - list: List of file statuses.
        """
        # self._logger.info("Getting the status list for HDFS path: %s", hdfs_filepath)
        file_exist = self.is_file_exist(hdfs_filepath=hdfs_filepath)
        if file_exist:
            status_list = self._hadoop_active_fs.listStatus(self._hadoop_path(hdfs_filepath))
        else:
            status_list = []
        return status_list

    def __get_file_status_details(self, file_status) -> list:
        """
        Map HDFS file status to DataFrame columns.

        Parameters:
        - file_status: HDFS file status.

        Returns:
        - list: List containing file modification_time, size, and path.
        """
        file_length = int(file_status.getLen())
        file_path = file_status.getPath().toString()
        modification_time = int(str(file_status.getModificationTime())[:10])
        modification_time = datetime.fromtimestamp(modification_time)

        return [modification_time, file_length, file_path]

    def get_all_files_details(self, hdfs_filepath: str) -> pd.DataFrame:
        """
        Get a DataFrame containing file information for the specified HDFS path.

        Parameters:
        - hdfs_filepath (str): The HDFS file path.

        Returns:
        - pd.DataFrame: DataFrame with columns ['modification_time', 'file_size', 'file'].
        """
        self.__check_hadoop_is_setup()
        status_list = self.__get_status_list(hdfs_filepath)
        data = list(map(self.__get_file_status_details, status_list))
        df_files = pd.DataFrame(data=data, columns=['modification_time', 'file_size', 'file'])
        # self._logger.info("[INFO] Total files are found. Count: %s", df_files.shape[0])
        return df_files

    def __sort_and_filter_files(self, df: pd.DataFrame, keyword: str, sort_by: str, asc: bool) -> pd.DataFrame:
        """
        Sort and filter files in the DataFrame based on keyword and sorting options.

        Parameters:
        - df (pd.DataFrame): DataFrame containing file information.
        - keyword (str): Search patterns for folders or files.
        - sort_by (str): Sorting keyword; it can be 'modification_time' or 'file_size'.
        - asc (bool): Data will be sorted in ascending order if True.

        Returns:
        - pd.DataFrame: Sorted and filtered DataFrame.
        """
        # self._logger.info("Sorting and filtering files by keyword: %s, sort_by: %s, ascending: %s", keyword, sort_by,asc)
        df = df.sort_values([sort_by], ascending=asc)
        if keyword:
            return df[df['file'].str.contains(keyword)]
        else:
            return df

    def __filter_by_date(self, df: pd.DataFrame, date: datetime, before_date: datetime,
                         after_date: datetime) -> pd.DataFrame:
        """
        Filter DataFrame by date.

        Parameters:
        - df (pd.DataFrame): DataFrame containing file information.
        - date (datetime): Returns all CSV file paths for the given date.
        - before_date (datetime): Returns all CSV file paths before the given date.
        - after_date (datetime): Returns all CSV file paths after the given date.

        Returns:
        - pd.DataFrame: Filtered DataFrame.
        """
        # self._logger.info("Filtering files by date: %s, before_date: %s, after_date: %s", date, before_date, after_date)
        if date is not None:
            df = df[df.modification_time == date]
        if before_date is not None:
            df = df[df.modification_time < before_date]
        if after_date is not None:
            df = df[df.modification_time > after_date]

        return df

    def __select_csv_files(self, df: pd.DataFrame, csv: str, file_num: int) -> list:
        """
        Select CSV files based on the specified criteria.

        Parameters:
        - df (pd.DataFrame): DataFrame containing file information.
        - csv (str): Which CSV filepaths will be returned; it can be 'first', 'all', 'date', 'file_num'.
        - file_num (int): Number of files to return if csv is set to 'file_num'.

        Returns:
        - list: List of CSV file paths.
        """
        # self._logger.info("Selecting CSV files by criteria: csv: %s, file_num: %s", csv, file_num)
        if csv == 'first':
            return [df.iloc[0].file] if not df.empty else []
        elif csv == 'file_num':
            return df.head(file_num).file.tolist()
        elif csv == 'all' or csv == 'date':
            return df.file.tolist()
        else:
            raise ValueError("[ERROR] Given csv parameter is not defined for the function. Given csv parameter:", csv,
                             ".It should be 'first', 'all', 'date', or 'file_num'")

    def get_filenames(self,
                      hdfs_filepath: str,
                      keyword: str = '',
                      sort_by: str = 'modification_time',
                      asc: bool = False,
                      csv: str = 'first',
                      file_num: int = 1,
                      date: datetime = None,
                      before_date: datetime = None,
                      after_date: datetime = None) -> list | DataFrame:
        """
        Get CSV paths for files in the given HDFS filepath.

        Parameters:
        - hdfs_filepath (str): HDFS filepath to search for files.
        - keyword (str): Search patterns for folders or files.
        - sort_by (str): Sorting keyword; it can be 'datetime' or 'file_size'.
        - asc (bool): Data will be sorted in ascending order if True.
        - csv (str): Which CSV filepaths will be returned; it can be 'first', 'all', 'date', 'file_num'.
        - file_num (int): Number of files to return if csv is set to 'file_num'.
        - date (datetime): Returns all CSV file paths for the given date.
        - before_date (datetime): Returns all CSV file paths before the given date.
        - after_date (datetime): Returns all CSV file paths after the given date.

        Returns:
        - list: List of CSV file paths.
        """
        self._logger.info(
            "Files are searching... >> Hdfs Filepath:%s\nKeyword:%s || Sort By: %s || Ascending: %s || Csv: %s || Date: %s || "
            "Before Date: %s || After Date: %s", hdfs_filepath, keyword, sort_by, asc, csv, date,
            before_date, after_date)

        df = self.get_all_files_details(hdfs_filepath)

        if df.empty:
            return df
            # raise FileNotFoundError(f"No file exists for given HDFS filepath {hdfs_filepath}")

        # self._logger.info("Count of files with keyword: %s", df.shape[0])

        if csv == 'date':
            df = self.__filter_by_date(df, date, before_date, after_date)

        df = self.__sort_and_filter_files(df, keyword, sort_by, asc)
        filepaths = self.__select_csv_files(df, csv, file_num)

        # self._logger.info("Final files found count: %s", len(filepaths))
        return filepaths

    # ------------------------------------ #
    #   Hdfs operations by SparkSession    #
    # ------------------------------------ #

    def read_file(self, hdfs_filepath: str, file_type: str = 'csv',
                  encoding: str = "ISO-8859-9", header='true', delimiter=',') -> DataFrame:
        """
        Read a file from HDFS into a PySpark DataFrame.

        Args:
            hdfs_filepath (str): HDFS file path.
            encoding (str): Encoding of the file (default is ISO-8859-9).

        Returns:
            DataFrame: PySpark DataFrame.
        """
        self._logger.info("Reading file from HDFS :: %s", hdfs_filepath)
        self.__check_spark_session_is_started()
        df = self._spark_session.read.format(file_type)

        if file_type not in ['csv', 'parquet']:
            raise Exception("Undefined file type. File types should be csv or parquet")

        if file_type == 'csv':
            df = df.option("header", header) \
                .option("mode", "DROPMALFORMED") \
                .option("delimiter", delimiter)

        if encoding:
            df = df.option('encoding', encoding)

        df = df.load(hdfs_filepath)

        if df.head(1):
            self._logger.info("Ok >> Read file from HDFS :: %s", hdfs_filepath)
        else:
            self._logger.warning("Fail >> Read file from HDFS :: %s", hdfs_filepath)

        return df

    def write_file(self, data: DataFrame, hdfs_filepath: str, mode: str = 'overwrite',
                   file_type: str = 'csv') -> None:
        """
        Write a PySpark DataFrame to HDFS.

        Args:
            data (DataFrame): PySpark DataFrame to write.
            hdfs_filepath (str): HDFS file path.
            mode (str): Specifies the behavior of the save operation (default is 'overwrite').
            file_type (str): The file_type used to save the DataFrame (default is 'csv').
        """
        self._logger.info("Writing file to HDFS :: %s", hdfs_filepath)
        self.__check_spark_session_is_started()

        if file_type not in ['csv', 'parquet']:
            raise Exception("Undefined file type. File types should be csv or parquet")

        try:
            data.write.mode(mode).format(file_type).save(hdfs_filepath)
            self._logger.info("Ok >> Write file from HDFS :: %s", hdfs_filepath)
        except Exception as e:
            self._logger.info("Fail >> Write file from HDFS :: %s", hdfs_filepath)
            raise Exception("Fail >> Write file from HDFS :: %s", hdfs_filepath)

    def delete_old_files(self, filepath: str, keyword='', date: datetime = None, before_date: datetime = None,
                         after_date: datetime = None):
        if not all((date, before_date, after_date)):
            before_date = datetime.now() - timedelta(days=1)

        filepaths = self.get_filenames(hdfs_filepath=filepath, keyword=keyword, csv='date',
                                       before_date=before_date, after_date=after_date, date=date)

        for hdfs_filepath in filepaths:
            file_exist = self.is_file_exist(hdfs_filepath=filepath)
            if file_exist:
                self.delete_file(hdfs_filepath=hdfs_filepath)

    # ------------------------------------ #
    #   Hdfs operations by HDFS Client     #
    # ------------------------------------ #

    def create_directory_with_hdfs_client(self, path: str):
        """
        Creates a directory in HDFS.

        Args:
            path (str): The HDFS path for the new directory.

        Example:
            hdfs_module.create_directory('/user/my_user/new_directory')
        """

        self.__check_hdfs_client_is_set()
        self._logger.info("Create directory in HDFS :: %s", path)
        self._hdfs_client.makedirs(path)

    def upload_file_with_hdfs_client(self, local_path: str, hdfs_path: str):
        """
        Uploads a file from the local filesystem to HDFS.

        Args:
            local_path (str): The local path of the file.
            hdfs_path (str): The destination path in HDFS.

        Example:
            hdfs_module.upload_file('/local/path/myfile.txt', '/user/my_user/hdfs_path/myfile.txt')
        """
        self.__check_hdfs_client_is_set()
        self._logger.info("Upload file from local  '%s' to HDFS '%s'", local_path, hdfs_path)
        with open(local_path, 'rb') as local_file:
            self._hdfs_client.write(hdfs_path, local_file)
            self._logger.info("Successfully uploaded file to HDFS :: %s", hdfs_path)

    def download_file_with_hdfs_client(self, hdfs_path: str, local_path: str):
        """
        Downloads a file from HDFS to the local filesystem.

        Args:
            hdfs_path (str): The path of the file in HDFS.
            local_path (str): The local destination path.

        Example:
            hdfs_module.download_file('/user/my_user/hdfs_path/myfile.txt', '/local/path/myfile.txt')
        """
        self.__check_hdfs_client_is_set()
        self._logger.info("Download file from HDFS '%s' to local '%s'", hdfs_path, local_path)
        with self._hdfs_client.read(hdfs_path) as hdfs_file, open(local_path, 'wb') as local_file:
            local_file.write(hdfs_file.read())
            self._logger.info("Successfully downloaded file to HDFS :: %s", hdfs_path)

    def delete_file_with_hdfs_client(self, hdfs_path: str):
        """
        Deletes a file from HDFS.

        Args:
            hdfs_path (str): The path of the file in HDFS.

        Example:
            hdfs_module.delete_file('/user/my_user/hdfs_path/myfile.txt')
        """
        self.__check_hdfs_client_is_set()
        self._logger.info("Delete file from HDFS :: %s", hdfs_path)
        self._hdfs_client.delete(hdfs_path)
        self._logger.info("Successfully deleted file to HDFS :: %s", hdfs_path)

    def read_file_from_hdfs_with_client(self, hdfs_filepath: str) -> bytes:
        """
        Read a file from HDFS using the HDFS client.

        Args:
            hdfs_filepath (str): HDFS file path.

        Returns:
            bytes: File content as bytes.
        """
        self.__check_hdfs_client_is_set()
        self._logger.info("Read file from HDFS :: %s", hdfs_filepath)
        try:
            with self._hdfs_client.read(hdfs_filepath) as reader:
                content = reader.read()
                self._logger.info("Successfully read file from HDFS :: %s", hdfs_filepath)
                return content
        except Exception as e:
            self._logger.error("Failed to read file from HDFS :: %s", str(e))
            raise

    def write_file_to_hdfs_with_client(self, local_filepath: str, hdfs_filepath: str) -> None:
        """
        Write a file to HDFS using the HDFS client.

        Args:
            local_filepath (str): Local file path.
            hdfs_filepath (str): HDFS file path.
        """
        self.__check_hdfs_client_is_set()
        self._logger.info("Write file to HDFS :: %s", hdfs_filepath)
        with open(local_filepath, 'rb') as local_file:
            self._hdfs_client.write(hdfs_filepath, local_file)
            self._logger.info("Successfully wrote file to HDFS :: %s", hdfs_filepath)
