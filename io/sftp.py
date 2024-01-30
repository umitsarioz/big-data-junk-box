import paramiko
import logging
import pandas as pd
from os import path
from datetime import datetime


class SFTPHelper:
    def __init__(self, logger: logging.Logger, host: str = None, user_name: str = None, password: str = None):
        """
        Initializes an SFTPHelper instance.

        Args:
            logger (logging.Logger, optional): Logger instance. Defaults to None.
            host (str, optional): SFTP server host. Defaults to None.
            user_name (str, optional): SFTP username. Defaults to None.
            password (str, optional): SFTP password. Defaults to None.
        """
        self._transport = None
        self._sftp = None
        self._host = host
        self._user_name = user_name
        self._password = password
        self._logger = logger

    def get_stfp_object(self) -> paramiko.sftp_client.SFTPClient:
        """
        Get the SFTP client object.

        Returns:
            paramiko.sftp_client.SFTPClient: SFTP client object.
        """
        return self._sftp

    def get_transport_object(self) -> paramiko.transport.Transport:
        """
        Get the transport object.

        Returns:
            paramiko.transport.Transport: Transport object.
        """
        return self._transport

    def get_connection_parameters(self) -> dict:
        """
        Get the connection parameters for the SFTP connection.

        Returns:
            dict: Connection parameters (username, password, host).
        """
        return {'user_name': self._user_name, 'password': self._password, 'host': self._host}

    def set_connection_parameters(self, host: str, user_name: str, password: str):
        """
        Set the connection parameters for the SFTP connection.

        Args:
            host (str): Host to set.
            user_name (str): Username to set.
            password (str): Password to set.
        """
        self._host = host
        self._user_name = user_name
        self._password = password

    def close_connections(self):
        """
        Closes the SFTP and Transport connections.

        Logs an informational message about the attempt to close connections.
        """
        self._logger.info("Trying to close SFTP and Transport Connections...")

        for connection, connection_name in [(self._transport, "Transport"), (self._sftp, "SFTP")]:
            if connection:
                connection.close()
                self._logger.info(f"{connection_name} connection is closed successfully.")
            else:
                self._logger.info(f"{connection_name} connection is already closed.")

    def connect_sftp(self):
        """
        Connects to the SFTP server using the provided connection parameters.

        Raises:
            Exception: If connection parameters are not defined properly or there is an error during connection.
        """
        self._logger.info("Trying to connect SFTP client...")

        conn_parameters_are_defined = all((self._host, self._user_name, self._password))

        if not conn_parameters_are_defined:
            error_message = (f"Connection parameters are not defined properly. "
                             f"Parameters: host:{self._host} | Username: {self._user_name} | "
                             f"Password: {self._password}")
            self._logger.error(error_message)
            raise Exception(error_message)

        try:
            self._transport = paramiko.Transport((self._host))
            self._transport.connect(None, self._user_name, self._password)
            self._sftp = paramiko.SFTPClient.from_transport(self._transport)
            self._logger.info("SFTP Client is connected successfully.")
        except Exception as e:
            self._logger.error(f"SFTP Client Connection Error: {str(e)}")
            self.close_connections()
            raise

    def get_filepaths(self, root_filepath: str, keyword='', csv='first', file_count=1) -> pd.DataFrame:
        """
        Get file paths and attributes from the SFTP server.

        Args:
            root_filepath (str): The root path on the SFTP server.
            keyword (str, optional): Keyword to filter files by name. Defaults to ''.
            csv (str, optional): Type of CSV output, 'first' or 'last'. Defaults to 'first'.
            file_count (int, optional): Number of files to retrieve. Defaults to 0 (all files).

        Returns:
            pd.DataFrame: A DataFrame containing file attributes.
        """
        self._logger.info(f"Getting file paths and attributes from {root_filepath}...")

        files_with_attributes = sorted(self._sftp.listdir_attr(root_filepath), key=lambda f: f.st_mtime, reverse=True)

        get_mod_datetime = lambda f: datetime.strftime(datetime.fromtimestamp(f.st_mtime), '%Y-%m-%d %H:%M:%S')
        get_abs_path = lambda f: path.join(root_filepath, f.filename)
        get_file_type = lambda f: f.filename.split('.')[-1]
        get_file_name = lambda f: f.filename
        get_file_size = lambda f: f.st_size

        files_names_all = [{'file_type': get_file_type(f),
                            'file_name': get_file_name(f),
                            'file_size': get_file_size(f),
                            'abs_path': get_abs_path(f),
                            'modified_time': get_mod_datetime(f)} for f in files_with_attributes]

        if keyword:
            files_names_all = [f for f in files_names_all if keyword in f.get('file_name')]

        if file_count > 1:
            files_names_all = files_names_all[:file_count]

        if csv == 'first':
            files_names_all = [files_names_all[0]]
        elif csv == 'last':
            files_names_all = [files_names_all[-1]]

        cols = ["file_type", "file_name", "file_size", "abs_path", "modified_time"]
        df = pd.DataFrame(data=[[None] * len(cols)], columns=cols)
        if files_names_all:
            df = pd.DataFrame.from_records(files_names_all)

        return df.dropna()

    def read_file(self, filepath: str, file_type: str = 'csv', sep: str = ',', encoding: str = 'ISO-8859-9',
                  low_memory: bool = False) -> pd.DataFrame:
        """
        Reads a file from the SFTP server.

        Args:
            filepath (str): The path to the file on the SFTP server.
            file_type (str, optional): Type of the file, 'csv' or 'parquet'. Defaults to 'csv'.
            sep (str, optional): Delimiter for CSV files. Defaults to ','.
            encoding (str, optional): Encoding for CSV files. Defaults to 'ISO-8859-9'.
            low_memory (bool, optional): Whether to use low memory mode for reading CSV files. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing the data from the file.
        """
        self._logger.info(f"Reading file from {filepath}...")

        df = pd.DataFrame()
        try:
            with self._sftp.open(filepath) as f:
                f.prefetch()
                if file_type == 'csv':
                    df = pd.read_csv(f, sep=sep, low_memory=low_memory, encoding=encoding)
                elif file_type == 'parquet':
                    df = pd.read_parquet(f)
                else:
                    raise Exception("Undefined file type: ", file_type)
        except Exception as e:
            self._logger.error(f"SFTP File Reading Exception: {str(e)}")
            raise
        finally:
            return df
