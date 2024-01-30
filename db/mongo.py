import logging
import pymongo
from typing import List, Union


class MongoHelper:
    def __init__(self, logger: logging.Logger, database_name: str = None, user_name: str = None, password: str = None,
                 host: str = 'localhost', port: int = 27017):
        """
        Initialize the MongoHelper instance.

        Parameters:
            logger (logging.Logger): Logger instance for logging messages.
            database_name (str): MongoDB database name.
            user_name (str): MongoDB username.
            password (str): MongoDB password.
            host (str): MongoDB host address. Default is 'localhost'.
            port (int): MongoDB server port. Default is 27017.
        """
        self._logger = logger
        self._host = host
        self._port = port
        self._user_name = user_name
        self._password = password
        self._database_name = database_name
        self._client = None
        self._db = None

    def get_all_params(self) -> dict:
        return {
            'host': self._host,
            'port': self._port,
            'user_name': self._user_name,
            'password': self._password,
            'database_name': self._database_name
        }

    def set_all_parameters(self, host: str, port: int, user_name: str, password: str, database_name: str):
        """
        Set all connection parameters.

        Parameters:
            host (str): MongoDB host address.
            port (int): MongoDB server port.
            user_name (str): MongoDB username.
            password (str): MongoDB password.
            database_name (str): MongoDB database name.

        Returns:
            None
        """
        self._host = host
        self._port = port
        self._database_name = database_name
        self._user_name = user_name
        self._password = password

    def _get_connection_string(self) -> str:
        """
        Get MongoDB connection string.

        Returns:
            str: MongoDB connection string.
        """
        if all([self._user_name, self._password, self._host, self._database_name]):
            return f"mongodb+srv://{self._user_name}:{self._password}@{self._host}/{self._database_name}"
        else:
            return f"mongodb://{self._host}:{self._port}/{self._database_name}"

    def connect(self, database_name: str):
        """
        Connect to MongoDB server and database.

        Args:
            database_name (str): Name of the database to connect to.

        Returns:
            None
        """
        try:
            uri = self._get_connection_string()
            self._client = pymongo.MongoClient(uri)
            self._db = self._client[database_name]
            self._logger.info(f"Connected to MongoDB: {uri}")
        except Exception as e:
            self._logger.error(f"Error connecting to MongoDB: {e}")
            raise

    def close_connection(self):
        """Close the connection to MongoDB."""
        try:
            if self._client:
                self._client.close()
                self._logger.info("Connection to MongoDB closed")
        except Exception as e:
            self._logger.error(f"Error closing connection to MongoDB: {e}")
            raise

    def insert_one(self, collection_name: str, document: dict):
        """
        Insert a single document into the specified collection.

        Args:
            collection_name (str): Name of the MongoDB collection.
            document (dict): Document to be inserted.

        Returns:
            None
        """
        try:
            result = self._db[collection_name].insert_one(document)
            self._logger.info(f"Document inserted with ID: {result.inserted_id}")
        except Exception as e:
            self._logger.error(f"Error inserting document: {e}")
            raise

    def insert_many(self, collection_name: str, documents: list):
        """
        Insert multiple documents into the specified collection.

        Args:
            collection_name (str): Name of the MongoDB collection.
            documents (list): List of documents to be inserted.

        Returns:
            None
        """
        try:
            result = self._db[collection_name].insert_many(documents)
            self._logger.info(f"{len(result.inserted_ids)} documents inserted")
        except Exception as e:
            self._logger.error(f"Error inserting documents: {e}")
            raise

    def find_one(self, collection_name: str, query: dict, project: dict, sort: bool, sorting_column: str = "_id",
                 asc: bool = False) -> Union[dict, None]:
        """
        Find a single document in the specified collection based on the query.

        Args:
            collection_name (str): Name of the MongoDB collection.
            query (dict): Query to filter documents.
            project (dict): Projection of fields to include or exclude.
            sort (bool): Whether to sort the result.
            sorting_column (str): Column to sort on (default is "_id").
            asc (bool): Whether to sort in ascending order (default is False).

        Returns:
            Union[dict, None]: The found document or None if not found.
        """
        try:
            if sort:
                sort_order = pymongo.ASCENDING if asc else pymongo.DESCENDING
                result = self._db[collection_name].find_one(query=query, projection=project).sort(sorting_column,
                                                                                                  sort_order)
            else:
                result = self._db[collection_name].find_one(query=query, projection=project)
            return result
        except Exception as e:
            self._logger.error(f"Error finding document: {e}")
            raise

    def find_many(self, collection_name: str, query: dict, project: dict, sort: bool, sort_by: str = "_id",
                  asc: bool = False, limit: int = 0) -> List[dict]:
        """
        Find multiple documents in the specified collection based on the query.

        Args:
            collection_name (str): Name of the MongoDB collection.
            query (dict): Query to filter documents.
            project (dict): Projection of fields to include or exclude.
            sort (bool): Whether to sort the result.
            sort_by (str): Column to sort on (default is "_id").
            asc (bool): Whether to sort in ascending order (default is False).
            limit (int): Limit zero (0) means is no limit.
        Returns:
            List[dict]: List of found documents.
        """
        try:
            if sort:
                sort_order = pymongo.ASCENDING if asc else pymongo.DESCENDING
                if limit == 0:
                    result = self._db[collection_name].find(query=query, projection=project) \
                        .sort(sort_by, sort_order)
                else:
                    result = self._db[collection_name].find(query=query, projection=project) \
                        .sort(sort_by, sort_order).limit(limit)
            else:
                result = self._db[collection_name].find(query=query, projection=project)
            return list(result)
        except Exception as e:
            self._logger.error(f"Error finding documents: {e}")
            raise

    def counts(self, collection_name: str, query: dict) -> int:
        """
        Get the total number of documents in a collection.

        Args:
            collection_name (str): Name of the MongoDB collection.
            query (dict): Query to find the documents.

        Returns:
            int: Total number of documents.
        """
        try:
            if not query:
                query = dict()

            count = self._db[collection_name].count_documents(query)
            return count
        except Exception as e:
            self._logger.error(f"Error getting document count: {e}")
            raise

    def check_data_exist(self, collection_name: str, query: dict) -> bool:
        """
        Check any document exist  in a collection by given query condition.

        Args:
           collection_name (str): Name of the MongoDB collection.
           query (dict): Query to find the documents - It is mean condition(where).

        Returns:
           bool: If it is True, then data is existed.
        """
        self._logger.info("Checking data is exist for %s given condition %s", collection_name, query)
        return self.counts(collection_name=collection_name, query=query) > 0

    def delete_one(self, collection_name: str, query: dict):
        """
        Delete a single document from the specified collection based on the query.

        Args:
            collection_name (str): Name of the MongoDB collection.
            query (dict): Query to find the document to delete.

        Returns:
            None
        """
        try:
            result = self._db[collection_name].delete_one(query)
            self._logger.info(f"{result.deleted_count} document deleted")
        except Exception as e:
            self._logger.error(f"Error deleting document: {e}")
            raise

    def delete_many(self, collection_name: str, query: dict):
        """
        Delete multiple documents from the specified collection based on the query.

        Args:
            collection_name (str): Name of the MongoDB collection.
            query (dict): Query to find the documents to delete.

        Returns:
            None
        """
        try:
            result = self._db[collection_name].delete_many(query)
            self._logger.info(f"{result.deleted_count} documents deleted")
        except Exception as e:
            self._logger.error(f"Error deleting documents: {e}")
            raise

    def update_one(self, collection_name: str, query: dict, update: dict):
        """
        Update a single document in the specified collection based on the query.

        Args:
            collection_name (str): Name of the MongoDB collection.
            query (dict): Query to find the document to update.
            update (dict): Update operation to be applied.

        Returns:
            None
        """
        try:
            result = self._db[collection_name].update_one(query, {'$set': update})
            self._logger.info(f"{result.modified_count} document updated")
        except Exception as e:
            self._logger.error(f"Error updating document: {e}")
            raise

    def update_many(self, collection_name: str, query: dict, update: dict):
        """
        Update multiple documents in the specified collection based on the query.

        Args:
            collection_name (str): Name of the MongoDB collection.
            query (dict): Query to find the documents to update.
            update (dict): Update operation to be applied.

        Returns:
            None
        """
        try:
            result = self._db[collection_name].update_many(query, {'$set': update})
            self._logger.info(f"{result.modified_count} documents updated")
        except Exception as e:
            self._logger.error(f"Error updating documents: {e}")
            raise

    def drop_table(self, collection_name: str):
        """
        Delete an entire collection from the database.

        Args:
            collection_name (str): Name of the MongoDB collection to be deleted.

        Returns:
            None
        """
        try:
            self._db[collection_name].drop()
            self._logger.info(f"Collection '{collection_name}' dropped")
        except Exception as e:
            self._logger.error(f"Error dropping collection: {e}")
            raise

    def create_table(self, collection_name: str):
        """
        Create a new collection without inserting any documents.

        Args:
            collection_name (str): Name of the MongoDB collection.

        Returns:
            None
        """
        try:
            self._db.create_collection(collection_name)
            self._logger.info(f"Collection '{collection_name}' created.")
        except Exception as e:
            self._logger.error(f"Error creating collection: {e}")
            raise

    def truncate_table(self, collection_name: str):
        """
        Truncate a collection by removing all documents.

        Args:
            collection_name (str): Name of the MongoDB collection to be truncated.

        Returns:
            None
        """
        try:
            result = self._db[collection_name].delete_many({})
            self._logger.info(f"{result.deleted_count} documents truncated in '{collection_name}'")
        except Exception as e:
            self._logger.error(f"Error truncating collection: {e}")
            raise
