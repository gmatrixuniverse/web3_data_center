from typing import Any, Dict, List, Optional, Union
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.collection import Collection
from .base_database_client import BaseDatabaseClient

class MongoDBClient(BaseDatabaseClient):
    """
    MongoDB client implementation.
    Provides methods for MongoDB-specific operations while maintaining base interface.
    """
    
    def __init__(
        self,
        connection_string: str,
        database: str,
        **kwargs
    ):
        """
        Initialize MongoDB client.
        
        Args:
            connection_string (str): MongoDB connection string
            database (str): Database name
            **kwargs: Additional configuration parameters
        """
        super().__init__(connection_string, **kwargs)
        self.database_name = database
        self._client = None
        self._db = None
        
    def connect(self) -> None:
        """Establish connection to MongoDB"""
        if not self._client:
            self._client = MongoClient(self.connection_string)
            self._db = self._client[self.database_name]
            self.logger.info(f"Connected to MongoDB database: {self.database_name}")
            
    def disconnect(self) -> None:
        """Close MongoDB connection"""
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            self.logger.info("MongoDB connection closed")
            
    def get_collection(self, collection_name: str) -> Collection:
        """
        Get MongoDB collection.
        
        Args:
            collection_name (str): Name of the collection
            
        Returns:
            MongoDB collection object
        """
        return self._db[collection_name]
        
    def execute_query(
        self,
        query: str,
        parameters: Optional[Dict[str, Any]] = None,
        fetch_all: bool = True,
        collection: str = None
    ) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Execute MongoDB query.
        
        Args:
            query (str): MongoDB operation type ('find', 'insert', 'update', 'delete')
            parameters (Optional[Dict[str, Any]]): Query parameters
            fetch_all (bool): If True, fetch all results (for find operations)
            collection (str): Collection name
            
        Returns:
            Query results for find operations
        """
        if not collection:
            raise ValueError("Collection name must be specified")
            
        col = self.get_collection(collection)
        parameters = parameters or {}
        
        if query == 'find':
            cursor = col.find(parameters)
            if fetch_all:
                return list(cursor)
            return cursor.next() if cursor.count() > 0 else None
        elif query == 'insert':
            return col.insert_one(parameters)
        elif query == 'update':
            filter_dict = parameters.get('filter', {})
            update_dict = parameters.get('update', {})
            return col.update_many(filter_dict, update_dict)
        elif query == 'delete':
            return col.delete_many(parameters)
        else:
            raise ValueError(f"Unsupported operation: {query}")
            
    def execute_batch(
        self,
        query: str,
        parameters: List[Dict[str, Any]],
        collection: str = None
    ) -> None:
        """
        Execute batch operation in MongoDB.
        
        Args:
            query (str): MongoDB operation type ('insert', 'update', 'delete')
            parameters (List[Dict[str, Any]]): List of parameter sets
            collection (str): Collection name
        """
        if not collection:
            raise ValueError("Collection name must be specified")
            
        col = self.get_collection(collection)
        
        if query == 'insert':
            col.insert_many(parameters)
        elif query == 'update':
            for param in parameters:
                filter_dict = param.get('filter', {})
                update_dict = param.get('update', {})
                col.update_many(filter_dict, update_dict)
        elif query == 'delete':
            for param in parameters:
                col.delete_many(param)
        else:
            raise ValueError(f"Unsupported batch operation: {query}")
            
    def aggregate(
        self,
        pipeline: List[Dict[str, Any]],
        collection: str
    ) -> List[Dict[str, Any]]:
        """
        Execute MongoDB aggregation pipeline.
        
        Args:
            pipeline (List[Dict[str, Any]]): Aggregation pipeline stages
            collection (str): Collection name
            
        Returns:
            Aggregation results
        """
        col = self.get_collection(collection)
        return list(col.aggregate(pipeline))
