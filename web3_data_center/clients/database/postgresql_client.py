import psycopg2
import psycopg2.extras
from psycopg2.extensions import register_adapter, AsIs
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote
import logging
from .base_database_client import BaseDatabaseClient

logger = logging.getLogger(__name__)

# Register list adapter for PostgreSQL arrays
def adapt_list(lst):
    """Convert Python list to PostgreSQL array string"""
    if not lst:
        return AsIs('ARRAY[]::text[]')
    return AsIs("ARRAY[%s]::text[]" % ','.join([psycopg2.extensions.adapt(item).getquoted().decode() for item in lst]))

register_adapter(list, adapt_list)

class PostgreSQLClient(BaseDatabaseClient):
    def __init__(self, config_path: str = None, connection_string: str = None):
        """Initialize PostgreSQL client with either config or connection string"""
        self._cursor = None  # Initialize cursor before super()
        super().__init__(config_path, connection_string)
        
    def __del__(self):
        """Ensure connection is closed on deletion"""
        self.disconnect()
        
    @property
    def connection(self):
        """Get the current connection, establishing one if needed"""
        if not self._connection or self._connection.closed:
            self.connect()
        return self._connection
        
    @connection.setter
    def connection(self, value):
        """Set the connection"""
        if hasattr(self, '_connection') and self._connection and not self._connection.closed:
            self.disconnect()
        self._connection = value
        self._cursor = None  # Reset cursor when connection changes
        
    @property
    def cursor(self):
        """Get cursor, creating one if needed"""
        if not self._cursor or self._cursor.closed:
            self._cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return self._cursor
        
    def connect(self) -> None:
        """Establish connection to PostgreSQL database"""
        try:
            if not self._connection or self._connection.closed:
                self._connection = psycopg2.connect(
                    self.connection_string,
                    cursor_factory=psycopg2.extras.RealDictCursor
                )
                self._connection.autocommit = True
                logger.debug("Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise
            
    def disconnect(self) -> None:
        """Close PostgreSQL connection"""
        try:
            if hasattr(self, '_cursor') and self._cursor and not self._cursor.closed:
                self._cursor.close()
            if hasattr(self, '_connection') and self._connection and not self._connection.closed:
                self._connection.close()
                logger.debug("Disconnected from PostgreSQL database")
        except Exception as e:
            logger.error(f"Error disconnecting from PostgreSQL: {str(e)}")
        finally:
            self._cursor = None
            self._connection = None
            
    def execute_query(
        self,
        query: str,
        parameters: Union[List[Any], Dict[str, Any], None] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries"""
        try:
            # Ensure parameters are passed correctly for psycopg2
            if parameters is None:
                parameters = []
            elif isinstance(parameters, (list, tuple)):
                # Keep parameters as list for proper handling
                parameters = list(parameters)
            
            # Detailed parameter validation and logging
            logger.debug(f"Original query: {query}")
            logger.debug(f"Parameters before processing: {parameters}")
            logger.debug(f"Parameter type before processing: {type(parameters)}")
            
            # Count placeholders in query
            placeholder_count = query.count('%s')
            if isinstance(parameters, (list, tuple)):
                param_count = len(parameters)
                logger.debug(f"Number of placeholders in query: {placeholder_count}")
                logger.debug(f"Number of parameters provided: {param_count}")
                
                if placeholder_count != param_count:
                    error_msg = f"Mismatch between number of placeholders ({placeholder_count}) and parameters ({param_count})"
                    logger.error(error_msg)
                    raise ValueError(error_msg)
            
            # Execute query with processed parameters
            try:
                self.cursor.execute(query, parameters)
                results = self.cursor.fetchall()
                
                # Log success
                logger.debug(f"Query executed successfully. Returned {len(results)} rows.")
                return [dict(row) for row in results]
            except Exception as e:
                logger.error(f"Error during query execution: {str(e)}")
                logger.error(f"Final query: {self.cursor.mogrify(query, parameters).decode()}")
                raise
            
        except Exception as e:
            if self._connection and not self._connection.closed:
                self._connection.rollback()
            
            # Detailed error logging
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(f"Parameters: {parameters}")
            logger.error(f"Parameter type: {type(parameters)}")
            
            raise
            
    def get_config_section(self) -> str:
        """Get config section name for PostgreSQL"""
        return "labels"  # Using the labels section for Web3 label database
        
    def build_connection_string(self, config: Dict[str, Any]) -> Optional[str]:
        """Build PostgreSQL connection string from config"""
        try:
            required_fields = ['username', 'password', 'host', 'port', 'database']
            if not all(field in config for field in required_fields):
                logger.warning("Missing required PostgreSQL configuration fields")
                return None
                
            username = config['username']
            password = quote(config['password'])
            host = config['host']
            port = config['port']
            database = config['database']
            
            return f"postgresql://{username}:{password}@{host}:{port}/{database}"
            
        except Exception as e:
            logger.error(f"Error building connection string: {str(e)}")
            return None
            
    def execute_batch(
        self,
        query: str,
        parameters: List[Union[Dict[str, Any], List[Any]]]
    ) -> None:
        """Execute batch operation with multiple parameter sets"""
        try:
            psycopg2.extras.execute_batch(self.cursor, query, parameters)
        except Exception as e:
            if self._connection and not self._connection.closed:
                self._connection.rollback()
            logger.error(f"Error executing batch operation: {str(e)}")
            raise
            
    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self._connection is not None and not self._connection.closed
