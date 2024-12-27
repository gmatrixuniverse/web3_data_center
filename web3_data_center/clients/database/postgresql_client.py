import psycopg2
import psycopg2.extras
import psycopg2.pool
from psycopg2.extensions import register_adapter, AsIs
from typing import Any, Dict, List, Optional, Union
from urllib.parse import quote
import logging
from .base_database_client import BaseDatabaseClient

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)  # Set default level to INFO

# Register list adapter for PostgreSQL arrays
def adapt_list(lst):
    """Convert Python list to PostgreSQL array string"""
    if not lst:
        return AsIs('ARRAY[]::text[]')
    return AsIs("ARRAY[%s]::text[]" % ','.join([psycopg2.extensions.adapt(item).getquoted().decode() for item in lst]))

register_adapter(list, adapt_list)

class PostgreSQLClient(BaseDatabaseClient):
    def __init__(self, config_path: str = None, connection_string: str = None, db_section: str = None):
        """
        Initialize PostgreSQL client with either config or connection string.
        
        Args:
            config_path: Path to YAML config file
            connection_string: Direct connection string
            db_section: Database section in config (e.g., 'local', 'labels')
        """
        self._cursor = None  # Initialize cursor before super()
        self._pool = None  # Instance-level pool
        self._connection = None
        super().__init__(config_path, connection_string, db_section)
        
        logger.info(f"Initializing PostgreSQL client with connection string: {self.connection_string}")
        
        # Initialize connection pool
        self._pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=1,
            maxconn=20,  # Adjust based on your needs
            dsn=self.connection_string
        )
        logger.info("Created new connection pool")
        
    def __del__(self):
        """Ensure connection is closed on deletion"""
        self.disconnect()
        if self._pool:
            self._pool.closeall()
            logger.info("Closed all connections in pool")
        
    def connect(self):
        """Get a connection from the pool"""
        try:
            if not self._connection:
                logger.info("Getting connection from pool...")
                self._connection = self._pool.getconn()
                logger.info("Got connection from pool")
                if not self._connection:
                    raise Exception("Failed to get connection from pool")
                logger.info(f"Connection autocommit: {self._connection.autocommit}")
        except Exception as e:
            logger.error(f"Error connecting to database: {str(e)}")
            raise
        
    def disconnect(self):
        """Return connection to the pool"""
        if self._connection and self._pool:
            self._pool.putconn(self._connection)
            self._connection = None
            self._cursor = None
            
    @property
    def connection(self):
        """Get a connection from the pool, establishing pool if needed"""
        if not self._connection or self._connection.closed:
            self.connect()
        return self._connection
        
    @property
    def cursor(self):
        """Get cursor, creating one if needed"""
        if not self._cursor or self._cursor.closed:
            self._cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        return self._cursor
        
    def execute_query(self, query: str, parameters: List[Any] = None) -> List[Dict[str, Any]]:
        """Execute a query and return results as list of dictionaries"""
        try:
            logger.info(f"Executing query with connection status: {self.is_connected()}")
            cursor = self.cursor
            cursor.execute(query, parameters)
            
            # Get column names and types
            columns = [desc[0] for desc in cursor.description]
            
            # Fetch results
            results = []
            for row in cursor.fetchall():
                # Convert row tuple to dictionary
                row_dict = {}
                for i, value in enumerate(row):
                    row_dict[columns[i]] = value
                results.append(row_dict)
                
            return results
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            raise
            
    def execute(self, query: str, parameters: Union[List[Any], Dict[str, Any], None] = None) -> List[Dict[str, Any]]:
        """Execute a query"""
        return self.execute_query(query, parameters)

    def close(self) -> None:
        """Close the database connection"""
        self.disconnect()

    def get_config_section(self) -> str:
        """Get config section name for PostgreSQL"""
        return "database"
        
    def build_connection_string(self, config: Dict[str, Any]) -> Optional[str]:
        """Build PostgreSQL connection string from config"""
        try:
            # Map config keys to expected keys
            key_mapping = {
                'username': 'username',
                'user': 'username',
                'password': 'password',
                'host': 'host',
                'port': 'port',
                'database': 'database',
                'name': 'database'
            }
            
            # Build normalized config
            normalized_config = {}
            for config_key, expected_key in key_mapping.items():
                if config_key in config:
                    normalized_config[expected_key] = config[config_key]
            
            required_fields = ['username', 'password', 'host', 'port', 'database']
            if not all(field in normalized_config for field in required_fields):
                missing = [f for f in required_fields if f not in normalized_config]
                raise ValueError(f"Missing required PostgreSQL configuration fields: {missing}")
                
            username = normalized_config['username']
            password = quote(normalized_config['password'])
            host = normalized_config['host']
            port = normalized_config['port']
            database = normalized_config['database']
            
            return f"postgresql://{username}:{password}@{host}:{port}/{database}"
            
        except Exception as e:
            logger.error(f"Error building connection string: {str(e)}")
            raise
            
    def execute_batch(
        self,
        query: str,
        parameters: List[Union[Dict[str, Any], List[Any]]]
    ) -> None:
        """Execute batch operation with multiple parameter sets"""
        try:
            # Use faster_execute_batch for better performance
            psycopg2.extras.execute_batch(
                self.cursor,
                query,
                parameters,
                page_size=1000  # Process 1000 rows at a time
            )
            self._connection.commit()
        except Exception as e:
            self._connection.rollback()
            logger.error(f"Error executing batch operation: {str(e)}")
            raise
            
    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self._connection is not None and not self._connection.closed

    def get_search_path(self) -> str:
        """Get current search_path"""
        try:
            cursor = self.cursor
            cursor.execute("SHOW search_path;")
            result = cursor.fetchone()
            return result[0] if result else None
        except Exception as e:
            logger.error(f"Error getting search_path: {str(e)}")
            return None

    def execute_dml(self, query: str, parameters: List[Any] = None) -> None:
        """Execute a DML query (INSERT/UPDATE/DELETE) that doesn't return results"""
        try:
            logger.info(f"Executing DML query with connection status: {self.is_connected()}")
            cursor = self.cursor
            cursor.execute(query, parameters)
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error executing DML query: {str(e)}")
            raise

    def execute_ddl(self, query: str, parameters: List[Any] = None) -> None:
        """Execute a DDL query that doesn't return results"""
        try:
            logger.info(f"Executing DDL query with connection status: {self.is_connected()}")
            cursor = self.cursor
            cursor.execute(query, parameters)
            self.connection.commit()
        except Exception as e:
            logger.error(f"Error executing DDL query: {str(e)}")
            raise
