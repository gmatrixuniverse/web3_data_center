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
    _pool = None  # Class-level connection pool
    
    def __init__(self, config_path: str = None, connection_string: str = None, db_section: str = None):
        """
        Initialize PostgreSQL client with either config or connection string.
        
        Args:
            config_path: Path to YAML config file
            connection_string: Direct connection string
            db_section: Database section in config (e.g., 'local', 'labels')
        """
        self._cursor = None  # Initialize cursor before super()
        super().__init__(config_path, connection_string, db_section)
        
        # Initialize connection pool if not already created
        if PostgreSQLClient._pool is None:
            PostgreSQLClient._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=20,  # Adjust based on your needs
                dsn=self.connection_string
            )
        
    def __del__(self):
        """Ensure connection is closed on deletion"""
        self.disconnect()
        
    @property
    def connection(self):
        """Get a connection from the pool, establishing pool if needed"""
        if not self._connection or self._connection.closed:
            self.connect()
        return self._connection
        
    def connect(self):
        """Get a connection from the pool"""
        if PostgreSQLClient._pool is None:
            PostgreSQLClient._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=20,  # Adjust based on your needs
                dsn=self.connection_string
            )
        
        logger.info(f"Getting connection from pool for database: {self.db_section}")
        self._connection = PostgreSQLClient._pool.getconn()
        self._cursor = None
        
    def disconnect(self):
        """Return connection to the pool"""
        if hasattr(self, '_connection') and self._connection:
            if self._cursor:
                self._cursor.close()
                self._cursor = None
            
            if not self._connection.closed:
                if PostgreSQLClient._pool:
                    PostgreSQLClient._pool.putconn(self._connection)
                    logger.info(f"Returned connection to pool for database: {self.db_section}")
                else:
                    self._connection.close()
                    logger.info(f"Closed connection for database: {self.db_section}")
            
            self._connection = None
            
    @property
    def cursor(self):
        """Get cursor, creating one if needed"""
        if not self._cursor or self._cursor.closed:
            self._cursor = self.connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return self._cursor
        
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
                parameters = list(parameters)
            
            # logger.info(f"Executing query: {query[:200]}...")  # Log first 200 chars of query
            
            # Execute query with processed parameters
            try:
                self.cursor.execute(query, parameters)
                
                # For SELECT queries, fetch results
                if query.strip().upper().startswith(('SELECT', 'RETURNING')):
                    results = self.cursor.fetchall()
                    # logger.info(f"Query returned {len(results)} rows")
                    return [dict(row) for row in results]
                else:
                    # For other queries (INSERT, UPDATE, DELETE, etc.), commit the transaction
                    affected = self.cursor.rowcount
                    self._connection.commit()
                    logger.info(f"Query affected {affected} rows")
                    return []
                    
            except Exception as e:
                self._connection.rollback()
                logger.error(f"Error during query execution: {str(e)}")
                logger.error(f"Final query: {self.cursor.mogrify(query, parameters).decode()}")
                raise
            
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            logger.error(f"Query: {query}")
            logger.error(f"Parameters: {parameters}")
            raise
            
    async def execute(self, query: str, parameters: Union[List[Any], Dict[str, Any], None] = None) -> List[Dict[str, Any]]:
        """Execute a query asynchronously"""
        result = self.execute_query(query, parameters)
        self.connection.commit()
        return result

    async def close(self) -> None:
        """Close the database connection asynchronously"""
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
