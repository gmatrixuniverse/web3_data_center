from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union
import yaml
from urllib.parse import quote
import logging

logger = logging.getLogger(__name__)

class BaseDatabaseClient(ABC):
    def __init__(self, config_path: str = None, connection_string: str = None):
        """
        Initialize database client with either config path or connection string.
        
        Args:
            config_path: Path to YAML config file
            connection_string: Direct connection string
        """
        self._connection = None
        self.connection_string = connection_string
        
        if config_path and not connection_string:
            self.connection_string = self._init_from_config(config_path)
            
        if not self.connection_string:
            raise ValueError("Either config_path or connection_string must be provided")
            
    @abstractmethod
    def connect(self) -> None:
        """Establish connection to database"""
        pass
        
    @abstractmethod
    def disconnect(self) -> None:
        """Close database connection"""
        pass
        
    @abstractmethod
    def execute_query(
        self,
        query: str,
        parameters: Union[List[Any], Dict[str, Any], None] = None
    ) -> List[Dict[str, Any]]:
        """Execute a query and return results"""
        pass
        
    def _init_from_config(self, config_path: str) -> Optional[str]:
        """
        Initialize connection string from config file.
        
        Args:
            config_path: Path to YAML config file
            
        Returns:
            Connection string if successful, None otherwise
        """
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
                
            db_config = config.get('database', {}).get(self.get_config_section(), {})
            if not db_config:
                logger.warning(f"No database configuration found for {self.get_config_section()}")
                return None
                
            conn_str = self.build_connection_string(db_config)
            if not conn_str:
                logger.warning("Failed to build connection string from config")
                return None
                
            return conn_str
            
        except Exception as e:
            logger.error(f"Error initializing from config: {str(e)}")
            return None
            
    @abstractmethod
    def get_config_section(self) -> str:
        """
        Get the configuration section name for this database client.
        For example: 'postgresql', 'mongodb', 'labels', etc.
        """
        pass
        
    @abstractmethod
    def build_connection_string(self, config: Dict[str, Any]) -> Optional[str]:
        """
        Build connection string from config dictionary.
        
        Args:
            config: Configuration dictionary with database settings
            
        Returns:
            Connection string if successful, None otherwise
        """
        pass
        
    def __enter__(self):
        """Context manager entry"""
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.disconnect()
        
    def is_connected(self) -> bool:
        """Check if database is connected"""
        return self._connection is not None