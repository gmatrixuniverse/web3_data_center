# Import the main class that users will interact with
from web3_data_center.core.data_center import DataCenter

# Import important models that users might need directly
from web3_data_center.models.token import Token
from web3_data_center.models.holder import Holder
from web3_data_center.models.transaction import Transaction
from web3_data_center.models.price_history_point import PriceHistoryPoint

# Import utility functions that might be useful
from web3_data_center.utils.config_loader import load_config

# You can also import and expose the version of your package
__version__ = "0.1.3"

# Define what gets imported when someone does `from web3_data_center import *`
__all__ = [
    'DataCenter',
    'Token',
    'Holder',
    'Transaction',
    'PriceHistoryPoint',
    'load_config'
]

# You can also include a brief description of your package
__doc__ = """
Web3 Data Center

This package provides a unified interface for accessing blockchain and Web3 data
from various sources. It integrates multiple APIs to offer comprehensive data
analysis capabilities for blockchain networks.

Main Features:
- Token information retrieval
- Holder analysis
- Transaction data
- Price history
- DeFi protocol integration
- Cross-chain data access

For more information, please refer to the documentation.
"""