# DataCenter

The `DataCenter` class is the main interface for the Web3 Data Center package. It provides methods to access various blockchain data and analysis features.

## Class: DataCenter

### `__init__(self, config_path: str = "config.yml", clients: Optional[Dict[str, BaseClient]] = None)`

Initialize a new DataCenter instance.

Parameters:
- `config_path` (str): Path to the configuration file. Default is "config.yml".
- `clients` (Optional[Dict[str, BaseClient]]): A dictionary of pre-initialized API clients. If not provided, default clients will be created.

### `async get_token_info(self, token_address: str, chain: str = 'solana') -> Optional[Token]`

Retrieve information about a token.

Parameters:
- `token_address` (str): The address of the token.
- `chain` (str): The blockchain network. Default is 'solana'.

Returns:
- `Optional[Token]`: A Token object containing the token information, or None if not found.

### `async get_top_holders(self, token_address: str, limit: int = 10, chain: str = 'solana') -> Optional[List[Holder]]`

Retrieve the top holders of a token.

Parameters:
- `token_address` (str): The address of the token.
- `limit` (int): The number of top holders to retrieve. Default is 10.
- `chain` (str): The blockchain network. Default is 'solana'.

Returns:
- `Optional[List[Holder]]`: A list of Holder objects representing the top holders, or None if not found.

...