import ccxt
from web3 import Web3
from typing import Dict, Any
import os

class ExchangeClient:
    def __init__(self, config_path: str = "config.yml"):
        # Initialize CCXT exchange
        exchange_id = os.getenv('CCXT_EXCHANGE', 'binance')
        exchange_class = getattr(ccxt, exchange_id)
        self.ccxt_exchange = exchange_class({
            'apiKey': os.getenv('CCXT_API_KEY'),
            'secret': os.getenv('CCXT_SECRET'),
            'enableRateLimit': True,
        })

        # Initialize Web3 for on-chain trading
        self.w3 = Web3(Web3.HTTPProvider(os.getenv('WEB3_PROVIDER_URL')))
        self.wallet_address = os.getenv('WALLET_ADDRESS')
        self.private_key = os.getenv('PRIVATE_KEY')

    async def place_market_buy_order(self, symbol: str, amount: float, on_chain: bool = False) -> Dict[str, Any]:
        if on_chain:
            return await self._place_on_chain_order(symbol, amount, is_buy=True)
        else:
            return await self._place_ccxt_order(symbol, amount, 'market', 'buy')

    async def place_market_sell_order(self, symbol: str, amount: float, on_chain: bool = False) -> Dict[str, Any]:
        if on_chain:
            return await self._place_on_chain_order(symbol, amount, is_buy=False)
        else:
            return await self._place_ccxt_order(symbol, amount, 'market', 'sell')

    async def _place_ccxt_order(self, symbol: str, amount: float, order_type: str, side: str) -> Dict[str, Any]:
        try:
            order = await self.ccxt_exchange.create_order(symbol, order_type, side, amount)
            return order
        except Exception as e:
            print(f"Error placing CCXT order: {str(e)}")
            return {}

    async def _place_on_chain_order(self, token_address: str, amount: float, is_buy: bool) -> Dict[str, Any]:
        # This is a simplified example. You'll need to implement the actual smart contract interaction
        # based on the specific DEX you're using (e.g., Uniswap, PancakeSwap, etc.)
        try:
            # Load the DEX contract ABI and address
            dex_abi = self._load_dex_abi()
            dex_address = os.getenv('DEX_CONTRACT_ADDRESS')
            dex_contract = self.w3.eth.contract(address=dex_address, abi=dex_abi)

            # Prepare the transaction
            if is_buy:
                tx = dex_contract.functions.swapExactETHForTokens(
                    amount,
                    [self.w3.eth.contract('WETH'), token_address],
                    self.wallet_address,
                    self.w3.eth.get_block('latest').timestamp + 1000  # Deadline
                ).build_transaction({
                    'from': self.wallet_address,
                    'value': self.w3.to_wei(amount, 'ether'),
                    'gas': 250000,
                    'gasPrice': self.w3.eth.gas_price,
                    'nonce': self.w3.eth.get_transaction_count(self.wallet_address),
                })
            else:
                tx = dex_contract.functions.swapExactTokensForETH(
                    self.w3.to_wei(amount, 'ether'),
                    0,  # Minimum amount of ETH to receive
                    [token_address, self.w3.eth.contract('WETH')],
                    self.wallet_address,
                    self.w3.eth.get_block('latest').timestamp + 1000  # Deadline
                ).build_transaction({
                    'from': self.wallet_address,
                    'gas': 250000,
                    'gasPrice': self.w3.eth.gas_price,
                    'nonce': self.w3.eth.get_transaction_count(self.wallet_address),
                })

            # Sign and send the transaction
            signed_tx = self.w3.eth.account.sign_transaction(tx, self.private_key)
            tx_hash = self.w3.eth.send_raw_transaction(signed_tx.rawTransaction)
            tx_receipt = self.w3.eth.wait_for_transaction_receipt(tx_hash)

            return {
                'transactionHash': tx_receipt.transactionHash.hex(),
                'status': 'success' if tx_receipt.status == 1 else 'failed',
            }
        except Exception as e:
            print(f"Error placing on-chain order: {str(e)}")
            return {}

    def _load_dex_abi(self):
        # Load the DEX ABI from a file or environment variable
        # This is just a placeholder, you'll need to implement this based on your setup
        return []