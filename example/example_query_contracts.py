import asyncio
import pandas as pd
import sys
import os
import csv
from tqdm import tqdm
from web3 import Web3
# Add the project root directory to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from web3_data_center.core.data_center import DataCenter
from web3_data_center.utils.logger import get_logger
logger = get_logger(__name__)

async def process_contracts(data_center, contracts, chain='ethereum'):
    results = []
    w3 = Web3()
    # Create progress bar
    pbar = tqdm(total=len(contracts), desc="Processing contracts")
    
    for contract in contracts:
        try:
            # Get contract metrics
            user_count = await data_center.get_contract_tx_user_count(address=w3.to_checksum_address(contract), chain=chain)
            # tx_count = await data_center.get_contract_tx_count(address=contract, chain=chain)
            
            results.append({
                'contract_address': contract,
                'tx_count': user_count['tx_count'],
                'user_count': user_count['user_count'],
            })

            logger.info(f"Contract: {contract}, Users: {user_count}")

            await asyncio.sleep(60)

        except Exception as e:
            logger.error(f"Error processing contract {contract}: {str(e)}")
            results.append({
                'contract_address': contract,
                'tx_count': None,
                'user_count': None,
            })

        pbar.update(1)

    pbar.close()
    return results

async def main():
    # Initialize DataCenter
    data_center = DataCenter()
    
    try:
        # Read contracts from CSV
        df = pd.read_csv('contracts.csv')
        contracts = df['contract_address'].tolist()
        # deployed_contracts = await data_center.get_deployed_contracts(address="0x37aAb97476bA8dC785476611006fD5dDA4eed66B", chain='ethereum')
        # Process contracts
        results = await process_contracts(data_center, contracts)
        
        # Save results to CSV
        output_df = pd.DataFrame(results)
        output_df.to_csv('contract_metrics.csv', index=False)
        logger.info("Results saved to contract_metrics.csv")
        
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    finally:
        await data_center.close()

if __name__ == "__main__":
    asyncio.run(main())