import asyncio
from web3_data_center.clients.opensearch_client import OpenSearchClient
from web3_data_center.core.data_center import DataCenter
from web3_data_center.utils.logger import get_logger
from web3_data_center.utils.database import Database
import traceback
import time
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

logger = get_logger(__name__)

# async def process_transactions(data_center, database, to_address: str, start_block: int, end_block: int):
#     try:
#         transactions = await data_center.get_specific_txs(to_address, start_block, end_block)
        
#         if not isinstance(transactions, list):
#             logger.error(f"get_specific_txs returned {type(transactions)} instead of a list")
#             return
        
#         logger.info(f"Retrieved {len(transactions)} transactions for address {to_address} from {start_block} to {end_block}")
        
#         if not transactions:
#             logger.warning("No transactions to insert")
#             return

#         logger.debug(f"First transaction: {transactions[0] if transactions else None}")

#         await database.insert_transactions(transactions)
#         logger.info("Transactions inserted successfully")
    
#     except Exception as e:
#         logger.error(f"An error occurred: {str(e)}")
#         logger.error(f"Error type: {type(e)}")
#         logger.error(f"Traceback: {traceback.format_exc()}")

async def process_transactions(data_center, database, to_address: str, start_block: int, end_block: int):
    try:
        total_transactions = 0
        batch_size = 1000  # You can adjust this value based on your needs
        queue = asyncio.Queue(maxsize=5)  # Limit the queue size to control memory usage
        
        progress_bar = tqdm(total=end_block - start_block + 1, desc="Processing blocks", unit="block")


        async def producer():
            async for batch in data_center.get_specific_txs_batched(to_address, start_block, end_block, batch_size):
                await queue.put(batch)
                progress_bar.update(len(set(tx['block_number'] for tx in batch)))
            await queue.put(None)  # Signal that production is complete
        
        async def consumer():
            nonlocal total_transactions
            while True:
                batch = await queue.get()
                if batch is None:  # Check for the completion signal
                    break
                
                if not isinstance(batch, list):
                    logger.error(f"get_specific_txs yielded {type(batch)} instead of a list")
                    queue.task_done()
                    continue
                
                batch_count = len(batch)
                total_transactions += batch_count
                
                logger.info(f"Processing batch of {batch_count} transactions for address {to_address}")
                
                if not batch:
                    logger.warning("Empty batch received, skipping insertion")
                    queue.task_done()
                    continue
                
                logger.debug(f"First transaction in batch: {batch[0] if batch else None}")
                
                await database.insert_transactions(batch)
                logger.info(f"Batch of {batch_count} transactions inserted successfully")
                queue.task_done()

        # Start the producer and consumer tasks
        producer_task = asyncio.create_task(producer())
        consumer_task = asyncio.create_task(consumer())

        # Wait for both tasks to complete
        await asyncio.gather(producer_task, consumer_task)
        
        progress_bar.close()
        logger.info(f"Total transactions processed: {total_transactions} for address {to_address} from {start_block} to {end_block}")
    
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        logger.error(f"Error type: {type(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
    finally:
        if 'progress_bar' in locals():
            progress_bar.close()

async def main():
    # opensearch_client = OpenSearchClient()
    data_center = DataCenter()

    try:
        db = Database("postgresql://matrix67:matrix67@localhost/postgres")
        await db.connect()
        await db.create_tables()

        # Example: Get blocks brief
        start_block = 20799995
        end_block =   20800010
        # end_block =   20800000
        # blocks = await data_center.get_blocks_brief(start_block, end_block)
        # logger.info(f"Retrieved {len(blocks)} blcks")
        # logger.info(blocks)
        # await db.insert_blocks(blocks)

        # Example: Get specific transactions
        to_address = "0x3328f7f4a1d1c57c35df56bbf0c9dcafca309c49"
        time_start = time.time()
        await process_transactions(data_center, db, to_address, start_block, end_block)
        time_end = time.time()
        logger.info(f"Time taken: {time_end - time_start} seconds")
        await db.close()

        # Example: Search logs
        # index = "eth_block"
        # event_topics = ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"]  # Transfer event topic
        # logs = await data_center.search_logs(index, start_block, end_block, event_topics)
        # logger.info(f"Retrieved {len(logs)} logs")

    except Exception as e:
        logger.error(f"An error occurred: {e}")
    finally:
        await data_center.close()

if __name__ == "__main__":
    print("Starting...")
    asyncio.run(main())