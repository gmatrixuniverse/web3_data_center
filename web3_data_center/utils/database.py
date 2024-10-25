# src/utils/database.py

import asyncpg
import json
from datetime import datetime
from typing import List, Dict, Any


import logging

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn)

    async def create_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute('''
            CREATE TABLE IF NOT EXISTS blocks (
                number BIGINT PRIMARY KEY,
                timestamp TIMESTAMP,
                hash TEXT UNIQUE,
                miner TEXT,
                difficulty NUMERIC,
                extra_data TEXT,
                gas_limit BIGINT,
                gas_used BIGINT,
                base_fee NUMERIC,
                blob_gas_used BIGINT,
                excess_blob_gas BIGINT,
                txn_count INTEGER
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id SERIAL PRIMARY KEY,
                block_number INTEGER REFERENCES blocks(number),
                timestamp TIMESTAMP,
                hash TEXT UNIQUE,
                from_address TEXT,
                to_address TEXT,
                value TEXT,
                gas_price TEXT,
                gas_limit INTEGER,
                gas_used INTEGER,
                gas_used_exec INTEGER,
                gas_used_init INTEGER,
                gas_used_refund INTEGER,
                nonce INTEGER,
                status INTEGER,
                type INTEGER,
                txn_index INTEGER,
                call_function TEXT,
                call_parameter TEXT,
                gas_fee_cap TEXT,
                gas_tip_cap TEXT,
                blob_fee_cap TEXT,
                blob_hashes TEXT,
                con_address TEXT,
                cum_gas_used INTEGER,
                error_info TEXT,
                int_txn_count INTEGER,
                output TEXT,
                serial_number BIGINT
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS access_list (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                storage_keys TEXT
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS balance_read (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                value TEXT
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS balance_write (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                current TEXT,
                prev TEXT
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS code_info_read (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                code_hash TEXT,
                code_size INTEGER
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS code_read (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS code_write (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                code TEXT,
                code_hash TEXT
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS created (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                deploy_code TEXT,
                int_txn_index INTEGER
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS internal_txns (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                call_function TEXT,
                call_parameter TEXT,
                con_address TEXT,
                error_info TEXT,
                evm_depth INTEGER,
                from_address TEXT,
                gas_limit INTEGER,
                gas_used INTEGER,
                int_id INTEGER,
                output TEXT,
                revert BOOLEAN,
                status BOOLEAN,
                to_address TEXT,
                txn_index INTEGER,
                type INTEGER,
                value TEXT
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS logs (
                id SERIAL PRIMARY KEY,
                block_number INTEGER REFERENCES blocks(number),
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                data TEXT,
                log_id INTEGER,
                int_txn_index INTEGER,
                revert BOOLEAN,
                topic0 TEXT,
                topics TEXT
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS nonce_read (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                value INTEGER
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS nonce_write (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                current INTEGER,
                prev INTEGER
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS storage_read (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                slot_key TEXT,
                slot_value TEXT
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS storage_write (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                slot_id INTEGER,
                slot_key TEXT,
                slot_current TEXT,
                slot_prev TEXT
            )
            ''')

            await conn.execute('''
            CREATE TABLE IF NOT EXISTS suicided (
                id SERIAL PRIMARY KEY,
                transaction_hash TEXT REFERENCES transactions(hash),
                address TEXT,
                balance TEXT,
                int_txn_index INTEGER,
                to_address TEXT
            )
            ''')

    async def insert_block_and_transactions(self, block: Dict[str, Any], transactions: List[Dict[str, Any]]):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                # Insert block
                await conn.execute('''
                INSERT INTO blocks (number, timestamp, hash, parent_hash, nonce, sha3_uncles, logs_bloom, 
                transactions_root, state_root, receipts_root, miner, difficulty, 
                total_difficulty, size, extra_data, gas_limit, gas_used, base_fee_per_gas, 
                blob_gas_used, excess_blob_gas, txn_count)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21)
                ON CONFLICT (number) DO UPDATE SET 
                timestamp = EXCLUDED.timestamp, 
                hash = EXCLUDED.hash, 
                parent_hash = EXCLUDED.parent_hash, 
                nonce = EXCLUDED.nonce, 
                sha3_uncles = EXCLUDED.sha3_uncles, 
                logs_bloom = EXCLUDED.logs_bloom, 
                transactions_root = EXCLUDED.transactions_root, 
                state_root = EXCLUDED.state_root, 
                receipts_root = EXCLUDED.receipts_root, 
                miner = EXCLUDED.miner, 
                difficulty = EXCLUDED.difficulty, 
                total_difficulty = EXCLUDED.total_difficulty, 
                size = EXCLUDED.size, 
                extra_data = EXCLUDED.extra_data, 
                gas_limit = EXCLUDED.gas_limit, 
                gas_used = EXCLUDED.gas_used, 
                base_fee_per_gas = EXCLUDED.base_fee_per_gas, 
                blob_gas_used = EXCLUDED.blob_gas_used, 
                excess_blob_gas = EXCLUDED.excess_blob_gas, 
                txn_count = EXCLUDED.txn_count
                ''', block['Number'], block['Timestamp'], block['Hash'], block['ParentHash'], block['Nonce'], block['Sha3Uncles'], block['LogsBloom'], block['TransactionsRoot'], block['StateRoot'], block['ReceiptsRoot'], block['Miner'], block['Difficulty'], block['TotalDifficulty'], block['Size'], block['ExtraData'], block['GasLimit'], block['GasUsed'], block['BaseFee'], block['BlobGasUsed'], block['ExcessBlobGas'], len(transactions))

                # Insert transactions
                for tx in transactions:
                    await conn.execute('''
                    INSERT INTO transactions (block_number, timestamp, hash, from_address, to_address, value, 
                    gas_price, gas_limit, gas_used, gas_used_exec, gas_used_init, gas_used_refund,
                    nonce, status, type, txn_index, call_function, call_parameter, gas_fee_cap, 
                    gas_tip_cap, blob_fee_cap, blob_hashes, con_address, cum_gas_used, error_info, 
                    int_txn_count, output, serial_number)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)
                    ON CONFLICT (hash) DO UPDATE SET 
                    block_number = EXCLUDED.block_number, 
                    timestamp = EXCLUDED.timestamp, 
                    from_address = EXCLUDED.from_address, 
                    to_address = EXCLUDED.to_address, 
                    value = EXCLUDED.value, 
                    gas_price = EXCLUDED.gas_price, 
                    gas_limit = EXCLUDED.gas_limit, 
                    gas_used = EXCLUDED.gas_used, 
                    gas_used_exec = EXCLUDED.gas_used_exec, 
                    gas_used_init = EXCLUDED.gas_used_init, 
                    gas_used_refund = EXCLUDED.gas_used_refund,
                    nonce = EXCLUDED.nonce, 
                    status = EXCLUDED.status, 
                    type = EXCLUDED.type, 
                    txn_index = EXCLUDED.txn_index, 
                    call_function = EXCLUDED.call_function, 
                    call_parameter = EXCLUDED.call_parameter, 
                    gas_fee_cap = EXCLUDED.gas_fee_cap, 
                    gas_tip_cap = EXCLUDED.gas_tip_cap, 
                    blob_fee_cap = EXCLUDED.blob_fee_cap, 
                    blob_hashes = EXCLUDED.blob_hashes, 
                    con_address = EXCLUDED.con_address, 
                    cum_gas_used = EXCLUDED.cum_gas_used, 
                    error_info = EXCLUDED.error_info, 
                    int_txn_count = EXCLUDED.int_txn_count, 
                    output = EXCLUDED.output, 
                    serial_number = EXCLUDED.serial_number
                    ''', block['Number'], block['Timestamp'], tx['Hash'], tx['FromAddress'], tx['ToAddress'], tx['Value'], tx['GasPrice'], tx['GasLimit'], tx['GasUsed'], tx['GasUsedExec'], tx['GasUsedInit'], tx['GasUsedRefund'], tx['Nonce'], tx['Status'], tx['Type'], tx['TxnIndex'], tx['CallFunction'], tx['CallParameter'], tx['GasFeeCap'], tx['GasTipCap'], tx['BlobFeeCap'], json.dumps(tx['BlobHashes']), tx['ConAddress'], tx['CumGasUsed'], tx['ErrorInfo'], tx['IntTxnCount'], tx['Output'], tx['SerialNumber'])

                    # Insert related data (logs, etc.)
                    # ...

    async def insert_blocks(self, blocks: List[Dict[str, Any]]):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for block in blocks:
                    timestamp = datetime.strptime(block['timestamp'], '%Y-%m-%dT%H:%M:%SZ')

                    await conn.execute('''
                    INSERT INTO blocks (
                        number, timestamp, hash, miner, difficulty, extra_data,
                        gas_limit, gas_used, base_fee, blob_gas_used, excess_blob_gas, txn_count
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                    ON CONFLICT (number) DO UPDATE SET
                        timestamp = EXCLUDED.timestamp,
                        hash = EXCLUDED.hash,
                        miner = EXCLUDED.miner,
                        difficulty = EXCLUDED.difficulty,
                        extra_data = EXCLUDED.extra_data,
                        gas_limit = EXCLUDED.gas_limit,
                        gas_used = EXCLUDED.gas_used,
                        base_fee = EXCLUDED.base_fee,
                        blob_gas_used = EXCLUDED.blob_gas_used,
                        excess_blob_gas = EXCLUDED.excess_blob_gas
                    ''', 
                    block['block_number'], 
                    timestamp, 
                    block['block_hash'],
                    block['miner'],
                    block['difficulty'],
                    block['extra_data'],
                    block['gas_limit'],
                    block['gas_used'],
                    block['base_fee'],
                    block['blob_gas_used'],
                    block['excess_blob_gas'],
                    block['transaction_count'])

    async def insert_transactions(self, transactions: List[Dict[str, Any]]):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                for tx in transactions:
                    if not isinstance(tx, dict):
                        logger.error(f"Invalid transaction data: {tx}")
                        continue
                        # Check for required fields
                    required_fields = ['timestamp', 'hash', 'block_number', 'from_address', 'to_address', 'value', 
                                        'gas_price', 'gas_limit', 'gas_used', 'nonce', 'status', 'type', 'txn_index']
                    for field in required_fields:
                        if field not in tx:
                            logger.error(f"Missing required field '{field}' in transaction: {tx['hash']}")

                    timestamp = datetime.strptime(tx['timestamp'], '%Y-%m-%dT%H:%M:%SZ')

                    # Insert main transaction data
                    await conn.execute('''
                    INSERT INTO transactions (
                        block_number, timestamp, hash, from_address, to_address, value, 
                        gas_price, gas_limit, gas_used, gas_used_exec, gas_used_init, gas_used_refund,
                        nonce, status, type, txn_index, call_function, call_parameter, gas_fee_cap, 
                        gas_tip_cap, blob_fee_cap, blob_hashes, con_address, cum_gas_used, error_info, 
                        int_txn_count, output, serial_number
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28)
                    ON CONFLICT (hash) DO UPDATE SET 
                        block_number = EXCLUDED.block_number, 
                        timestamp = EXCLUDED.timestamp, 
                        from_address = EXCLUDED.from_address, 
                        to_address = EXCLUDED.to_address, 
                        value = EXCLUDED.value, 
                        gas_price = EXCLUDED.gas_price, 
                        gas_limit = EXCLUDED.gas_limit, 
                        gas_used = EXCLUDED.gas_used, 
                        gas_used_exec = EXCLUDED.gas_used_exec, 
                        gas_used_init = EXCLUDED.gas_used_init, 
                        gas_used_refund = EXCLUDED.gas_used_refund,
                        nonce = EXCLUDED.nonce, 
                        status = EXCLUDED.status, 
                        type = EXCLUDED.type, 
                        txn_index = EXCLUDED.txn_index, 
                        call_function = EXCLUDED.call_function, 
                        call_parameter = EXCLUDED.call_parameter, 
                        gas_fee_cap = EXCLUDED.gas_fee_cap, 
                        gas_tip_cap = EXCLUDED.gas_tip_cap, 
                        blob_fee_cap = EXCLUDED.blob_fee_cap, 
                        blob_hashes = EXCLUDED.blob_hashes, 
                        con_address = EXCLUDED.con_address, 
                        cum_gas_used = EXCLUDED.cum_gas_used, 
                        error_info = EXCLUDED.error_info, 
                        int_txn_count = EXCLUDED.int_txn_count, 
                        output = EXCLUDED.output, 
                        serial_number = EXCLUDED.serial_number
                    ''', 
                    tx['block_number'], timestamp, tx['hash'], tx['from_address'], tx['to_address'], 
                    tx['value'], tx['gas_price'], tx['gas_limit'], tx['gas_used'], tx['gas_used_exec'], 
                    tx['gas_used_init'], tx['gas_used_refund'], tx['nonce'], tx['status'], tx['type'], 
                    tx['txn_index'], tx['call_function'], tx['call_parameter'], tx['gas_fee_cap'], 
                    tx['gas_tip_cap'], tx['blob_fee_cap'], json.dumps(tx.get('blob_hashes', [])), tx['con_address'], 
                    tx['cum_gas_used'], tx['error_info'], tx['int_txn_count'], tx['output'], tx['serial_number']
                    )

                    access_list = tx.get('access_list', [])
                    if access_list is not None:
                        # Insert access_list
                        for access in access_list:
                            await conn.execute('''
                            INSERT INTO access_list (transaction_hash, address, storage_keys)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                            ''', tx['hash'], access['Address'], json.dumps(access['StorageKeys']))

                    balance_read = tx.get('balance_read', [])
                    if balance_read is not None:
                        # Insert balance_read
                        for balance_read in balance_read:
                            await conn.execute('''
                            INSERT INTO balance_read (transaction_hash, address, value)
                            VALUES ($1, $2, $3)
                            ON CONFLICT DO NOTHING
                            ''', tx['hash'], balance_read['Address'], balance_read['Value'])

                    balance_write = tx.get('balance_write', []) 
                    if balance_write is not None:
                        # Insert balance_write
                        for balance_write in balance_write:
                            await conn.execute('''
                            INSERT INTO balance_write (transaction_hash, address, current, prev)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT DO NOTHING
                            ''', tx['hash'], balance_write['Address'], balance_write['Current'], balance_write['Prev'])

                    # code_info_read = tx.get('code_info_read', [])
                    # if code_info_read is not None:
                    #     # Insert code_info_read
                    #     for code_info_read in code_info_read:
                    #         code_size = code_info_read.get('CodeSize')
                    #         if code_size is not None:
                    #             await conn.execute('''
                    #             INSERT INTO code_info_read (transaction_hash, address, code_hash, code_size)
                    #             VALUES ($1, $2, $3, $4)
                    #             ON CONFLICT DO NOTHING
                    #             ''', tx['hash'], code_info_read['Address'], code_info_read['CodeHash'], code_size)
                    #         else:
                    #             await conn.execute('''
                    #             INSERT INTO code_info_read (transaction_hash, address, code_hash)
                    #             VALUES ($1, $2, $3)
                    #             ON CONFLICT DO NOTHING
                    #             ''', tx['hash'], code_info_read['Address'], code_info_read['CodeHash'])

                    code_read = tx.get('code_read', [])
                    if code_read is not None:
                        # Insert code_read
                        for code_read in code_read:
                            await conn.execute('''
                            INSERT INTO code_read (transaction_hash, address)
                            VALUES ($1, $2)
                            ON CONFLICT DO NOTHING
                            ''', tx['hash'], code_read['Address'])

                    code_write = tx.get('code_write', [])
                    if code_write is not None:
                        # Insert code_write
                        for code_write in code_write:
                            await conn.execute('''
                            INSERT INTO code_write (transaction_hash, address, code, code_hash)
                            VALUES ($1, $2, $3, $4)
                            ON CONFLICT DO NOTHING
                            ''', tx['hash'], code_write['Address'], code_write['Code'], code_write['CodeHash'])

                    # Insert created
                    # if tx.get('created'):
                    #     await conn.execute('''
                    #     INSERT INTO created (transaction_hash, address, deploy_code, int_txn_index)
                    #     VALUES ($1, $2, $3, $4)
                    #     ON CONFLICT DO NOTHING
                    #     ''', tx['hash'], tx['created']['Address'], tx['created']['DeployCode'], tx['created']['IntTxnIndex'])

                    internal_txns = tx.get('internal_txns', [])
                    if internal_txns is not None:
                        # Insert internal_txns
                        for internal_tx in internal_txns:
                            await conn.execute('''
                            INSERT INTO internal_txns (
                                transaction_hash, call_function, call_parameter, con_address, error_info,
                                evm_depth, from_address, gas_limit, gas_used, int_id, output, revert,
                                status, to_address, type, value
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
                            ON CONFLICT DO NOTHING
                            ''', tx['hash'], internal_tx['CallFunction'], internal_tx['CallParameter'], 
                            internal_tx['ConAddress'], internal_tx['ErrorInfo'], internal_tx['EvmDepth'], 
                            internal_tx['FromAddress'], internal_tx['GasLimit'], internal_tx['GasUsed'], 
                            internal_tx['Id'], internal_tx['Output'], internal_tx['Revert'], internal_tx['Status'], 
                            internal_tx['ToAddress'], internal_tx['Type'], internal_tx['Value'])

                    logs = tx.get('logs', [])
                    if logs is not None:
                        # Insert logs
                        for log in logs:
                            await conn.execute('''
                            INSERT INTO logs (
                                transaction_hash, block_number, address, data, log_id, int_txn_index, revert, topic0, topics
                            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                            ON CONFLICT DO NOTHING
                            ''', tx['hash'], tx['block_number'], log['Address'], log['Data'], log['Id'], log['IntTxnIndex'], 
                            log['Revert'], log['Topics'][0], json.dumps(log['Topics']))

                    # nonce_read = tx.get('nonce_read', [])
                    # if nonce_read is not None:
                    #     # Insert nonce_read
                    #     for nonce_read in nonce_read:
                    #         await conn.execute('''
                    #         INSERT INTO nonce_read (transaction_hash, address, value)
                    #         VALUES ($1, $2, $3)
                    #         ON CONFLICT DO NOTHING
                    #         ''', tx['hash'], nonce_read['Address'], nonce_read['Value'])

                    # nonce_write = tx.get('nonce_write', [])
                    # if nonce_write is not None:
                    #     # Insert nonce_write
                    #     for nonce_write in nonce_write:
                    #         await conn.execute('''
                    #         INSERT INTO nonce_write (transaction_hash, address, current, prev)
                    #         VALUES ($1, $2, $3, $4)
                    #         ON CONFLICT DO NOTHING
                    #         ''', tx['hash'], nonce_write['Address'], nonce_write['Current'], nonce_write['Prev'])

                    # storage_read = tx.get('storage_read', [])
                    # if storage_read is not None:
                    #     # Insert storage_read
                    #     for storage_read in storage_read:
                    #         for slot in storage_read.get('Slots', []):
                    #             await conn.execute('''
                    #             INSERT INTO storage_read (transaction_hash, address, slot_key, slot_value)
                    #             VALUES ($1, $2, $3, $4)
                    #             ON CONFLICT DO NOTHING
                    #             ''', tx['hash'], storage_read['Address'], slot['Key'], slot['Value'])

                    # storage_write = tx.get('storage_write', [])
                    # if storage_write is not None:
                    #     # Insert storage_write
                    #     for storage_write in storage_write:
                    #         for slot in storage_write.get('Slots', []):
                    #             await conn.execute('''
                    #             INSERT INTO storage_write (transaction_hash, address, slot_id, slot_key, slot_current, slot_prev)
                    #             VALUES ($1, $2, $3, $4, $5, $6)
                    #             ON CONFLICT DO NOTHING
                    #             ''', tx['hash'], storage_write['Address'], slot['Id'], slot['Key'], slot['Current'], slot['Prev'])

                    
                    # Insert suicided
                    if tx.get('suicided'):
                        await conn.execute('''
                        INSERT INTO suicided (transaction_hash, address, balance, int_txn_index, to_address)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT DO NOTHING
                        ''', tx['hash'], tx['suicided']['Address'], tx['suicided']['Balance'], 
                        tx['suicided']['IntTxnIndex'], tx['suicided']['ToAddress'])

    async def close(self):
        await self.pool.close()