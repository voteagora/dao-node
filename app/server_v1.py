from sanic import Sanic
from sanic.response import text, html
import multiprocessing
import csv, time, pdb
import datetime as dt
from collections import defaultdict

from sanic.worker.manager import WorkerManager

WorkerManager.THRESHOLD = 1200 # 2 minutes

# from configs.config import get_config
from google.cloud import storage


from web3 import Web3

# Initialize a connection to the Ethereum node (Infura in this case)
infura_url = "http://127.0.0.1:8545"
web3 = Web3(Web3.HTTPProvider(infura_url))

# Check if connection is successful
if web3.is_connected():
    print("Connected to Ethereum")
else:
    raise Exception("Could not connect to Ethereum")

async def catchup_to_latest(app):

    # Specify the contract and topic (event signature)
    contract_address = "0x4200000000000000000000000000000000000042"
    event_signature_hash = web3.keccak(text="Transfer(address,address,uint256)").hex()  # E.g., "Transfer(address,address,uint256)"

    print(event_signature_hash)

    start_block = app.ctx.head_block_number + 1
    end_block = web3.eth.block_number
    step = 2000

    def get_paginated_logs(contract_address, event_signature_hash, start_block, end_block, step):
        all_logs = []
        for from_block in range(start_block, end_block, step):
            to_block = min(from_block + step - 1, end_block)  # Ensure we don't exceed the end_block

            # Set filter parameters for each range
            event_filter = {
                "fromBlock": from_block,
                "toBlock": to_block,
                "address": contract_address,
                "topics": [event_signature_hash]
            }

            # Fetch the logs for the current block range
            logs = web3.eth.get_logs(event_filter)

            event_abi = {
                "anonymous": False,
                "inputs": [
                    {"indexed": True, "name": "from", "type": "address"},
                    {"indexed": True, "name": "to", "type": "address"},
                    {"indexed": False, "name": "value", "type": "uint256"},
                ],
                "name": "Transfer",
                "type": "event"
            }
            
            contract_events = web3.eth.contract(abi=[event_abi]).events
            
            processor = getattr(contract_events, 'Transfer')().process_log

            all_logs.extend(map(processor, logs))  # Append the logs to the results list

            print(f"Fetched logs from block {from_block} to {to_block}. Total logs: {len(all_logs)}")
            
            if len(all_logs) > 3000:
                break

        return all_logs

    # Fetch logs with pagination
    logs = get_paginated_logs(contract_address, event_signature_hash, start_block, end_block, step)

    # Parse and print logs
    for log in logs:
        args = log['args']
        print(args, log.keys())
        app.ctx.balances[args['from']] -= args['value']
        app.ctx.balances[args['to']] += args['value']
        
        app.ctx.head_block_number = max(app.ctx.head_block_number, log['blockNumber'])



# Background task to stream CSV from GCS
async def download_csv_from_gcs(app):

    # Initialize the GCS client
    client = storage.Client()

    # Specify the bucket and file path
    bucket_name = 'daonode-mr-us'
    bucket_name = 'daonode-us-public'
    file_path = 'v1/events/10/0x4200000000000000000000000000000000000042/transfer(address,uint256).csv'

    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_path)

    start = dt.datetime.now()

    balances = defaultdict(int)

    print("Starting Download")
    # Fetch and process CSV rows
    records = []

    # with open('/Users/jm/Downloads/events_10_0x4200000000000000000000000000000000000042_transfer(address,uint256).txt', 'r') as f:
    with open('/Users/jm/Downloads/transfer(address,uint256).csv', 'r') as f:
    # with blob.open("r") as f:
        csv_reader = csv.reader(f)
        
        head_block_number = 0

        next(csv_reader, None)  # skip the headers

        for row in csv_reader:
            balances[row[3]] -= int(row[5])
            balances[row[4]] += int(row[5])
            head_block_number = max(head_block_number, int(row[0]))

            # records.append(row)

    end = dt.datetime.now()

    print(f"Done Download in {(end - start).total_seconds()}")

    # Store records in app context
    app.ctx.balances = balances
    app.ctx.head_block_number = head_block_number
    print(f"CSV download completed: {len(balances)} balances loaded.")


