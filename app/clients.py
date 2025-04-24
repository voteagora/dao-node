from warnings import warn
from pathlib import Path
import csv
import os
import sys
from datetime import datetime, timedelta
import websocket

from web3 import Web3, AsyncWeb3, WebSocketProvider
from web3.middleware import ExtraDataToPOAMiddleware
from sanic.log import logger as logr, error_logger as errlogr

from .utils import camel_to_snake

csv.field_size_limit(sys.maxsize)

DAO_NODE_ARCHIVE_NODE_HTTP_BLOCK_COUNT_SPAN = int(os.getenv('DAO_NODE_ARCHIVE_NODE_HTTP_BLOCK_COUNT_SPAN', 5))

DAO_NODE_USE_POA_MIDDLEWARE = os.getenv('DAO_NODE_USE_POA_MIDDLEWARE', "false").lower() in ('true', '1')

DEBUG = False

INT_TYPES = [f"uint{i}" for i in range(8, 257, 8)]
INT_TYPES.append("uint")

class CSVClient:
    timeliness = 'archive'

    def __init__(self, path):

        if not isinstance(path, Path):
            self.path = Path(path)
        else:
            self.path = path

    def is_valid(self):
        
        if os.path.exists(self.path):
            print(f"The path '{self.path}' exists, this client is valid.")
            return True
        else:
            print(f"The path '{self.path}' does not exist, this client is not valid.")
            return False

    def get_fallback_block(self):
        return 0

    def fname(self, chain_id, address, signature):
        return self.path / f'{chain_id}/{address}/{signature}.csv'

    def read(self, chain_id, address, signature, abis, after=0):

        abi_frag = abis.get_by_signature(signature)

        if abi_frag is None:
            raise KeyError(f"Signature `{signature}` Not Found")

        fname = self.fname(chain_id, address, signature)

        int_fields = [camel_to_snake(o['name']) for o in abi_frag.inputs if o['type'] in INT_TYPES]

        cnt = 0

        if after == 0:
            
            try:
                fs = open(fname)
                reader = csv.DictReader(fs)
            except FileNotFoundError:
                warn(f"Warning: {fname} not found, skipping.")
                reader = []
            
            for row in reader:

                row['block_number'] = int(row['block_number'])
                row['log_index'] = int(row['log_index'])
                row['transaction_index'] = int(row['transaction_index'])

                # TODO - kill off either signature or sighash, we don't need 
                #        to maintain both.

                # Approach A - Support sighash only.  
                #              Code becomes harder to read and boot time
                #              is slower.
                # Approach B - Support signature only.
                #              Need to maintain a reverse lookup for JSON-RPC
                #              Boot would be faster, code would be prettier.
                #              But we would have one structurally complext part 
                #              of the code (to build and use the reverse lookup).

                row['signature'] = signature
                row['sighash'] = abi_frag.topic

                for int_field in int_fields:
                    try:
                        row[int_field] = int(row[int_field])
                    except ValueError:
                        print(f"E182250323 - Problem with casting {int_field} to int, from file {fname}.")
                    except KeyError:
                        print(f"E184250323 - Problem with getting {int_field} from file {fname}.")

                yield row


                cnt += 1
                
                if DEBUG and (cnt == 1000000):
                    break


class JsonRpcHistHttpClient:
    timeliness = 'archive'

    def __init__(self, url):
        self.url = url
        self.fallback_block = None

    def connect(self):
        
        w3 = Web3(Web3.HTTPProvider(self.url))

        if DAO_NODE_USE_POA_MIDDLEWARE:
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        
        return w3

    def is_valid(self):

        if self.url in ('', 'ignored', None):
            ans = False
        else:
            w3 = self.connect()
            ans = w3.is_connected()
        
        if ans:
            print(f"The server '{self.url}' is valid.")
        else:
            print(f"The server '{self.url}' is not valid.")
        
        return ans
    
    def get_fallback_block(self):

        if self.fallback_block is not None:
            return self.fallback_block
        
        w3 = self.connect()
            
        if not w3.is_connected():
            raise Exception(f"Could not connect to {self.url}")

        now = datetime.utcnow()
        target_date = now - timedelta(days=4)

        latest_block = w3.eth.block_number

        print(f"Searching for a block ~4 days ago from block {latest_block}")

        # Step backwards to find the block
        for i in range(latest_block, 0, -1 * DAO_NODE_ARCHIVE_NODE_HTTP_BLOCK_COUNT_SPAN):

            block = w3.eth.get_block(i)
            block_time = datetime.utcfromtimestamp(block.timestamp)

            # print(f"Block {block.number}: {block_time.isoformat()} UTC")

            if block_time < target_date:
                logr.info(f"Found block from 4+ days ago: {block.number} @ {block_time.isoformat()} UTC")

                self.fallback_block = block.number 

                return block.number
        else:
            logr.info("No block older than 4 days found.")
            return 0

    def get_paginated_logs(self, w3, contract_address, event_signature_hash, start_block, end_block, step, abi):

        all_logs = []

        # TODO: the abifsm library should clean this up.
        if event_signature_hash[:2] != "0x":
            event_signature_hash = "0x" + event_signature_hash
        
        for from_block in range(start_block, end_block, step):

            to_block = min(from_block + step - 1, end_block)  # Ensure we don't exceed the end_block

            logr.debug(f"Looping block {from_block=}, {to_block=}")

            # Set filter parameters for each range
            event_filter = {
                "fromBlock": from_block,
                "toBlock": to_block,
                "address": contract_address,
                "topics": [event_signature_hash]
            }

            # Fetch the logs for the current block range
            logs = w3.eth.get_logs(event_filter)

            EVENT_NAME = abi['name']            
            contract_events = w3.eth.contract(abi=[abi]).events
            processor = getattr(contract_events, EVENT_NAME)().process_log

            all_logs.extend(map(processor, logs)) 

            logr.info(f"Fetched logs from block {from_block} to {to_block}. Total logs: {len(all_logs)}")

            if (len(all_logs) > 4000) and DEBUG:
                break
            
        return all_logs


    def read(self, chain_id, address, signature, abis, after):

        w3 = self.connect()

        event = abis.get_by_signature(signature)
        
        abi = event.literal

        # TODO make sure inclusivivity is handled properly.    
        start_block = after
        end_block = w3.eth.block_number
        step = DAO_NODE_ARCHIVE_NODE_HTTP_BLOCK_COUNT_SPAN 

        cs_address = Web3.to_checksum_address(address)

        logs = self.get_paginated_logs(w3, cs_address, event.topic, start_block, end_block, step, abi)

        for log in logs:

            out = {}
            
            out['block_number'] = log['blockNumber']
            out['transaction_index'] = log['transactionIndex']
            out['log_index'] = log['logIndex']

            args = log['args']
            
            out.update(**args)

            out['signature'] = signature
            out['sighash'] = event.topic

            def bytes_to_str(x):
                if isinstance(x, bytes):
                    return x.hex()
                return x

            def array_of_bytes_to_str(x):
                if isinstance(x, list):
                    return [bytes_to_str(i) for i in x]
                elif isinstance(x, bytes):
                    return bytes_to_str(x)
                return x
            
            out = {camel_to_snake(k) : array_of_bytes_to_str(v) for k,v in out.items()}
           
            yield out
            
class JsonRpcRTWsClient:
    timeliness = 'realtime'

    def __init__(self, url):
        self.url = url

    def is_valid(self):

        if self.url in ('', 'ignored', None):
            ans = False
        else:

            try:
                ws = websocket.create_connection(self.url)
                ws.close()
                ans = True
            except Exception:
                ans = False

        if ans:
            print(f"The server '{self.url}' is valid.")
        else:
            print(f"The server '{self.url}' is not valid.")
        
        return ans
    
    async def read(self, chain_id, address, signature, abis, after):

        while True:
            logr.info(f"Starting read-loop for {chain_id} {address} {signature}")
            try:
                async for event in self.attempt_read(chain_id, address, signature, abis, after):
                    yield event
            except Exception as err:
                errlogr.info("info note was here")
                errlogr.error("error note was here")
                logr.exception(f"Problem getting real time data for {address} {signature}: {err}")
                await asyncio.sleep(120)

    async def attempt_read(self, chain_id, address, signature, abis, after):

        event = abis.get_by_signature(signature)
        
        abi = event.literal

        async with AsyncWeb3(WebSocketProvider(self.url)) as w3:

            if DAO_NODE_USE_POA_MIDDLEWARE:
                w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
            
            EVENT_NAME = abi['name']            
            contract_events = w3.eth.contract(abi=[abi]).events
            processor = getattr(contract_events, EVENT_NAME)().process_log

            event_filter = {
                "address": address,
                "topics": ["0x" + event.topic]
            }

            subscription_id = await w3.eth.subscribe("logs", event_filter)
            logr.info(f"Setup subscription ID: {subscription_id} for {event_filter}")

            async for response in w3.socket.process_subscriptions():

                decoded_response = processor(response['result'])

                out = {}
                out['block_number'] = decoded_response['blockNumber']
                out['log_index'] = decoded_response['logIndex']
                out['transaction_index'] = decoded_response['transactionIndex']
                out.update(**decoded_response['args'])

                out['signature'] = signature
                out['sighash'] = event.topic

                def bytes_to_str(x):
                    if isinstance(x, bytes):
                        return x.hex()
                    return x

                def array_of_bytes_to_str(x):
                    if isinstance(x, list):
                        return [bytes_to_str(i) for i in x]
                    elif isinstance(x, bytes):
                        return bytes_to_str(x)
                    return x

                out = {camel_to_snake(k) : array_of_bytes_to_str(v) for k,v in out.items()}

                logr.info(f"Received event {out['signature']} at block {out['block_number']}")

                yield out
