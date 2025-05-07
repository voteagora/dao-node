from warnings import warn
from pathlib import Path
import csv
import os
import sys
import json
from datetime import datetime, timedelta
import websocket, websockets
import asyncio
from eth_abi.abi import decode as decode_abi

from web3 import Web3, AsyncWeb3, WebSocketProvider
from web3.middleware import ExtraDataToPOAMiddleware
from sanic.log import logger as logr, error_logger as errlogr

from .utils import camel_to_snake

csv.field_size_limit(sys.maxsize)

def resolve_block_count_span(chain_id=None):

    target = 2000

    if chain_id is None:
        default_block_span = target
    elif chain_id in (1, 11155111): # Ethereum, Sepolia
        default_block_span = target
    elif chain_id in (10, 8453): # Optimism, Base
        default_block_span = target * 6
    elif chain_id in (7560,): # Cyber
        default_block_span = 10_000 
    elif chain_id in (534352,): # Scroll
        default_block_span = target * 4
    elif chain_id in (901, 957): # Derive & it's Testnet
        default_block_span = target * 6
    elif chain_id in (59144, 59141): # Linea, Linea Sepolia
        default_block_span = int(target * 4.76)
    elif chain_id in (42161, 421614): # Arbitrum One (ie XAI), Arbitrum Sepolia
        default_block_span = target * 48
    else:
        default_block_span = target

    try:
        override = int(os.getenv('DAO_NODE_ARCHIVE_NODE_HTTP_BLOCK_COUNT_SPAN'))
        assert override > 0
    except:
        override = None
    
    return override or default_block_span


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

    def get_fallback_block(self, signature):
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
        self.fallback_block = {}

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
    
    def get_fallback_block(self, signature):

        cur_fallback = self.fallback_block.get(signature, None)

        if cur_fallback:
            return cur_fallback
        
        w3 = self.connect()
            
        if not w3.is_connected():
            raise Exception(f"Could not connect to {self.url}")

        now = datetime.utcnow()
        days_back = 1 # TODO: Change back to 4, after we get infra stable.
        target_date = now - timedelta(days=days_back)

        latest_block = w3.eth.block_number

        chain_id = w3.eth.chain_id

        print(f"Searching for a block ~{days_back} days ago from block {latest_block}")

        step = resolve_block_count_span(chain_id)

        # Step backwards to find the block
        for i in range(latest_block, 0, -1 * step):

            block = w3.eth.get_block(i)
            block_time = datetime.utcfromtimestamp(block.timestamp)

            # print(f"Block {block.number}: {block_time.isoformat()} UTC")

            if block_time < target_date:
                logr.info(f"Found block from ~{days_back} days ago: {block.number} @ {block_time.isoformat()} UTC")

                self.fallback_block[signature] = block.number 

                return block.number
        else:
            logr.info(f"No block older than {days_back} days found.")
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

        chain_id = w3.eth.chain_id

        step = resolve_block_count_span(chain_id) 

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
            
class JsonRpcRTWsClientV1:
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
                async for event in self.attempt_read(chain_id, address, signature, abis):
                    yield event
            except websockets.exceptions.ConnectionClosedError as err:
                logr.exception(f"ConnectionClosedError: Problem getting real time data for {address} {signature}: {err}")
            except Exception as err:
                logr.exception(f"Other Exception: Problem getting real time data for {address} {signature}: {err}")
                # TODO - reduce this.  Honestly, we might need a pool of websockets, and then
                # a mechanism to de-dupe or replay.
                await asyncio.sleep(120)
            finally:
                logr.info("[WS TASK] Exiting WebSocket listener task.")
                
    async def attempt_read(self, chain_id, address, signature, abis):

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


class JsonRpcRTWsClient(JsonRpcRTWsClientV1):

    @staticmethod
    def decode_payload(ws_payload, inputs, signature, topic):

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
            
        log_data = ws_payload["data"]
        log_topics = ws_payload["topics"]

        # Extract indexed vs non-indexed inputs
        indexed_inputs = [i for i in inputs if i['indexed']]
        non_indexed_inputs = [i for i in inputs if not i['indexed']]

        # Decode indexed topics (skip topic[0] which is event sig hash)
        indexed_values = [
            decode_abi([i["type"]], bytes.fromhex(t[2:]))[0]
            for i, t in zip(indexed_inputs, log_topics[1:])
        ]
        non_indexed_values = list(decode_abi(
            [i["type"] for i in non_indexed_inputs],
            bytes.fromhex(log_data[2:])
        ))

        decoded = {}
        for i, arg in enumerate(indexed_inputs + non_indexed_inputs):
            decoded[arg["name"]] = (indexed_values + non_indexed_values)[i]

        out = {
            "block_number": int(ws_payload["blockNumber"], 16),
            "log_index": int(ws_payload["logIndex"], 16),
            "transaction_index": int(ws_payload["transactionIndex"], 16),
            "signature": signature,
            "sighash": topic,
        }
        out.update(decoded)

        out = {
            camel_to_snake(k): array_of_bytes_to_str(v)
            for k, v in out.items()
        }

        return out

    async def attempt_read(self, chain_id, address, signature, abis):

        after = None # 'after' is only included in the signature to be compatible with the other method calls.

        event = abis.get_by_signature(signature)
        
        abi = event.literal

        inputs = abi['inputs']

        # Extract indexed vs non-indexed inputs
        indexed_inputs = [i for i in inputs if i['indexed']]
        non_indexed_inputs = [i for i in inputs if not i['indexed']]

        async with websockets.connect(self.url, ping_interval=5, ping_timeout=3) as ws:
   
            subscribe_params = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "eth_subscribe",
                "params": [
                    "logs",
                    {
                        "address": address,
                        "topics": ["0x" + event.topic]
                    }
                ]
            }

            await ws.send(json.dumps(subscribe_params))
            subscription_response = await ws.recv()
            subscription = json.loads(subscription_response)
            sub_id = subscription.get("result")
            logr.info(f"Setup subscription ID: {sub_id} for address: {address}")

            while True:
                message = json.loads(await ws.recv())
                if message.get("method") == "eth_subscription":
                    result = message["params"]["result"]

                    out = self.decode_payload(result, inputs, event.signature, event.topic)

                    logr.info(f"Received event {out['signature']} at block {out['block_number']}")
                    yield out
                await asyncio.sleep(0.1)
