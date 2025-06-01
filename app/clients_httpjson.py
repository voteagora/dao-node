
import os
from datetime import datetime, timedelta
from collections import defaultdict

from web3 import Web3
from web3.middleware import ExtraDataToPOAMiddleware
from sanic.log import logger as logr

from .utils import camel_to_snake
from .clients_csv import SubscriptionPlannerMixin
from .dev_modes import CAPTURE_CLIENT_OUTPUTS_TO_DISK
from .signatures import DELEGATE_CHANGED_2

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

class JsonRpcHistHttpClientCaster:
    
    def __init__(self, abis):
        self.abis = abis

    def lookup(self, signature):

        abi_frag = self.abis.get_by_signature(signature)
        EVENT_NAME = abi_frag.name       
        contract_events = Web3().eth.contract(abi=[abi_frag.literal]).events
        processor = getattr(contract_events, EVENT_NAME)().process_log

        """
        
        # This patch was needed for the proposal-create events when we were brute forcing types by data-product.

        elif isinstance(obj, bytes):
            obj = [obj.hex()] <- This was likely a bad idea, it turned something that was not an array into an array.
        elif isinstance(obj, (list, tuple)):
            out = []
            for o in obj:
                try:
                    o = o.hex()
                except:
                    assert isinstance(o, str)
                out.append(o)
            obj = out
        """

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

        def caster_fn(log):
            tmp = processor(log)
            args = {camel_to_snake(k) : array_of_bytes_to_str(v) for k,v in tmp['args'].items()}
            return args
        
        if signature == DELEGATE_CHANGED_2:

            def parse_delegates(array):
                return [(x['_delegatee'].lower(), x['_numerator']) for x in array]

            def caster_fn(log):

                print(log)
                
                tmp = processor(log)
                args = dict(tmp['args'])
                
                args['old_delegatees'] = parse_delegates(args['oldDelegatees'])
                args['new_delegatees'] = parse_delegates(args['newDelegatees'])

                del args['oldDelegatees']
                del args['newDelegatees']
                
                return args
        
        return caster_fn

class JsonRpcHistHttpClient(SubscriptionPlannerMixin):
    timeliness = 'archive'

    def __init__(self, url):
        self.url = url
        self.fallback_block = None
        
        self.init()

        self.casterCls = JsonRpcHistHttpClientCaster

        self.event_subsription_meta = defaultdict(lambda: defaultdict(dict))
        self.block_subsription_meta = []

    def connect(self):
        
        w3 = Web3(Web3.HTTPProvider(self.url))

        if DAO_NODE_USE_POA_MIDDLEWARE:
            w3.middleware_onion.inject(ExtraDataToPOAMiddleware, layer=0)
        
        return w3
        
    def plan_event(self, chain_id, address, signature):

        abi_frag = self.abis.get_by_signature(signature)

        caster_fn = self.caster.lookup(signature)

        cs_address = Web3.to_checksum_address(address)

        # TODO: the abifsm library should clean this up.
        topic = abi_frag.topic
        if topic[:2] != "0x":
            topic = "0x" + topic

        self.event_subsription_meta[chain_id][cs_address][topic] = (caster_fn, signature)


    def plan_block(self, chain_id):

        self.block_subsription_meta.append(chain_id)


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

        if self.fallback_block:
            return self.fallback_block
        
        w3 = self.connect()
            
        if not w3.is_connected():
            raise Exception(f"Could not connect to {self.url}")

        now = datetime.utcnow()

        if CAPTURE_CLIENT_OUTPUTS_TO_DISK:
            days_back = 15 
        else:
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

                self.fallback_block = block.number 

                return block.number
        else:
            logr.info(f"No block older than {days_back} days found.")
            return 0

    def get_logs_unsafe(self, w3, contract_address, topics, from_block, to_block):

        # Set filter parameters for each range
        event_filter = {
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": contract_address,
            "topics": [topics]
        }

        # logr.info(f"Fetching logs for {contract_address} from block {from_block} to {to_block}, for topics={topics}")
        # Fetch the logs for the current block range
        try:
            logs = w3.eth.get_logs(event_filter)
        except Exception as e:
            logr.error(f"Failed to get logs: {e}")
            print(event_filter)
            raise
        
        return logs


    def get_paginated_logs(self, w3, contract_address, topics, start_block, step):

        def chunk_list(lst, step):
            """Split a list into chunks of size `step`."""
            return [lst[i:i + step] for i in range(0, len(lst), step)]

        topics = chunk_list(list(topics), 4)

        logr.info(f"👉 Fetching {len(topics)} topic chunk(s) for {contract_address} from block {start_block}")

        from_block = start_block

        all_logs = []

        while True:

            logs = []
            
            end_block = w3.eth.block_number

            to_block = min(from_block + step - 1, end_block)  # Ensure we don't exceed the end_block

            for topic_chunk in topics:
                chunk_logs = self.get_logs_unsafe(w3, contract_address, topic_chunk, from_block, to_block)
                logs.extend(chunk_logs)
            
            if len(logs):
                logr.info(f"Fetched {len(logs)} logs from block {from_block} to {to_block}")
            
            all_logs.extend(logs)

            from_block = to_block + 1

            if from_block > end_block:
                break

        return all_logs
        


        

    def get_paginated_blocks(self, w3, chain_id, start_block, end_block, step):

        for block_num in range(start_block, end_block, step):
            full_block = w3.eth.get_block(block_num)

            block = {}
            timestamp = full_block['timestamp']
            assert isinstance(timestamp, int)
            block['timestamp'] = timestamp

            block_number = full_block['number']
            assert isinstance(block_number, int)
            block['block_number'] = block_number

            yield block

    def read_blocks(self, chain_id, after):
        
        w3 = self.connect()

        latest_block = w3.eth.block_number

        chain_id = w3.eth.chain_id

        step = resolve_block_count_span(chain_id) 
        
        blocks = self.get_paginated_blocks(w3, chain_id, start_block=after, end_block=latest_block, step=step)

        for block in blocks:
            yield block

    def read(self, after):


        start_block = after

        new_signal = True

        for chain_id in self.block_subsription_meta:
            blocks = self.read_blocks(chain_id, start_block)
            for block in blocks:
                yield block, f"{chain_id}.blocks", new_signal
            new_signal = False

        w3 = self.connect()

        all_logs = []

        new_signal = True
        for chain_id in self.event_subsription_meta.keys():
            
            step = resolve_block_count_span(chain_id) 

            for cs_address in self.event_subsription_meta[chain_id].keys():

                topics = self.event_subsription_meta[chain_id][cs_address].keys()

                logs = self.get_paginated_logs(w3, cs_address, topics, start_block, step)

                for log in logs:


                    topic = "0x" + log['topics'][0].hex()

                    caster_fn, signature = self.event_subsription_meta[chain_id][cs_address][topic]

                    args = caster_fn(log)

                    out = {}

                    out['block_number'] = str(log['blockNumber'])
                    out['transaction_index'] = log['transactionIndex']
                    out['log_index'] = log['logIndex']

                    out.update(**args)

                    out['signature'] = signature
                    signal = f"{chain_id}.{cs_address.lower()}.{signature}"
                    out['sighash'] = topic.replace("0x", "")

                    all_logs.append((out, signal, new_signal))

        all_logs.sort(key=lambda x: (x[0]['block_number'], x[0]['transaction_index'], x[0]['log_index']))   

        for log in all_logs:
            yield log
