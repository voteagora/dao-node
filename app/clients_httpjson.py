import json
import os
from datetime import datetime, timedelta
from collections import defaultdict

from web3 import Web3
from web3.exceptions import Web3RPCError
from web3.middleware import ExtraDataToPOAMiddleware
from sanic.log import logger as logr

from .utils import camel_to_snake
from .clients_csv import SubscriptionPlannerMixin
from .dev_modes import CAPTURE_CLIENT_OUTPUTS_TO_DISK
from .signatures import DELEGATE_CHANGED_2, VOTE_CAST_1, VOTE_CAST_WITH_PARAMS_1, PROPOSAL_CREATED_1, PROPOSAL_CREATED_MODULE

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

        if signature == DELEGATE_CHANGED_2:

            def parse_delegates(array):
                return [(x['_delegatee'].lower(), x['_numerator']) for x in array]

            def caster_fn(log):

                tmp = processor(log)
                args = dict(tmp['args'])
                
                args['old_delegatees'] = parse_delegates(args['old_delegatees'])
                args['new_delegatees'] = parse_delegates(args['new_delegatees'])
                
                return args
        
        elif signature == VOTE_CAST_1:
        
            def caster_fn(log):
                tmp = processor(log)
                args = {camel_to_snake(k) : v for k,v in tmp['args'].items()}
                args['voter'] = args['voter'].lower()
                return args

        elif signature == VOTE_CAST_WITH_PARAMS_1:

            def caster_fn(log):
                tmp = processor(log)
                args = {camel_to_snake(k) : v for k,v in tmp['args'].items()}
                args['voter'] = args['voter'].lower()
                args['params'] = args['params'].hex()
                return args

        elif signature == PROPOSAL_CREATED_1:

            def caster_fn(log):
                try:
                    tmp = processor(log)
                    args = {camel_to_snake(k) : array_of_bytes_to_str(v) for k,v in tmp['args'].items()}
                    
                    return args
                    
                except UnicodeDecodeError:
                    if isinstance(log['data'], str):
                        data_bytes = bytes.fromhex(log['data'].replace("0x", ""))
                    else:
                        data_bytes = bytes(log['data'])
                    assert isinstance(data_bytes, bytes)
                    
                    if b'#proposalData=' in data_bytes:
                        split_point = data_bytes.find(b'#proposalData=')
                        cleaned_data = data_bytes[:split_point] + b'\x00' * (len(data_bytes) - split_point)
                    else:
                        cleaned_data = data_bytes.replace(b'\xc0', b'\x00').replace(b'\x80', b'\x00')
                    
                    patched_log = dict(log)
                    patched_log['data'] = '0x' + cleaned_data.hex()
                    
                    tmp = processor(patched_log)
                    args = {camel_to_snake(k) : array_of_bytes_to_str(v) for k,v in tmp['args'].items()}
                    
                    if 'description' in args and isinstance(args['description'], str):
                        args['description'] = args['description'].rstrip('\x00 ')
                        if '#proposalData=' in args['description']:
                            args['description'] = args['description'][:args['description'].find('#proposalData=')]
                    
                    return args

        elif signature == PROPOSAL_CREATED_MODULE:

            def parse_settings(settings):
                if hasattr(settings, 'values'):
                    return list(settings.values())
                elif isinstance(settings, list):
                    return [next(iter(item.values())) if hasattr(item, 'values') else item for item in settings]
                return settings
            
            def caster_fn(log):
                tmp = processor(log)
                args = {camel_to_snake(k) : array_of_bytes_to_str(v) for k,v in tmp['args'].items()}
                args['settings'] = parse_settings(args['settings'])
                args['options'] = parse_settings(args['options'])
                return args
        
        else: 

            def caster_fn(log):
                tmp = processor(log)
                args = {camel_to_snake(k) : array_of_bytes_to_str(v) for k,v in tmp['args'].items()}
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

        self.noisy = True

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
            logr.info(f"The server '{self.url}' is valid for {self.__class__.__name__}.")
        else:
            logr.info(f"The server '{self.url}' is not valid for {self.__class__.__name__}.")
        
        return ans
    
    def get_fallback_block(self):

        if self.fallback_block:
            return self.fallback_block
        
        w3 = self.connect()
            
        if not w3.is_connected():
            raise Exception(f"Could not connect to {self.url}")

        now = datetime.utcnow()

        if CAPTURE_CLIENT_OUTPUTS_TO_DISK:
            days_back = 30 
        else:
            days_back = 1 # TODO: Change back to 4, after we get infra stable.

        target_date = now - timedelta(days=days_back)

        latest_block = w3.eth.block_number

        chain_id = w3.eth.chain_id

        logr.info(f"Searching for a block ~{days_back} days ago from block {latest_block}")

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

    def get_paginated_logs(self, w3, contract_address, topics, step, start_block, end_block=None):

        def chunk_list(lst, chunk_size):
            """Split a list into chunks of size `chunk_size`."""
            return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]

        topics = chunk_list(list(topics), chunk_size=4)

        if self.noisy:
            logr.info(f"👉 Fetching {len(topics)} topic chunk(s) for {contract_address} from block {start_block}")

        from_block = start_block

        all_logs = []

        while True:

            logs = []

            if end_block is None:
                end_block = w3.eth.block_number

            to_block = min(from_block + step - 1, end_block)  # Ensure we don't exceed the end_block

            for topic_chunk in topics:
                chunk_logs = self.get_logs_by_block_range(w3, contract_address, topic_chunk, from_block, to_block)
                logs.extend(chunk_logs)
            
            if len(logs) and self.noisy:
                logr.info(f"Fetched {len(logs)} logs from block {from_block} to {to_block}")
            
            all_logs.extend(logs)

            from_block = to_block + 1

            if from_block > end_block:
                break

        return all_logs

    def get_logs_by_block_range(self, w3, contract_address, event_signature_hash, from_block, to_block,
                                current_recursion_depth=0, max_recursion_depth=2000):
        """
        This is a recursive function that will split itself apart to handle block ranges that exceed the block limit of the external API.

        It is unlikely that this function will ever be called directly, and is instead called by
            the :py:meth:`~.clients.JsonRpcHistHttpClient.get_paginated_logs` function, where additional
            processing is performed.

        :param w3: The web3 object used to interact with the external API.
        :param contract_address: The address of the contract to which the event is emitted.
        :param event_signature_hash: The hash of the event signature.
        :param from_block: The starting block number for the block range.
        :param to_block: The ending block number for the block range.
        :param current_recursion_depth: The current recursion depth of the function. Used for tracking recursion depth.
        :param max_recursion_depth: The maximum recursion depth allowed for the function. If the recursion depth exceeds this value, an exception will be raised. This prevents infinite recursion.
        :returns: A list of logs from the specified block range.
           """

        # Set filter parameters for each range
        event_filter = {
            "fromBlock": from_block,
            "toBlock": to_block,
            "address": contract_address,
            "topics": [event_signature_hash]
        }

        try:
            logs = w3.eth.get_logs(event_filter)
        except Exception as e:
            # catch and attempt to recover block limitation ranges
            if isinstance(e, Web3RPCError):
                error_dict = eval(str(e.args[0]))  # Convert string representation to dict
                api_error_code = error_dict['code']
                if api_error_code == -32600 or api_error_code == -32602:
                    # add one to recursion depth
                    new_recursion_depth = current_recursion_depth + 1
                    # split block range in half
                    mid = (from_block + to_block) // 2
                    # Get results from both recursive calls
                    first_half = self.get_logs_by_block_range(
                        w3=w3,
                        from_block=from_block,
                        to_block=mid - 1,
                        contract_address=contract_address,
                        event_signature_hash=event_signature_hash,
                        current_recursion_depth=new_recursion_depth,
                        max_recursion_depth=max_recursion_depth
                    )

                    second_half = self.get_logs_by_block_range(
                        w3=w3,
                        from_block=mid,
                        to_block=to_block,
                        contract_address=contract_address,
                        event_signature_hash=event_signature_hash,
                        current_recursion_depth=new_recursion_depth,
                        max_recursion_depth=max_recursion_depth
                    )

                    # Combine results, handling potential None values
                    logs = []
                    if first_half is not None:
                        logs.extend(first_half)
                    if second_half is not None:
                        logs.extend(second_half)
                    return logs
            # Fallback to raising the exception
            raise e
        return logs

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

                logs = self.get_paginated_logs(w3, cs_address, topics, step, start_block)

                for log in logs:


                    topic = "0x" + log['topics'][0].hex()

                    caster_fn, signature = self.event_subsription_meta[chain_id][cs_address][topic]

                    args = caster_fn(log)

                    out = {}

                    out['block_number'] = str(log['blockNumber'])
                    out['transaction_index'] = log['transactionIndex']
                    out['log_index'] = log['logIndex']

                    out.update(**args)

                    signal = f"{chain_id}.{cs_address.lower()}.{signature}"
                    out['signature'] = signature
                    out['sighash'] = topic.replace("0x", "")

                    all_logs.append((out, signal, new_signal))

        all_logs.sort(key=lambda x: (x[0]['block_number'], x[0]['transaction_index'], x[0]['log_index']))   

        for log in all_logs:
            yield log

class JsonRpcRtHttpClient(JsonRpcHistHttpClient):
    timeliness = 'polling'

    def __init__(self, url, name):
        self.url = url
        self.name = name
        
        self.init()

        self.casterCls = JsonRpcHistHttpClientCaster

        self.event_subsription_meta = defaultdict(lambda: defaultdict(dict))
        self.block_subsription_meta = []

        self.noisy = False


    async def read(self):

        w3 = self.connect()

        all_logs = []

        latest_block = w3.eth.block_number

        for chain_id in self.event_subsription_meta.keys():

            span = resolve_block_count_span(chain_id) 
            lookback_block = latest_block - int(span / 200) # 10 blocks back for ETH, for example.

            for cs_address in self.event_subsription_meta[chain_id].keys():

                topics = self.event_subsription_meta[chain_id][cs_address].keys()

                logs = self.get_paginated_logs(w3, cs_address, topics, step=span, start_block=lookback_block, end_block=latest_block)

                for log in logs:

                    topic = "0x" + log['topics'][0].hex()

                    caster_fn, signature = self.event_subsription_meta[chain_id][cs_address][topic]

                    args = caster_fn(log)

                    out = {}

                    out['block_number'] = str(log['blockNumber'])
                    out['transaction_index'] = log['transactionIndex']
                    out['log_index'] = log['logIndex']

                    out.update(**args)

                    out['signal'] = f"{chain_id}.{cs_address.lower()}.{signature}"
                    out['signature'] = signature
                    out['sighash'] = topic.replace("0x", "")

                    all_logs.append(out)

        all_logs.sort(key=lambda x: (x['block_number'], x['transaction_index'], x['log_index']))   

        if self.noisy:
            logr.info(f"{self.name} read {len(all_logs)} logs")
        
        for log in all_logs:
            yield log

    