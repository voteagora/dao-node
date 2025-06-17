import os, json, asyncio, websocket, websockets
from eth_abi.abi import decode as decode_abi
from collections import defaultdict
from pprint import pprint

from web3 import Web3
from sanic.log import logger as logr

from .utils import camel_to_snake
from .clients_httpjson import SubscriptionPlannerMixin
from .signatures import DELEGATE_CHANGED_2, VOTE_CAST_1, VOTE_CAST_WITH_PARAMS_1, PROPOSAL_CREATED_1, PROPOSAL_CREATED_MODULE
from .dev_modes import CAPTURE_WS_CLIENT_OUTPUTS

DAO_NODE_USE_POA_MIDDLEWARE = os.getenv('DAO_NODE_USE_POA_MIDDLEWARE', "false").lower() in ('true', '1')

class Reset(Exception):
    pass

class JsonRpcRtWsClientCaster:
    
    def __init__(self, abis):
        self.abis = abis


    def lookup(self, signature):

        abi_frag = self.abis.get_by_signature(signature)
        EVENT_NAME = abi_frag.name       
        contract_events = Web3().eth.contract(abi=[abi_frag.literal]).events
        processor = getattr(contract_events, EVENT_NAME)().process_log

        def cast_log_header(log):
            return {
                    "block_number": str(int(log["blockNumber"], 16)),
                    "log_index": int(log["logIndex"], 16),
                    "transaction_index": int(log["transactionIndex"], 16),
                   }
        
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
            # THIS FUNCTION IS SOoooo close to the one in clients_httpjson, we should be able to collapse, 
            # if we could figure out a rig to test the diffs.

            def parse_delegates(array):
                return [(x['_delegatee'].lower(), x['_numerator']) for x in array]
                
            def caster_fn(log):

                tmp = processor(log)
                args = {camel_to_snake(k) : v for k,v in tmp['args'].items()}
                
                args['old_delegatees'] = parse_delegates(args['old_delegatees'])
                args['new_delegatees'] = parse_delegates(args['new_delegatees'])
                
                header = cast_log_header(log)
                args.update(header)

                return args

        elif signature == VOTE_CAST_1:
        
            def caster_fn(log):
                tmp = processor(log)
                args = {camel_to_snake(k) : v for k,v in tmp['args'].items()}
                args['voter'] = args['voter'].lower()

                header = cast_log_header(log)
                args.update(header)

                return args
    
        elif signature == VOTE_CAST_WITH_PARAMS_1:
        
            def caster_fn(log):
                tmp = processor(log)
                args = {camel_to_snake(k) : v for k,v in tmp['args'].items()}
                args['voter'] = args['voter'].lower()
                args['params'] = args['params'].hex()

                header = cast_log_header(log)
                args.update(header)

                return args

        elif signature == PROPOSAL_CREATED_1:

            def caster_fn(log):
                try:
                    tmp = processor(log)
                    args = {camel_to_snake(k) : array_of_bytes_to_str(v) for k,v in tmp['args'].items()}
                    
                    header = cast_log_header(log)
                    args.update(header)
                    
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
                    
                    header = cast_log_header(log)
                    args.update(header)
                    
                    return args

        elif signature == PROPOSAL_CREATED_MODULE:

            def parse_settings(settings):
                if hasattr(settings, 'values'):
                    return list(settings.values())
                return settings
            
            def caster_fn(log):
                tmp = processor(log)
                args = {camel_to_snake(k) : array_of_bytes_to_str(v) for k,v in tmp['args'].items()}
                args['settings'] = parse_settings(args['settings'])
                
                header = cast_log_header(log)
                args.update(header)
                
                return args

        else:

            abi = self.abis.get_by_signature(signature)
            inputs = abi.literal['inputs']
                
            def caster_fn(log):

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
                    
                log_data = log["data"]
                log_topics = log["topics"]

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

                out = {}
                for i, arg in enumerate(indexed_inputs + non_indexed_inputs):
                    field = camel_to_snake(arg["name"])
                    out[field] = array_of_bytes_to_str((indexed_values + non_indexed_values)[i])

                header = cast_log_header(log)
                out.update(header)

                return out

        return caster_fn


class JsonRpcRtWsClient(SubscriptionPlannerMixin):
    timeliness = 'realtime'

    def __init__(self, url, name):
        self.name = name
        self.url = url
        self.ws = None
        self.next_sub_request_id = 1
        self.ws_lock = asyncio.Lock()

        self.init()

        self.casterCls = JsonRpcRtWsClientCaster

        self.event_subsription_meta = defaultdict(lambda: defaultdict(dict))
        self.block_subsription_meta = []

        self.sub_ids = {}

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
            try:
                ws = websocket.create_connection(self.url)
                ws.close()
                ans = True
            except Exception:
                ans = False

        if ans:
            logr.info(f"The server '{self.url}' is valid for {self.__class__.__name__}")
        else:
            logr.info(f"The server '{self.url}' is not valid for {self.__class__.__name__}.")
        
        return ans

    async def _subscribe_to_block_headers(self):

        for chain_id in self.block_subsription_meta:
            """Subscribe to new block headers"""

            subscribe_params = {
                "jsonrpc": "2.0",
                "id": self.next_sub_request_id,
                "method": "eth_subscribe",
                "params": ["newHeads"]
            }
            self.next_sub_request_id += 1

            logr.info("Subscribing to block headers")

            async with self.ws_lock:
                await self.ws.send(json.dumps(subscribe_params))
                response = json.loads(await self.ws.recv())

            if "result" in response:
                self.block_header_sub_id = response["result"]
                self.sub_ids[self.block_header_sub_id] = ("blocks", (chain_id,))
                logr.info(f"{self.name}: Successfully subscribed to block headers with ID: {self.block_header_sub_id}")
            else:
                error = response.get('error', {})
                logr.error(f"{self.name}: Failed to subscribe to block headers: {error.get('message', 'Unknown error')}")

    async def _subscribe_to_event_logs(self):

        for chain_id in self.event_subsription_meta.keys():

            new_sub_ids = {}
            mapping_our_id_to_subscription_method = {}

            for cs_address in self.event_subsription_meta[chain_id].keys():

                topics = self.event_subsription_meta[chain_id][cs_address].keys()

                for topic in topics:

                    """Subscribe to new event logs"""
                    subscribe_params = {
                        "jsonrpc": "2.0",
                        "id": self.next_sub_request_id,
                        "method": "eth_subscribe",
                        "params": [
                            "logs",
                            {
                                "address": cs_address,
                                "topics": [topic]
                            }
                        ]
                    }

                    logr.info(f"{self.name}: Subscription Req ID: {self.next_sub_request_id} to event logs for {cs_address} with topic {topic}")

                    await self.ws.send(json.dumps(subscribe_params))

                    mapping_our_id_to_subscription_method[self.next_sub_request_id] = (chain_id, cs_address, topic)

                    self.next_sub_request_id += 1

            while len(new_sub_ids) < len(mapping_our_id_to_subscription_method):

                response = json.loads(await self.ws.recv())

                if "result" in response:
                    sub_id = response["result"]
                    our_id = response["id"]
                    new_sub_ids[sub_id] = "event",mapping_our_id_to_subscription_method[our_id]
                    logr.info(f"{self.name}: Successfully subscribed to event logs with ID: {our_id}")
                elif "method" in response and response["method"] == "eth_subscription":
                    sub_id = response["params"]["subscription"]
                    event = response["params"]["result"]
                    logr.info(f"{self.name}: Received event for subscription {sub_id}, ignoring while in setup mode. TODO: HANDLE THIS BETTER!!!")
                else:
                    error = response.get('error', {})
                    raise Reset(f"{self.name}: E24720250528: Failed to subscribe to event logs: {error}")
            
            logr.info(f"{self.name}: Successfully subscribed to {len(new_sub_ids)} event logs.")
            self.sub_ids.update(new_sub_ids)




    async def read(self):

        while True:
            try:
                async with websockets.connect(self.url) as ws:
                    self.ws = ws

                    await self._subscribe_to_block_headers()
                    await self._subscribe_to_event_logs()

                    async for message in self.ws:
                        payload = json.loads(message)
                        
                        method = payload.get("method", "no-method")

                        out = {}

                        if method == "eth_subscription":
                            sub_id = payload["params"]["subscription"]
                            event = payload["params"]["result"]


                            log_type, meta = self.sub_ids[sub_id]

                            if log_type == "blocks":

                                chain_id = meta[0]

                                block_number = int(event['number'], 16)

                                # logr.info(f"Received event for subscription {sub_id} : BLOCK: {block_number}")

                                out['block_number'] = block_number
                                out['timestamp'] = int(event['timestamp'], 16)
                                out['signal'] = f"{chain_id}.blocks"

                                yield out

                            elif log_type == "event":

                                chain_id, cs_address, topic = meta

                                caster_fn, signature = self.event_subsription_meta[chain_id][cs_address][topic]

                                out = caster_fn(event)

                                block_number = out['block_number']

                                # logr.info(f"Received event for subscription {sub_id} : EVENT: {block_number}-{topic}")

                                out['sighash'] = topic.replace("0x", "")
                                out['signature'] = signature
                                out['signal'] = f"{chain_id}.{cs_address.lower()}.{signature}"
                                
                                if CAPTURE_WS_CLIENT_OUTPUTS:
                                    out['removed'] = event.get('removed', False)
                                    out['txhash'] = event['transactionHash']


                                yield out
                        else:
                            logr.error(f"Unknown payload, skipping:")
                            logr.error(payload)
            except Exception as e:
                logr.error(f"Failed to setup or read from websocket: {e}")
                
