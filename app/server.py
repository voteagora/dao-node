import csv, time, pdb, os
import datetime as dt
import asyncio
import psycopg2 
import psycopg2.extras
from collections import defaultdict
from pathlib import Path
from bisect import bisect_left

import yaml
from web3 import AsyncWeb3, Web3
from web3.providers.persistent import (
    AsyncIPCProvider,
    WebSocketProvider,
)
from google.cloud import storage
import websocket

from sanic_ext import openapi
from sanic.worker.manager import WorkerManager
from sanic import Sanic
from sanic.response import text, html, json

from middleware import start_timer, add_server_timing_header, measure

from utils import camel_to_snake
from data_products import Balances, ProposalTypes, Delegations, Proposals, Votes, ParticipationModel

######################################################################
#
# ABIs need to be available somewhere to be picked up by teh abifsm
# library.  A future enhancement would be to ship these with DAOnode.
#
######################################################################

from abifsm import ABI, ABISet
os.environ['ABI_URL'] = 'https://storage.googleapis.com/agora-abis/v2'

######################################################################
#
# We need a YAML config matching the Agora Governor Deployment Spec.
#
######################################################################
    
CONTRACT_DEPLOYMENT = os.getenv('CONTRACT_DEPLOYMENT', 'main')

DAO_NODE_DATA_PATH = Path(os.getenv('DAO_NODE_DATA_PATH', './data'))

DAO_NODE_ARCHIVE_NODE_HTTP = os.getenv('DAO_NODE_ARCHIVE_NODE_HTTP', None)
if DAO_NODE_ARCHIVE_NODE_HTTP:

    # This pattern enables a deployer to put either the base URL in plane text or the full URL in
    # plain text, leaving ALCHEMY_API_KEY in an optional secret.

    # ...but also use an anvil fork, without any trouble of setting keys.

    ARCHIVE_NODE_HTTP_URL = DAO_NODE_ARCHIVE_NODE_HTTP

    if 'alchemy.com' in DAO_NODE_ARCHIVE_NODE_HTTP:
        ARCHIVE_NODE_HTTP_URL = ARCHIVE_NODE_HTTP_URL + os.getenv('ALCHEMY_API_KEY', '') 
        print("Using alchemy for Archive.")

    if 'quiknode.pro' in DAO_NODE_ARCHIVE_NODE_HTTP:
        ARCHIVE_NODE_HTTP_URL = ARCHIVE_NODE_HTTP_URL + os.getenv('QUICKNODE_API_KEY', '')
        print("Using quiknode.pro for Archive.")
    

DAO_NODE_ARCHIVE_NODE_HTTP_BLOCK_COUNT_SPAN = int(os.getenv('DAO_NODE_ARCHIVE_NODE_HTTP_BLOCK_COUNT_SPAN', 5))

DAO_NODE_REALTIME_NODE_WS = os.getenv('DAO_NODE_REALTIME_NODE_WS', None)
if DAO_NODE_REALTIME_NODE_WS:

    # This pattern enables a deployer to put either the base URL in plane text or the full URL in
    # plain text, leaving ALCHEMY_API_KEY in an optional secret.

    # ...but also use an anvil fork, without any trouble of setting keys.

    REALTIME_NODE_WS_URL = DAO_NODE_REALTIME_NODE_WS

    if 'alchemy.com' in DAO_NODE_REALTIME_NODE_WS:
        REALTIME_NODE_WS_URL = REALTIME_NODE_WS_URL + os.getenv('ALCHEMY_API_KEY', '')
        print("Using alchemy for Web Socket.")
    
    if 'quiknode.pro' in DAO_NODE_REALTIME_NODE_WS:
        REALTIME_NODE_WS_URL = REALTIME_NODE_WS_URL + os.getenv('QUICKNODE_API_KEY', '')
        print("Using quiknode.pro for Web Socket.")

AGORA_CONFIG_FILE = Path(os.getenv('AGORA_CONFIG_FILE', '/app/config.yaml'))
with open(AGORA_CONFIG_FILE, 'r') as f:
    config = yaml.safe_load(f)
public_config = {k : config[k] for k in ['governor_spec', 'token_spec']}

deployment = config['deployments'][CONTRACT_DEPLOYMENT]
del config['deployments']
public_deployment = {k : deployment[k] for k in ['gov', 'ptc', 'token','chain_id'] if k in deployment}

########################################################################

WorkerManager.THRESHOLD = 600 * 20 # 20 minutes

DEBUG = False

class GCSClient:
    timeliness = 'archive'

    def __init__(self, bucket):
        self.bucket = bucket
    
    def read(self, chain_id, address, signature, abi_frag, after):
        pass

INT_TYPES = [f"uint{i}" for i in range(8, 257, 8)]
INT_TYPES.append("uint")

class CSVClient:
    timeliness = 'archive'

    def __init__(self, path):
        self.path = path

    def is_valid(self):
        
        if os.path.exists(self.path):
            print(f"The path '{self.path}' exists, this client is valid.")
            return True
        else:
            print(f"The path '{self.path}' does not exist, this client is not valid.")
            return False

    def read(self, chain_id, address, signature, abis, after=0):

        abi_frag = abis.get_by_signature(signature)

        if abi_frag is None:
            raise KeyError(f"Signature `{signature}` Not Found")

        fname = self.path / f'{chain_id}/{address}/{signature}.csv'

        int_fields = [camel_to_snake(o['name']) for o in abi_frag.inputs if o['type'] in INT_TYPES]

        cnt = 0

        if after == 0:
            
            reader = csv.DictReader(open(fname))

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
                        print(f"Problem with casting {int_field} to int, from file {fname}.")
                    except KeyError:
                        print(f"Problem with getting {int_field} from file {fname}.")

                yield row


                cnt += 1
                
                if DEBUG and (cnt == 1000000):
                    break

def test_csv_client():
    csvc = CSVClient('TODO')

    abi = ABI.from_internet('token', '0x4200000000000000000000000000000000000042')
    abis = ABISet('mydao', [abi])
    frag = abis.get_by_signature('Transfer(address,address,uint256)')

    for event in csvc.read(10, '0x4200000000000000000000000000000000000042', 'Transfer(address,address,uint256)', frag.literal):
        print(event['block_number'], event['value'])


class PostGresClient:
    timeliness = 'archive'

    def __init__(self, config):
        self.config = config
    
    def read(self, chain_id, address, signature, abis, after=0):

        event = abis.get_by_signature(signature)

        pg_table = abis.pgtable(event)

        # A tactical hack to get the right table name for optimism.
        # Needs to be removed once data is ported.
        if pg_table == 'optimism_token_transfer':
            pg_table = 'optimism_transfer_events'
            extra_crit = f"address = '{address}'" # PURE UPSTREAM TECH-DEBT
        else:
            extra_crit = '' # PURE UPSTREAM TECH-DEBT

        fields = [o['name'] for o in event.inputs]

        fields = '","'.join(fields)
        if len(fields):
            fields = f', "{fields}"'

        int_fields = [o['name'] for o in event.inputs if 'int' in o['type']]

        with psycopg2.connect(self.config) as conn:

            # TODO: The only valid tables, must be sorted by block, txn, log.
            # TODO: The only valid tables, must be filtered to the contract.
            QRY = f"SELECT block_number, transaction_index, log_index{fields} FROM center.{pg_table}"

            if after:
                QRY = QRY + f" WHERE block_number > {after}"

            # HANDLING TECH-DEBT MANAGEMENT, THIS SHOULD NOT EXIST
            if extra_crit:
                if 'WHERE' in QRY:
                    QRY = QRY + " AND " + extra_crit
                else:
                    QRY = QRY + f" WHERE {extra_crit}"
            
            if DEBUG:
                QRY = QRY + " LIMIT 100"
            
            QRY = QRY + ";"

            print(QRY)
         
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            cur.execute(QRY)

            cnt = 0

            for row in cur.fetchall():

                for int_field in int_fields:
                    try:
                        row[int_field] = int(row[int_field])
                    except:
                        print(f"Couldn't cast field '{int_field}' to type int, got {row[int_field]}.")

                yield row

                cnt += 1
                
                if (cnt == 100) and DEBUG:
                    break

class JsonRpcHistHttpClient:
    timeliness = 'archive'

    def __init__(self, url):
        self.url = url

    def is_valid(self):

        if self.url in ('', 'ignored', None):
            ans = False
        else:
            w3 = Web3(Web3.HTTPProvider(self.url))
            ans = w3.is_connected()
        
        if ans:
            print(f"The server '{self.url}' is valid.")
        else:
            print(f"The server '{self.url}' is not valid.")
        
        return ans
        
    def get_paginated_logs(self, w3, contract_address, event_signature_hash, start_block, end_block, step, abi):

        all_logs = []

        # TODO: the abifsm library should clean this up.
        if event_signature_hash[:2] != "0x":
            event_signature_hash = "0x" + event_signature_hash
        
        for from_block in range(start_block, end_block, step):

            to_block = min(from_block + step - 1, end_block)  # Ensure we don't exceed the end_block

            if DEBUG:
                print(f"Looping block {from_block=}, {to_block=}")

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

            if DEBUG:
                print(f"Fetched logs from block {from_block} to {to_block}. Total logs: {len(all_logs)}")

            if (len(all_logs) > 4000) and DEBUG:
                break
            
        return all_logs


    def read(self, chain_id, address, signature, abis, after):

        w3 = Web3(Web3.HTTPProvider(self.url))

        if not w3.is_connected():
            raise Exception(f"Could not connect to {self.url}")

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

        event = abis.get_by_signature(signature)
        
        abi = event.literal

        async with AsyncWeb3(WebSocketProvider(self.url)) as w3:
            
            EVENT_NAME = abi['name']            
            contract_events = w3.eth.contract(abi=[abi]).events
            processor = getattr(contract_events, EVENT_NAME)().process_log

            event_filter = {
                "address": address,
                "topics": ["0x" + event.topic]
            }

            subscription_id = await w3.eth.subscribe("logs", event_filter)
            print(f"Setup subscription ID: {subscription_id} for {event_filter}")
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

                print(f"Received event {out['signature']} at block {out['block_number']}")

                yield out

class ClientSequencer:
    def __init__(self, clients):
        self.clients = clients
        self.num = len(clients)
        self.pos = 0
        self.lock = asyncio.Lock()
    
    def __iter__(self):
        return self

    def __next__(self): 

        self.pos += 1
        if self.pos <= self.num:
            return self.clients[self.pos - 1]

        self.pos = 0

        raise StopIteration

    def __aiter__(self):
        self.pos = 0  # Reset for new async iteration
        return self  # The object itself implements __anext__

    async def __anext__(self):
        async with self.lock:
            if self.pos < self.num:
                client = self.clients[self.pos]
                self.pos += 1
                return client
            
            self.pos = 0  # Reset for reuse
            raise StopAsyncIteration

    def get_async_iterator(self):
        return self 
    
class EventFeed:
    def __init__(self, chain_id, address, signature, abis, client_sequencer):
        self.chain_id = chain_id
        self.address = address
        self.signature = signature
        self.abis = abis
        self.cs = client_sequencer
        self.block = 0
    
    def archive_read(self):

        for client in self.cs:

            if client.timeliness == 'archive':

                reader = client.read(self.chain_id, self.address, self.signature, self.abis, after=self.block)

                for event in reader:

                    self.block = max(self.block, event['block_number'])

                    yield event

    async def realtime_async_read(self):

        async for client in self.cs.get_async_iterator():

            if client.timeliness == 'realtime':

                reader = client.read(self.chain_id, self.address, self.signature, self.abis, after=self.block)

                async for event in reader:

                    self.block = max(self.block, event['block_number'])

                    yield event

    async def boot(self, app):
        
        cnt = 0

        start = dt.datetime.now()

        print(f"Loading {self.chain_id}.{self.address}.{self.signature}", flush=True)

        data_product_dispatchers = app.ctx.dps[f"{self.chain_id}.{self.address}.{self.signature}"]

        for event in self.archive_read():
            cnt += 1
            for data_product_dispatcher in data_product_dispatchers:
                data_product_dispatcher.handle(event)

            if (cnt % 1_000_000) == 0:
                print(f"loaded {cnt} so far {( dt.datetime.now() - start).total_seconds()}", flush=True)
        
        end = dt.datetime.now()

        print(f"Done booting {cnt} records in {(end - start).total_seconds()} seconds.", flush=True)
        
        await asyncio.sleep(.01)

        return 
    
    async def run(self, app):

        data_product_dispatchers = app.ctx.dps[f"{self.chain_id}.{self.address}.{self.signature}"]

        async for event in self.realtime_async_read():
            for data_product_dispatcher in data_product_dispatchers:
                event['signature'] = self.signature
                data_product_dispatcher.handle(event)

            # See note below about Sanic Signals

            # sig = f"{self.chain_id}.{self.address}.{self.signature}"
            # await app.dispatch("data.model." + sig, context=event)


class DataProductContext:
    def __init__(self):

        self.dps = defaultdict(list)
        self.event_feeds = []
    
    def handle_dispatch(self, chain_id_contract_signature, context):

        print(f"Handle Dispatch Called : {chain_id_contract_signature}")

        data_product_dispatchers = self.dps[chain_id_contract_signature]

        if len(data_product_dispatchers):
            raise Exception(f"No data products registered for {chain_id_contract_signature}")

        for data_product in self.dps[chain_id_contract_signature]:
            data_product.handle(context)

    def register(self, signature, data_product):
        self.dps[signature].append(data_product)

        setattr(self, data_product.name, data_product)


    def add_event_feed(self, event_feed):
        self.event_feeds.append(event_feed)
    
app = Sanic('DaoNode', ctx=DataProductContext())
app.middleware('request')(start_timer)
app.middleware('response')(add_server_timing_header)

# TODO: Figure out Sanic Signals
# 
# For some reason, I couldn't get this pattern to work such that the events
# are processed using the "signal"-pattern enabled by sanic.
# I'm not sure why.  It would be more elegant if we could use it, but, alas it
# it's not working, but enabled without the framework.
# 
# @app.signal("data.model.<chain_id_contract_signature>")
# async def log_event_signal_handler(chain_id_contract_signature, **context):
#     print(f"Handling: {chain_id_contract_signature}")
#     app.ctx.handle_dispatch(chain_id_contract_signature, context)

######################################################################
#
# Application Endpoints
#
######################################################################

@app.route('v1/balance/<addr>')
@openapi.tag("Token State")
@openapi.summary("Token balance for a given address")
@openapi.description("""
## Description
The balance of the voting token used for governance for a specific EOA as of the last block heard.

## Methodology
Balances are updated on every transfer event.

## Performance
- 🟢 
- O(1)
- E(t) <= 100 μs

## Planned Enhancements

None

""")
@measure
async def balances(request, addr):
	return json({'balance' : str(app.ctx.balances.balance_of(addr)),
                 'address' : addr})

@app.route('v1/proposals')
@openapi.tag("Proposal State")
@openapi.summary("All proposals with the latest state of their outcome.")
@openapi.parameter(
    "set", 
    str, 
    location="query", 
    required=False, 
    default="relevant",
    description="Flag to filter the list of proposals, down to only the ones which are relevant."
)
@measure
async def proposals(request):

    proposal_set = request.args.get("set", "relevant").lower()

    if proposal_set == 'relevant':
        res = app.ctx.proposals.relevant()
    else:
        res = app.ctx.proposals.unfiltered()

    proposal_id_field_name = app.ctx.proposals.proposal_id_field_name

    proposals = []
    for prop in res:
        proposal = prop.to_dict()
        outcome = app.ctx.votes.proposal_aggregation[proposal[proposal_id_field_name]]
        keys = outcome.keys()
        for key in keys:
            outcome[key] = str(outcome[key])
        proposal['proposal_results'] = outcome

        print(proposal['id'])
        print(proposal)
        print(json(proposal))
        
        proposals.append(proposal)

    return json({'proposals' : proposals})

@app.route('v1/proposal/<proposal_id>')
@openapi.tag("Proposal State")
@openapi.summary("A single proposal's details, including voting record and aggregate outcome.")
@openapi.description("""
## Description
A specific proposal's details, how every voter has voted to date, and the aggregate of votes across the options impacting the outcome.

## Methodology
There are three O(1) lookups, and the all are combined into a single JSON reponse.

## Performance
- 🟢 
- O(1) x 3
- E(t) <= 200 μs

## Enhancements

None

""")
@measure
async def proposal(request, proposal_id:str):
    
    proposal = app.ctx.proposals.proposals[proposal_id].to_dict()

    outcome = app.ctx.votes.proposal_aggregation[proposal_id]
    keys = outcome.keys()
    for key in keys:
        outcome[key] = str(outcome[key])        
    proposal['outcome'] = outcome

    voting_record = app.ctx.votes.proposal_vote_record[proposal_id]
    proposal['voting_record'] = voting_record

    return json({'proposal' : proposal})

@app.route('v1/proposal_types')
@openapi.tag("Proposal State")
@openapi.summary("Latest information all proposal types")
@measure
async def proposal_types(request):
	return json({'proposal_types' : app.ctx.proposal_types.proposal_types})

@app.route('v1/proposal_type/<proposal_type_id>')
@openapi.tag("Proposal State")
@openapi.summary("Latest information about a specific proposal type")
@measure
async def proposal_type(request, proposal_type_id: int):
	return json({'proposal_type' : app.ctx.proposal_types.proposal_types[proposal_type_id],
                 'proposal_type_id' : proposal_type_id})

DEFAULT_PAGE_SIZE = 200
DEFAULT_OFFSET = 0

@app.route('v1/delegates')
@openapi.tag("Delegation State")
@openapi.summary("A sorted list of delegates.")
@openapi.description("""
## Description
Get full list of delegates sorted by number of delegators.

## Methodology
We're storing the full dataset, looking up each delegate's count and vp, to compile objects at time of request, then sorting them at time of request.

## Performance

The performance of this endpoint is a function of the sort, enriching options, and some base costs.  

The sort is done on the full list of all known delegates, at endpoint invocation time.

Enriching happens after the sort and crop to the page-size, so only the response is enriched.

### 🟡 Base Costs

Regardless of options, there are two base steps in all responses:

- 🔴 O(n) for purging delegates with 0 Voting Power (VP) or Delegator Count (DC).  🚧 This should move to the indexing in the long-run.
- 🟢 O(page_size) loop added to serialize the response.

### 🟡 Sorting only (by VP or Delegator-Count)
O(n * log(n)) is the average for python's built in `sort` method.

This could be improved by moving the sort upstream to the indexing stage, perhaps on completion of the boot. Framework enhancements are needed to achieve this.

### Enriching 
#### 🟢 With Voting Power and/or Delegator-Count 
O(page_size)

In any case (either `and` or `or`), the cost is a constant O(page_size), because the response is always enriched by the sort key.

#### 🟡 With Participation Rate
Opr = O(page_size * min(# of proposals, 10) * min(# of votes, 10))

the upper bound is O(page_size * 10 * 10)

#### Total for a fully enriched response

Total = O(n) + O(page_size) + O(n * log(n)) + O(page_size) + Opr = O(n) + O(n * log(n)) + O(102 * page_size)

## Enhancements

- Maintain the sorted view of the list in indexing
- Calculate Participation rate on proposal complete

## Test Coverage

❌ - Bad, none exists.

""")
@openapi.parameter(
    "page_size", 
    int, 
    location="query", 
    required=False, 
    default=DEFAULT_PAGE_SIZE,
    description="Number of records to return in one response."
)
@openapi.parameter(
    "offset", 
    int, 
    location="query", 
    required=False, 
    default=DEFAULT_OFFSET,
    description="Number of records to skip (ie zero-indexed) from the start."
)
@openapi.parameter(
    "sort_by", 
    str, 
    location="query", 
    required=False, 
    default='VP',
    description="Sort by either voting-power ('VP') or delegator-count ('DC')."
)
@openapi.parameter(
    "reverse", 
    bool, 
    location="query", 
    required=False, 
    default=True,
    description="To sort descending (largest value first), set to True."
)
@openapi.parameter(
    "include", 
    str, 
    location="query", 
    required=False, 
    default='DC,PR',
    description="Comma seperated list of other dimensions to include, beyond the sort-by criteria. Use 'VP', 'DC' and 'PR' for voting power, delegator count and participation rate respectively."
)
@measure
async def delegates(request):

    sort_by = request.args.get("sort_by", 'VP')
    sort_by_vp = sort_by == 'VP'
    offset = int(request.args.get("offset", DEFAULT_OFFSET))
    page_size = int(request.args.get("page_size", DEFAULT_PAGE_SIZE))
    reverse = request.args.get("reverse", "true").lower() == "true"
    include = request.args.get("include", 'DC,PR').split(",")

    if sort_by_vp:
        out = list(app.ctx.delegations.delegatee_vp.items())
    else:
        out = list(app.ctx.delegations.delegatee_cnt.items())

    # TODO This should not be necessary.  The data model should prune zeros.
    print(f"Number of records: {len(out)}")
    out = [obj for obj in out if obj[1] > 0]
    print(f"Number of records (excluding zeros): {len(out)}")

    out.sort(key=lambda x: x[1], reverse = reverse)    

    if offset:
        out = out[offset:]

    if page_size:
        if len(out) > page_size:
            out = out[:page_size]

    add_delegator_count = 'DC' in include
    add_participation_rate = 'PR' in include
    add_voting_power = 'VP' in include

    if add_participation_rate:
        pm = ParticipationModel(app.ctx.proposals, app.ctx.votes)

    if sort_by_vp:
        if add_delegator_count and add_participation_rate:
            out = [{'addr' : obj[0], 'voting_power' : str(obj[1]), 'from_cnt' : app.ctx.delegations.delegatee_cnt[obj[0]], 'participation' : pm.calculate(obj[0])} for obj in out]
        elif add_delegator_count:
            out = [{'addr' : obj[0], 'voting_power' : str(obj[1]), 'from_cnt' : app.ctx.delegations.delegatee_cnt[obj[0]]} for obj in out]
        elif add_participation_rate:
            out = [{'addr' : obj[0], 'voting_power' : str(obj[1]), 'participation' : pm.calculate(obj[0])} for obj in out]
        else:
            out = [{'addr' : obj[0], 'voting_power' : str(obj[1])} for obj in out]
    else: # sort_by_from_cnt
        if add_voting_power and add_participation_rate:
            out = [{'addr' : obj[0], 'from_cnt' : obj[1], 'voting_power' : str(app.ctx.delegations.delegatee_vp[obj[0]]), 'participation' : pm.calculate(obj[0])} for obj in out]
        elif add_voting_power:
            out = [{'addr' : obj[0], 'from_cnt' : obj[1], 'voting_power' : str(app.ctx.delegations.delegatee_vp[obj[0]])} for obj in out]
        elif add_participation_rate:
            out = [{'addr' : obj[0], 'from_cnt' : obj[1], 'participation' : pm.calculate(obj[0])} for obj in out]
        else:
            out = [{'addr' : obj[0], 'from_cnt' : obj[1]} for obj in out]

    return json({'delegates' : out})


@app.route('v1/delegate/<addr>')
@openapi.tag("Delegation State")
@openapi.summary("Information about a specific delegate")
@measure
async def delegate(request, addr):

    from_list = [(a, str(app.ctx.balances.balance_of(a))) for a in app.ctx.delegations.delegatee_list[addr]]

    return json({'delegate' : 
                {'addr' : addr,
                'from_cnt' : app.ctx.delegations.delegatee_cnt[addr],
                'from_list' : from_list,
                'voting_power' : str(app.ctx.delegations.delegatee_vp[addr])}})

@app.route('v1/delegate_vp/<addr>/<block_number>')
@openapi.tag("Delegation State")
@openapi.summary("Voting power at a block for one delegate.")
@openapi.description("""
## Description
Get a specific delegate's voting power as of a specific block height.  For tip, use the `delegate` endpoint.

## Methodology
We're storing the full history of change in vp by delegate.  We look up the delegate's history, and then do a bisect search.

## Performance
- 🟢 
- Lookup delegate + Bisect Search = O(1) + O(log n)
- E(t) <= 100 μs; the search is expected to have constant time, something on the order of 350 ns for 100K perfectly distributed records.

## Enhancements

Add transaction index and log-index awareness.

""")
@measure
async def delegate_vp(request, addr : str, block_number : int):

    vp_history = [(0, 0)] + app.ctx.delegations.delegatee_vp_history[addr]
    index = bisect_left(vp_history, (block_number,)) - 1

    index = max(index, 0)

    try:
        vp = vp_history[index][1]
    except:
        vp = 0

    return json({'voting_power' : vp,
                 'delegate' : addr,
                 'block_number' : block_number,
                 'history' : vp_history[1:]})

@app.route('v1/voting_power')
@openapi.tag("Delegation State")
@openapi.summary("Voting power for the entire DAO.")
@openapi.description("""
## Description
The total voting power across all delegations for the DAO, as of the last block heard.

## Methodology
Voting power is calculated as the cumulative sum of the difference between new and prior in every DelegateVotesChanged `event`.

## Performance
- 🟢 
- O(0)
- E(t) <= 100 μs

## Enhancements

None

""")
@measure
async def voting_power(request):
	return json({'voting_power' : str(app.ctx.delegations.voting_power)})



#################################################################################
#
# Event registration
#
################################################################################


@app.before_server_start(priority=0)
async def bootstrap_event_feeds(app, loop):

    clients = []

    # gcsc = GCSClient('gs://eth-event-feed')
    
    csvc = CSVClient(DAO_NODE_DATA_PATH)
    if csvc.is_valid():
        clients.append(csvc)
    
    # sqlc = PostGresClient('postgres://postgres:...:5432/prod')

    rpcc = JsonRpcHistHttpClient(ARCHIVE_NODE_HTTP_URL)
    if rpcc.is_valid():
       clients.append(rpcc)

    jwsc = JsonRpcRTWsClient(REALTIME_NODE_WS_URL)
    if jwsc.is_valid():
       clients.append(jwsc)


    ##########################
    # Create a sequence of clients to pull events from.  Each with their own standards for comms, drivers, API, etc. 
    dcqs = ClientSequencer(clients) 

    ##########################
    # Get a full picture of all available contracts relevant for this app.

    chain_id = int(deployment['chain_id'])

    abi_list = []

    token_addr = deployment['token']['address'].lower()
    print(f"Using {token_addr=}", flush=True)
    token_abi = ABI.from_internet('token', token_addr, chain_id=chain_id, implementation=True)
    abi_list.append(token_abi)

    gov_addr = deployment['gov']['address'].lower()
    print(f"Using {gov_addr=}", flush=True)
    gov_abi = ABI.from_internet('gov', gov_addr, chain_id=chain_id, implementation=True)
    abi_list.append(gov_abi)

    if 'ptc' in deployment:
        ptc_addr = deployment['ptc']['address'].lower()
        print(f"Using {ptc_addr=}", flush=True)
        ptc_abi = ABI.from_internet('ptc', ptc_addr, chain_id=chain_id, implementation=True)

        proposal_type_set_signature = None

        for fragment in ptc_abi.fragments:
            if fragment.type == 'event':
                if fragment.signature.startswith("ProposalTypeSet"):
                    proposal_type_set_signature = fragment.signature

        assert proposal_type_set_signature in ('ProposalTypeSet(uint8,uint16,uint16,string)', 'ProposalTypeSet(uint256,uint16,uint16,string)'), f"found {proposal_type_set_signature}"
        abi_list.append(ptc_abi)
    
    abis = ABISet('daonode', abi_list)


    ##########################
    # Instantiate a "Data Product", that would need to be maintained given one or more events.

    balances = Balances(token_spec=public_config['token_spec'])
    app.ctx.register(f'{chain_id}.{token_addr}.Transfer(address,address,uint256)', balances)

    delegations = Delegations()
    app.ctx.register(f'{chain_id}.{token_addr}.DelegateVotesChanged(address,uint256,uint256)', delegations)
    app.ctx.register(f'{chain_id}.{token_addr}.DelegateChanged(address,address,address)', delegations)

    if 'ptc' in deployment:
        proposal_types = ProposalTypes()
        app.ctx.register(f'{chain_id}.{ptc_addr}.ProposalTypeSet(uint8,uint16,uint16,string)', proposal_types)


    proposals = Proposals(governor_spec=public_config['governor_spec'])

    PROPOSAL_CREATED_1 = 'ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string)'
    PROPOSAL_CREATED_2 = 'ProposalCreated(uint256,address,address,bytes,uint256,uint256,string,uint8)'
    PROPOSAL_CREATED_3 = 'ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string,uint8)'
    PROPOSAL_CREATED_4 = 'ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string)'

    PROPOSAL_CANCELED = 'ProposalCanceled(uint256)'
    PROPOSAL_QUEUED   = 'ProposalQueued(uint256,uint256)'
    PROPOSAL_EXECUTED = 'ProposalExecuted(uint256)'

    if public_config['governor_spec']['name'] == 'compound':
        app.ctx.register(f'{chain_id}.{gov_addr}.' + PROPOSAL_CREATED_1, proposals)
        PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_1]
    else:
        app.ctx.register(f'{chain_id}.{gov_addr}.' + PROPOSAL_CREATED_2, proposals)
        app.ctx.register(f'{chain_id}.{gov_addr}.' + PROPOSAL_CREATED_3, proposals)
        app.ctx.register(f'{chain_id}.{gov_addr}.' + PROPOSAL_CREATED_4, proposals)
        PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4]

    app.ctx.register(f'{chain_id}.{gov_addr}.' + PROPOSAL_CANCELED, proposals)
    app.ctx.register(f'{chain_id}.{gov_addr}.' + PROPOSAL_QUEUED, proposals)
    app.ctx.register(f'{chain_id}.{gov_addr}.' + PROPOSAL_EXECUTED, proposals)

    PROPOSAL_LIFECYCLE_EVENTS = PROPOSAL_CREATED_EVENTS + [PROPOSAL_CANCELED, PROPOSAL_QUEUED, PROPOSAL_EXECUTED]

    votes = Votes(governor_spec=public_config['governor_spec'])
    app.ctx.register(f'{chain_id}.{gov_addr}.VoteCast(address,uint256,uint8,uint256,string)', votes)


    ##########################
    # Instatiate an "EventFeed", for every...
    #   - network, contract, and relevant event signature.
    #   - a fully-qualified ABI for all contracts in use globally across the app.
    #   - an ordered list of clients where we should pull history of, ideally starting with archive/bulk and ending with JSON-RPC


    ev = EventFeed(chain_id, token_addr, 'Transfer(address,address,uint256)', abis, dcqs)
    app.ctx.add_event_feed(ev)
    app.add_task(ev.boot(app))

    ev = EventFeed(chain_id, token_addr, 'DelegateVotesChanged(address,uint256,uint256)', abis, dcqs)
    app.ctx.add_event_feed(ev)
    app.add_task(ev.boot(app))

    ev = EventFeed(chain_id, token_addr, 'DelegateChanged(address,address,address)', abis, dcqs)
    app.ctx.add_event_feed(ev)
    app.add_task(ev.boot(app))

    if 'ptc' in deployment:
        ev = EventFeed(chain_id, ptc_addr, proposal_type_set_signature, abis, dcqs)
        app.ctx.add_event_feed(ev)
        app.add_task(ev.boot(app))

    for signature in PROPOSAL_LIFECYCLE_EVENTS:
       ev = EventFeed(chain_id, gov_addr, signature, abis, dcqs)
       app.ctx.add_event_feed(ev)
       app.add_task(ev.boot(app))

    ev = EventFeed(chain_id, gov_addr, 'VoteCast(address,uint256,uint8,uint256,string)', abis, dcqs)
    app.ctx.add_event_feed(ev)
    app.add_task(ev.boot(app))


@app.after_server_start
async def subscribe_event_fees(app, loop):

    print("Adding signal handler for each event feed.")
    for ev in app.ctx.event_feeds:
        app.add_task(ev.run(app))


##################################
#
# Tactical DevOps Testing Endpoint
#
##################################

import socket
from sanic import response

@app.get("/health")
@openapi.tag("Checks")
@openapi.summary("Server health check")
async def health_check(request):
    # Get list of files
    try:
        files = os.listdir(DAO_NODE_DATA_PATH)
    except Exception as e:
        # If directory listing fails for some reason, handle it gracefully
        return response.json({"status": "error", "message": str(e)}, status=500)

    # Get server IP address
    try:
        ip_address = socket.gethostbyname(socket.gethostname())
    except Exception as e:
        # If IP resolution fails
        ip_address = "unknown"

    return json({
        "files": files,
        "ip_address": ip_address,
        "config" : public_config,
        "deployment": public_deployment
    })

@app.get("/config")
@openapi.tag("Checks")
@openapi.summary("Server configuration")
async def config_endpoint(request):
    return json({'config' : public_config})

@app.get("/deployment")
@openapi.tag("Checks")
@openapi.summary("Server's Smart Contract set")
async def deployment_endpoint(request):
    return json({'deployment' : public_deployment})

from textwrap import dedent
app.ext.openapi.describe(
    f"DAO Node for {config['friendly_short_name']}",
    version="0.1.0-alpha",
    description=dedent(
        f"""
# About

DAO Node is a blazing fast tip-following read-only API for testable & scaleable Web3 governance apps.

## Fast by Measuring

All responses [include](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Server-Timing) a `server-timing` header.

These are denominated in milliseconds.

Example:

```
server-timing: data;dur=0.070,total;dur=0.481 
```

In the above example, it means the server spent 481 μs processing the full request, and just 70 μs of that on the business-logic of the request.

## Tested & Testing

All endpoints are tested two ways.

1. Unit Tests on the DataProduct objects using static sample data.
2. Unit Tests on the Endpoints using mocked DataProduct objects.

Additionally, DAO Node iteself is intentional about it's architecture to support testing of consumer apps.

DAO Node can boot off an archive, then accept an Anvil Fork for most networks, enabling scripts to create 
on-chain events and reconcile results against the DAO Node API as well as the downstream consumer.

## Intentional & Latest

DAO Node's scope is explicit, in the sense that it does what it was designed to do well, and nothing more.

It is intentional about maintaining on-chain data as of the latest block, without any cache logic or delay.  Only in limited cases can we maintain look-back logic, as these will be a challenge in the long-run to scale. 

It is intentional about avoiding serving slower (Eg. end-of-day, point-in-time, or timeseries) data.

It is not an API for agora-next or any other specific application, but it is designed with agora-next as an intentional first and likely only consumer.  

It is not a replacement for JSON-RPC provider, in the sense that contract-calls don't necessarily make sense to route to DOA Node instead, although in the future that might be a logical evolution.

#  Deployment
```
{yaml.dump(public_deployment, sort_keys=True).strip()}
```

# Config
```
{yaml.dump(public_config, sort_keys=True).strip()}
```
"""
    ),
)

# from sanic_ext import Extend
# Extend(app, config={
#    "openapi_title": "DAO Node",
#     "openapi_description": "API Documentation for My Sanic Service",
#     "openapi_version": "1.0.0",
# })
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8001, dev=True, debug=True)
    #app.run(host="0.0.0.0", port=7654, dev=True, workers=1, access_log=True, debug=True)

