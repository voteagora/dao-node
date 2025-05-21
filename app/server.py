from dotenv import load_dotenv
from pyenvdiff import Environment

load_dotenv()

from importlib.metadata import version as importlib_version
this_env = Environment()

import csv, time, pdb, os, logging
import datetime as dt
import asyncio
from collections import defaultdict
from pathlib import Path
from bisect import bisect_left

import yaml
from google.cloud import storage
from copy import copy

from sanic_ext import openapi
from sanic.worker.manager import WorkerManager
from sanic import Sanic
from sanic.response import text, html, json
from sanic.blueprints import Blueprint
from sanic.log import logger as logr

from .middleware import start_timer, add_server_timing_header, measure
from .clients import CSVClient, JsonRpcHistHttpClient, JsonRpcRTWsClient
from .data_products import Balances, ProposalTypes, Delegations, Proposals, Votes, ParticipationModel
from .signatures import *
from . import __version__
from .logsetup import get_logger 

import random

glogr = get_logger('global')

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

GIT_COMMIT_SHA = os.getenv('GIT_COMMIT_SHA', 'n/a')
glogr.info(f"GIT_COMMIT_SHA={GIT_COMMIT_SHA}")

DAO_NODE_DATA_PATH = Path(os.getenv('DAO_NODE_DATA_PATH', './data'))

def secret_text(t, n):
    if len(t) > ((2 * n) + 3):
        return t[:n] + "..." + t[-1 * n:]
    else:
        return t[:n] + "***..."

DAO_NODE_ARCHIVE_NODE_HTTP = os.getenv('DAO_NODE_ARCHIVE_NODE_HTTP', None)
glogr.info(f"{DAO_NODE_ARCHIVE_NODE_HTTP=}")
if DAO_NODE_ARCHIVE_NODE_HTTP:

    # This pattern enables a deployer to put either the base URL in plane text or the full URL in
    # plain text, leaving ALCHEMY_API_KEY in an optional secret.

    # ...but also use an anvil fork, without any trouble of setting keys.

    ARCHIVE_NODE_HTTP_URL = DAO_NODE_ARCHIVE_NODE_HTTP

    if 'alchemy.com' in DAO_NODE_ARCHIVE_NODE_HTTP:
        ARCHIVE_NODE_HTTP_URL = ARCHIVE_NODE_HTTP_URL + os.getenv('ALCHEMY_API_KEY', '') 
        glogr.info(f"Using alchemy for Archive: {secret_text(ARCHIVE_NODE_HTTP_URL, 6)}")

    if 'quiknode.pro' in DAO_NODE_ARCHIVE_NODE_HTTP:
        ARCHIVE_NODE_HTTP_URL = ARCHIVE_NODE_HTTP_URL + os.getenv('QUICKNODE_API_KEY', '')
        glogr.info(f"Using quiknode.pro for Archive: {secret_text(ARCHIVE_NODE_HTTP_URL, 6)}")
    

DAO_NODE_REALTIME_NODE_WS = os.getenv('DAO_NODE_REALTIME_NODE_WS', None)
if DAO_NODE_REALTIME_NODE_WS:

    # This pattern enables a deployer to put either the base URL in plane text or the full URL in
    # plain text, leaving ALCHEMY_API_KEY in an optional secret.

    # ...but also use an anvil fork, without any trouble of setting keys.

    REALTIME_NODE_WS_URL = DAO_NODE_REALTIME_NODE_WS

    if 'alchemy.com' in DAO_NODE_REALTIME_NODE_WS:
        REALTIME_NODE_WS_URL = REALTIME_NODE_WS_URL + os.getenv('ALCHEMY_API_KEY', '')
        glogr.info(f"Using alchemy for Web Socket: {secret_text(REALTIME_NODE_WS_URL, 6)}")
    
    if 'quiknode.pro' in DAO_NODE_REALTIME_NODE_WS:
        REALTIME_NODE_WS_URL = REALTIME_NODE_WS_URL + os.getenv('QUICKNODE_API_KEY', '')
        glogr.info(f"Using quiknode.pro for Web Socket: {secret_text(REALTIME_NODE_WS_URL, 6)}")


try:
    AGORA_CONFIG_FILE = Path(os.getenv('AGORA_CONFIG_FILE', '/app/config.yaml'))
    with open(AGORA_CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)
    
    glogr.info(config)
    public_config = {k : config[k] for k in ['governor_spec', 'token_spec']}

    deployment = config['deployments'][CONTRACT_DEPLOYMENT]
    del config['deployments']
    public_deployment = {k : deployment[k] for k in ['gov', 'ptc', 'token','chain_id'] if k in deployment}
except:
    glogr.info("Failed to load config of any kind.  DAO Node probably isn't going to do much.")
    config = {
        'friendly_short_name': 'Unknown',
        'deployments': {}
    }
    public_config = {}
    public_deployment = {}
    deployment = {'chain_id' : 1, 'token' : {'address' : '0x0000000000000000000000000000000000000000'}}


ERC20 = public_config['token_spec']['name'] == 'erc20'
NORMAL_STYLE = public_config['token_spec'].get('style', 'normal') == 'normal'
INCLUDE_BALANCES = ERC20 and NORMAL_STYLE

########################################################################

WorkerManager.THRESHOLD = 600 * 45 # 45 minutes

class ClientSequencer:
    def __init__(self, clients):
        self.clients = clients
        self.num = len(clients)
        self.pos = 0
        self.lock = asyncio.Lock()
    
    def __iter__(self):
        self.pos = 0
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
        self.booting = True

    @property
    def name(self):
        return f"EventFeed({self.chain_id}, {self.address}, {self.signature})"

    def archive_read(self):
        previous_csv_client_failed_filenotfound = False 

        for i, client in enumerate(self.cs):

            if client.timeliness == 'archive':

                if i > 0 and not previous_csv_client_failed_filenotfound:
                    self.block = max(self.block, client.get_fallback_block(self.signature))
                if previous_csv_client_failed_filenotfound:
                    previous_csv_client_failed_filenotfound = False

                emoji = random.choice(['😀', '🎉', '🚀', '🐍', '🔥', '🌈', '💡', '😎'])

                logr.info(f"{emoji} Reading from {client.timeliness} client of type {type(client)} from block {self.block}")

                try:
                    reader = client.read(self.chain_id, self.address, self.signature, self.abis, after=self.block)

                    cnt = 0
                    for event in reader:
                        cnt += 1
                        self.block = max(self.block, int(event['block_number']))
                        yield event

                    logr.info(f"{emoji} Done reading {cnt} {self.signature} events as block {self.block}")

                except FileNotFoundError as e:
                    logr.warn(f"{emoji} File not found for {self.signature} by {type(client).__name__}: {e}. Skipping to next client.")
                    previous_csv_client_failed_filenotfound = isinstance(client, CSVClient)
                    continue

    async def realtime_async_read(self):

        async for client in self.cs.get_async_iterator():

            if client.timeliness == 'realtime':

                logr.info(f"Reading from {client.timeliness} client of type {type(client)}")

                if self.block is None:
                    raise Exception("Unexpected configuration.  Please provide at least one archive, or send a PR to support archive-free mode!")

                reader = client.read(self.chain_id, self.address, self.signature, self.abis, after=self.block)

                async for event in reader:

                    self.block = max(self.block, int(event['block_number']))

                    yield event

    async def boot(self, app):
        
        cnt = 0

        start = dt.datetime.now()

        logr.info(f"Loading {self.chain_id}.{self.address}.{self.signature}")

        data_product_dispatchers = app.ctx.dps[f"{self.chain_id}.{self.address}.{self.signature}"]

        for event in self.archive_read():
            cnt += 1
            for data_product_dispatcher in data_product_dispatchers:
                data_product_dispatcher.handle(event)

            if (cnt % 1_000_000) == 0:
                logr.info(f"loaded {cnt} so far {( dt.datetime.now() - start).total_seconds()}")
        
        end = dt.datetime.now()
        
        await asyncio.sleep(.01)

        self.booting = False

        logr.info(f"Done booting {cnt} records in {(end - start).total_seconds()} seconds.")

        return 
    
    async def run(self, app):

        data_product_event_dispatchers = app.ctx.dps[f"{self.chain_id}.{self.address}.{self.signature}"]

        async for event in self.realtime_async_read():
            for data_product_event_dispatcher in data_product_event_dispatchers:
                event['signature'] = self.signature
                data_product_event_dispatcher.handle(event)

            # See note below about Sanic Signals

            # sig = f"{self.chain_id}.{self.address}.{self.signature}"
            # await app.dispatch("data.model." + sig, context=event)

class BlockFeed:
    def __init__(self, chain_id, client_sequencer):
        self.chain_id = chain_id
        self.cs = client_sequencer
        self.block = 0
        self.booting = True

    @property
    def name(self):
        return f"BlockFeed({self.chain_id})"

    def archive_read(self):
        previous_csv_client_failed_filenotfound = False 

        for i, client in enumerate(self.cs):

            if client.timeliness == 'archive':

                emoji = random.choice(['😀', '🎉', '🚀', '🐍', '🔥', '🌈', '💡', '😎'])

                logr.info(f"{emoji} Reading from {client.timeliness} client of type {type(client)} from block {self.block}")

                try:
                    reader = client.read_blocks(self.chain_id, self.block)

                    cnt = 0
                    for event in reader:
                        cnt += 1
                        self.block = max(self.block, int(event['block_number']))
                        yield event

                    logr.info(f"{emoji} Done reading {cnt} blocks, latest is {self.block}")

                except FileNotFoundError as e:
                    logr.warn(f"{emoji} File not found for {self.signature} by {type(client).__name__}: {e}. Skipping to next client.")
                    previous_csv_client_failed_filenotfound = isinstance(client, CSVClient)
                    continue

    async def realtime_async_read(self):

        async for client in self.cs.get_async_iterator():

            if client.timeliness == 'realtime':

                logr.info(f"Reading from {client.timeliness} client of type {type(client)}")

                if self.block is None:
                    raise Exception("Unexpected configuration.  Please provide at least one archive, or send a PR to support archive-free mode!")

                reader = client.read_blocks(self.chain_id, self.block)

                async for event in reader:
                    self.block = max(self.block, event['block_number'])
                    yield event

    async def boot(self, app):
        
        cnt = 0

        start = dt.datetime.now()

        logr.info(f"Loading {self.chain_id}")

        data_product_dispatchers = app.ctx.dps[f"{self.chain_id}"]

        for event in self.archive_read():
            cnt += 1
            for data_product_dispatcher in data_product_dispatchers:
                data_product_dispatcher.handle_block(event)

            if (cnt % 1_000_000) == 0:
                logr.info(f"loaded {cnt} so far {( dt.datetime.now() - start).total_seconds()}")
        
        end = dt.datetime.now()
        
        await asyncio.sleep(.01)

        self.booting = False

        logr.info(f"Done booting {cnt} records in {(end - start).total_seconds()} seconds.")

        return 
    
    async def run(self, app):

        data_product_event_dispatchers = app.ctx.dps[f"{self.chain_id}"]

        async for block in self.realtime_async_read():
            for data_product_event_dispatcher in data_product_event_dispatchers:
                data_product_event_dispatcher.handle_block(block)


class DataProductContext:
    def __init__(self):

        self.dps = defaultdict(list)
        self.feeds = []
        self.feed_meta = defaultdict(list)
    
    def handle_dispatch(self, chain_id_contract_signature, context):

        logr.info(f"Handle Dispatch Called : {chain_id_contract_signature}")

        data_product_dispatchers = self.dps[chain_id_contract_signature]

        if len(data_product_dispatchers):
            raise Exception(f"No data products registered for {chain_id_contract_signature}")

        for data_product in self.dps[chain_id_contract_signature]:
            data_product.handle(context)

    def register(self, chain_id_contract_signature, data_product):

        if "." in chain_id_contract_signature:
            
            _, contract, signature = chain_id_contract_signature.split(".")

            self.feed_meta[contract].append(signature)

        self.dps[chain_id_contract_signature].append(data_product)

        setattr(self, data_product.name, data_product)


    def add_feed(self, feed):
        self.feeds.append(feed)
    
app = Sanic('DaoNode', ctx=DataProductContext())
app.middleware('request')(start_timer)
app.middleware('response')(add_server_timing_header)

# Create static blueprint
static_bp = Blueprint('static')
app.static('/static', './static')

@app.get('/')
async def index(request):
    with open('./static/html/index.html') as f:
        return html(f.read())

@app.get('/ui/proposals')
async def proposals_ui(request):
    with open('./static/html/proposals.html') as f:
        return html(f.read())

@app.get('/ui/delegates')
async def delegates_ui(request):
    with open('./static/html/delegates.html') as f:
        return html(f.read())

@app.get('/ui/proposal')
async def proposal_ui(request):
    with open('./static/html/proposal.html') as f:
        return html(f.read())

# TODO: Figure out Sanic Signals
# 
# For some reason, I couldn't get this pattern to work such that the events
# are processed using the "signal"-pattern enabled by sanic.
# I'm not sure why.  It would be more elegant if we could use it, but, alas it
# it's not working, but enabled without the framework.
# 
# @app.signal("data.model.<chain_id_contract_signature>")
# async def log_event_signal_handler(chain_id_contract_signature, **context):
#     logr.info(f"Handling: {chain_id_contract_signature}")
#     app.ctx.handle_dispatch(chain_id_contract_signature, context)

######################################################################
#
# Application Endpoints
#
######################################################################

if INCLUDE_BALANCES:
    @app.route('/v1/balance/<addr>')
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

#############################################################################################################################################

@app.route('/v1/proposals')
@openapi.tag("Proposal State")
@openapi.summary("All proposals with the latest state of their outcome.")
@openapi.parameter(
    "set", 
    str, 
    location="query", 
    required=False, 
    default="all",
    description="Flag to filter the list of proposals, down to only the ones which are relevant."
)
@openapi.parameter(
    "sort", 
    str, 
    location="query", 
    required=False, 
    default="", 
    description="Key to sort the list of proposals by.  Recommended values: id, block_number, proposer, start_block, end_block"
)
@measure
async def proposals(request):
    return await proposals_handler(app, request)

async def proposals_handler(app, request):
    proposal_set = request.args.get("set", "all").lower()
    sort_key = request.args.get("sort", "").lower()


    if proposal_set == 'relevant':
        res = app.ctx.proposals.relevant()
    else:
        res = app.ctx.proposals.unfiltered()

    proposals = []
    for prop in res:
        proposal = prop.to_dict()
        totals = app.ctx.votes.proposal_aggregations[proposal['id']].totals()
        proposal['totals'] = totals
        proposals.append(proposal)
    
    if sort_key:
        proposals.sort(key=lambda x: x[sort_key], reverse=True)

    return json({'proposals' : proposals})

##################################################################################################################################################

@app.route('/v1/proposal/<proposal_id>')
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
    return await proposal_handler(app, request, proposal_id)

async def proposal_handler(app, request, proposal_id):
    proposal = app.ctx.proposals.proposals[proposal_id].to_dict()

    proposal = copy(proposal)

    totals = app.ctx.votes.proposal_aggregations[proposal_id].totals()
    proposal['totals'] = totals

    voting_record = app.ctx.votes.proposal_vote_record[proposal_id]
    proposal['voting_record'] = voting_record

    return json({'proposal' : proposal})

@app.route('/v1/proposal_types')
@openapi.tag("Proposal State")
@openapi.summary("Latest information all proposal types")
@measure
async def proposal_types(request):
    return await proposal_types_handler(app, request)

async def proposal_types_handler(app, request):
	return json({'proposal_types' : app.ctx.proposal_types.proposal_types})

DEFAULT_PAGE_SIZE = 200
DEFAULT_OFFSET = 0

@app.route('/v1/delegates')
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

### 🟡 Sorting only (by VP, Delegator-Count, or Last-Vote-Block)
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

- 🟡 - Some unit tests for the underlying data model.

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
    description="Sort by either voting-power ('VP'), delegator-count ('DC'), most-recent-delegation ('MRD'), oldest-delegation ('OLD'), last-vote-block ('LVB') or 7-day-voting-power-change ('VPC')."
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
    description="Comma separated list of other dimensions to include, beyond the sort-by criteria. Use 'VP' for voting power, 'DC' for delegator count, 'PR' for participation rate, and 'VPC' for 7-day voting power change."
)
@measure
async def delegates(request):
    return await delegates_handler(app, request)

async def delegates_handler(app, request):

    sort_by = request.args.get("sort_by", 'VP')
    print(sort_by)
    
    sort_by_vp  = sort_by == 'VP'  # Voting Power
    sort_by_dc  = sort_by == 'DC'  # Delegator Count
    sort_by_pr  = False            # Partipcipation Rate ( Not supported yet) 
    sort_by_lvb = sort_by == 'LVB' # Last Vote Block
    sort_by_mrd = sort_by == 'MRD' # Most Recent Delegation
    sort_by_old = sort_by == 'OLD' # Oldest Delegation
    sort_by_vpc = sort_by == 'VPC' # 7-day Voting Power Change

    offset = int(request.args.get("offset", DEFAULT_OFFSET))
    page_size = int(request.args.get("page_size", DEFAULT_PAGE_SIZE))

    reverse = request.args.get("reverse", "true").lower() == "true"

    include = request.args.get("include", 'DC,PR').split(",")

    # Get the initial list based on sort criteria
    if sort_by_vp:
        out = list(app.ctx.delegations.delegatee_vp.items())
    elif sort_by_mrd:
        out = [(addr, int(event['block_number'])) 
               for addr, event in app.ctx.delegations.delegatee_latest_event.items()]
    elif sort_by_old:
        out = [(addr, int(event['block_number'])) 
               for addr, event in app.ctx.delegations.delegatee_oldest_event.items()]
    elif sort_by_dc:
        out = list(app.ctx.delegations.delegatee_cnt.items())
    elif sort_by_lvb:
        out = [(addr, int(app.ctx.votes.latest_vote_block.get(addr, 0)))
               for addr in app.ctx.delegations.delegatee_vp.keys()]
        out  = [obj for obj in out if obj[1] > 0] # TODO This should not be necessary. The data model should prune zeros.
    elif sort_by_vpc:
        out = [(addr, app.ctx.delegations.delegate_seven_day_vp_change(addr))
               for addr in app.ctx.delegations.delegatee_vp.keys()]
    else:
        raise Exception(f"Sort by '{sort_by}' not implemented.")

    out.sort(key=lambda x: x[1], reverse=reverse)    

    if offset:
        out = out[offset:]

    if page_size:
        if len(out) > page_size:
            out = out[:page_size]

    # Cast big numbers to str, only after sorting and cropping...
    if sort_by_vp or sort_by_vpc:
        out = [(addr, str(v)) for addr, v in out]

    add_delegator_count = 'DC' in include or sort_by_dc
    add_participation_rate = 'PR' in include or sort_by_pr
    add_voting_power = 'VP' in include or sort_by_vp
    add_last_vote_block = 'LVB' in include or sort_by_lvb
    add_most_recent_delegation = 'MRD' in include or sort_by_mrd
    add_oldest_delegation = 'OLD' in include or sort_by_old
    add_seven_day_vp_change = 'VPC' in include or sort_by_vpc

    if add_participation_rate:
        pm = ParticipationModel(app.ctx.proposals, app.ctx.votes)

    # Everything below, replaces that gawdaful combinatorical mess with a single dict(**(k, func(v)),
    # where func(v) is a lambda that returns the value for key k, but ... this might actually be slower
    # than writing a million if-statements out, per-row, because we're iterating over transformers
    # dynamically at runtime, rather than at compile time.  Would love to profile the diff...
    # but alas, I'm out of time...

    # If this is terribly slow...backup plan is now the idea Carl had, and if both of these
    # are too slow, then Jeff will write the million if-statements out.

    voting_power_func = lambda x, y: str(app.ctx.delegations.delegatee_vp[x])
    from_cnt_func = lambda x, y: app.ctx.delegations.delegatee_cnt[x]
    participation_func = lambda x, y: pm.calculate(x)
    last_vote_block_func = lambda x, y: app.ctx.votes.latest_vote_block.get(x, 0)
    most_recent_delegation_func = lambda x, y: app.ctx.delegations.delegatee_latest_event[x]['block_number']
    oldest_delegation_func = lambda x, y: app.ctx.delegations.delegatee_oldest_event[x]['block_number']
    seven_day_vp_change_func = lambda x, y: str(app.ctx.delegations.delegate_seven_day_vp_change(x))

    use_sort_key = lambda x, y: y
    addr_func = lambda x, y: x
    
    transformers = [('addr', addr_func)]

    if add_voting_power:
        transformers.append(('VP',  use_sort_key if sort_by_vp else voting_power_func))
    if add_delegator_count:
        transformers.append(('DC',  use_sort_key if sort_by_dc else from_cnt_func))
    if add_participation_rate:
        transformers.append(('PR',  participation_func))
    if add_last_vote_block:
        transformers.append(('LVB', use_sort_key if sort_by_lvb else last_vote_block_func))
    if add_most_recent_delegation:
        transformers.append(('MRD', use_sort_key if sort_by_mrd else most_recent_delegation_func))
    if add_oldest_delegation:
        transformers.append(('OLD', use_sort_key if sort_by_old else oldest_delegation_func))
    if add_seven_day_vp_change:
        transformers.append(('VPC', use_sort_key if sort_by_vpc else seven_day_vp_change_func))

    out = [dict([(k, func(addr, sort_val)) for k, func in transformers]) for addr, sort_val in out]

    return json({'delegates': out})

############################################################################################################################################################

async def delegate_handler(app, request, addr):
    from_list_with_info = []

    for delegator, (block_number, transaction_index) in app.ctx.delegations.delegatee_list[addr].items():
        
        if addr in app.ctx.delegations.delegation_amounts and delegator in app.ctx.delegations.delegation_amounts[addr]:
            amount = app.ctx.delegations.delegation_amounts[addr][delegator]
        else:
            amount = 10000

        row = {'delegator' : delegator, 'percentage' : amount, 'bn' : block_number, 'tid' : transaction_index}

        if INCLUDE_BALANCES:
            balance = str(app.ctx.balances.balance_of(delegator))
            row['balance'] = balance

        from_list_with_info.append(row)

    return json({'delegate' : 
                {'addr' : addr,
                'from_cnt' : app.ctx.delegations.delegatee_cnt[addr],
                'from_list' : from_list_with_info,
                'voting_power' : str(app.ctx.delegations.delegatee_vp[addr])}})

@app.route('/v1/delegate/<addr>')
@openapi.tag("Delegation State")
@openapi.summary("Information about a specific delegate")
@measure
async def delegate(request, addr):
    return await delegate_handler(app, request, addr)

@app.route('/v1/delegate/<addr>/voting_history')
@openapi.tag("Delegate Participation")
@openapi.summary("Information about a specific delegate's voting history")
@measure
async def delegate_voting_history(request, addr):
    voting_history = app.ctx.votes.voter_history[addr]

    return json({'voting_history' : voting_history})


@app.route('/v1/delegate_vp/<addr>/<block_number>')
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
async def delegate_vp(request, addr : str, block_number : str):
    return await delegate_vp_handler(app, request, addr, block_number)

async def delegate_vp_handler(app, request, addr, block_number):

    vp_history = [(0, 0)] + app.ctx.delegations.delegatee_vp_history[addr]
    index = bisect_left(vp_history, (int(block_number),)) - 1

    index = max(index, 0)

    try:
        vp = vp_history[index][1]
    except:
        vp = 0

    return json({'voting_power' : vp,
                 'delegate' : addr,
                 'block_number' : block_number,
                 'history' : vp_history[1:]})

#################################################################################################################################################

@app.route('/v1/voting_power')
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
# ⏫ 🌎 BOOT SEQUENCE
#
################################################################################

@app.before_server_start(priority=0)
async def bootstrap_event_feeds(app, loop):

    #################################################################################
    # ⚡️ 📀 Client Setup

    clients = []

    csvc = CSVClient(DAO_NODE_DATA_PATH)
    if csvc.is_valid():
        clients.append(csvc)
    
    rpcc = JsonRpcHistHttpClient(ARCHIVE_NODE_HTTP_URL)
    if rpcc.is_valid():
       clients.append(rpcc)

    jwsc = JsonRpcRTWsClient(REALTIME_NODE_WS_URL)
    if jwsc.is_valid():
       clients.append(jwsc)

    # Create a sequence of clients to pull events from.  Each with their own standards for comms, drivers, API, etc. 
    dcqs = ClientSequencer(clients) 

    #################################################################################
    # 👀 🕹️ ABI Setup - Load the ABIs relevant for the DAO.  

    # Get a full picture of all available contracts relevant for this DAO.
    chain_id = int(deployment['chain_id'])
    AGORA_GOV = public_config['governor_spec']['name'] == 'agora'

    abi_list = []

    token_addr = deployment['token']['address'].lower()
    logr.info(f"Using {token_addr=}")
    token_abi = ABI.from_internet('token', token_addr, chain_id=chain_id, implementation=True)
    abi_list.append(token_abi)

    gov_addr = deployment['gov']['address'].lower()
    logr.info(f"Using {gov_addr=}")

    GOV_ABI_OVERRIDE_URL = os.getenv('GOV_ABI_OVERRIDE_URL', None)
    if GOV_ABI_OVERRIDE_URL:
        logr.info("Overriding Gov ABI")
        gov_abi = ABI.from_url('gov', GOV_ABI_OVERRIDE_URL)
    else:
        gov_abi = ABI.from_internet('gov', gov_addr, chain_id=chain_id, implementation=True)
    abi_list.append(gov_abi)

    if 'ptc' in deployment:
        ptc_addr = deployment['ptc']['address'].lower()
        logr.info(f"Using {ptc_addr=}")
        ptc_abi = ABI.from_internet('ptc', ptc_addr, chain_id=chain_id, implementation=True)
        abi_list.append(ptc_abi)

    abis = ABISet('daonode', abi_list)

    #################################################################################
    # 🎪 🧠 Instantiate "Data Products".  These are the singletons that store data 
    #      in RAM, that need to be maintained for every event.

    if INCLUDE_BALANCES:
        balances = Balances(token_spec=public_config['token_spec'])
        app.ctx.register(f'{chain_id}.{token_addr}.{TRANSFER}', balances)

    delegations = Delegations()
    app.ctx.register(f'{chain_id}', delegations)
    app.ctx.register(f'{chain_id}.{token_addr}.{DELEGATE_VOTES_CHANGE}', delegations)

    if 'IVotesPartialDelegation' in public_config['token_spec'].get('interfaces', []):
        app.ctx.register(f'{chain_id}.{token_addr}.{DELEGATE_CHANGED_2}', delegations)
    else:
        app.ctx.register(f'{chain_id}.{token_addr}.{DELEGATE_CHANGED_1}', delegations)

    if 'ptc' in deployment:
        proposal_types = ProposalTypes()

        PROP_TYPE_SET_SIGNATURE = None

        for prop_type_set_signature in [PROP_TYPE_SET_1, PROP_TYPE_SET_2, PROP_TYPE_SET_3, PROP_TYPE_SET_4]:
            if abis.get_by_signature(prop_type_set_signature):
                app.ctx.register(f'{chain_id}.{ptc_addr}.{prop_type_set_signature}', proposal_types)
                PROP_TYPE_SET_SIGNATURE = prop_type_set_signature
        
        if AGORA_GOV and public_config['governor_spec']['version'] >= 1.1:
            app.ctx.register(f'{chain_id}.{ptc_addr}.{SCOPE_CREATED}' , proposal_types)
            app.ctx.register(f'{chain_id}.{ptc_addr}.{SCOPE_DISABLED}', proposal_types)
            app.ctx.register(f'{chain_id}.{ptc_addr}.{SCOPE_DELETED}' , proposal_types)

    proposals = Proposals(governor_spec=public_config['governor_spec'])

    gov_spec_name = public_config['governor_spec']['name']
    if gov_spec_name in ('compound', 'ENSGovernor'):
        PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_1]
    elif gov_spec_name == 'agora' and public_config['governor_spec']['version'] == 0.1:
        PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_1, PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4]
    elif gov_spec_name == 'agora':
        PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_2, PROPOSAL_CREATED_4]
    else:
        raise Exception(f"Govenor Unsupported: {gov_spec_name}")

    PROPOSAL_LIFECYCLE_EVENTS = PROPOSAL_CREATED_EVENTS + [PROPOSAL_CANCELED, PROPOSAL_QUEUED, PROPOSAL_EXECUTED]
    for PROPOSAL_EVENT in PROPOSAL_LIFECYCLE_EVENTS:
        app.ctx.register(f'{chain_id}.{gov_addr}.' + PROPOSAL_EVENT, proposals)

    VOTE_EVENTS = [VOTE_CAST_1]    
    if not (public_config['governor_spec']['name'] in ('compound', 'ENSGovernor')):
        VOTE_EVENTS.append(VOTE_CAST_WITH_PARAMS_1)

    votes = Votes(governor_spec=public_config['governor_spec'])
    for VOTE_EVENT in VOTE_EVENTS:
        app.ctx.register(f'{chain_id}.{gov_addr}.' + VOTE_EVENT, votes)

    #################################################################################
    # 🎪 🍔 Instantiate an "Event Feed" for every network, contract, and relevant 
    #       event signature.  Then register each one with the client sequencer so it
    #       can know to read in the past and subscribe to the future.
    #       This has been automatically handled, by picking up metadata from
    #       the data product registration step.  

    cf = BlockFeed(chain_id, dcqs)
    app.ctx.add_feed(cf)
    app.add_task(cf.boot(app))

    for address, signatures in app.ctx.feed_meta.items():
        for signature in signatures:
            ef = EventFeed(chain_id, address, signature, abis, dcqs)
            app.ctx.add_feed(ef)
            app.add_task(ef.boot(app))

@app.after_server_start
async def subscribe_feeds(app):

    logr.info("Adding signal handler for each feed.")
    for feed in app.ctx.feeds:
        logr.info(f"Invoking feed.run(app) for {feed.name}")
        app.add_task(feed.run(app))

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
        "deployment": public_deployment,
        "version": __version__,
        "gitsha": GIT_COMMIT_SHA,
        "env": {'PipDistributions' : {mod : importlib_version(mod) for mod in ['websockets', 'web3', 'sanic', 'sanic-ext', 'abifsm']}}
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
    version=__version__,
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
    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8004, dev=True, debug=True)
    #app.run(host="0.0.0.0", port=7654, dev=True, workers=1, access_log=True, debug=True)

