from dotenv import load_dotenv
from pyenvdiff import Environment

load_dotenv()

from importlib.metadata import version as importlib_version
this_env = Environment()

import time, os
import asyncio
from collections import defaultdict
from pathlib import Path
from datetime import datetime
from random import randint
import yaml
from copy import copy, deepcopy
import json as j
import random
from eth_utils import to_checksum_address

from pympler import asizeof

from sanic_ext import openapi
from sanic.worker.manager import WorkerManager
from sanic import Sanic
from sanic.response import html, json
from sanic.blueprints import Blueprint
from sanic.log import logger as logr

from .middleware import start_timer, add_server_timing_header, measure
from .profiling import Profiler

from .clients_csv import CSVClient
from .clients_httpjson import JsonRpcHistHttpClient, JsonRpcRtHttpClient
from .clients_wsjson import JsonRpcRtWsClient

from .data_products import Balances, ProposalTypes, Delegations, Proposals, Votes
from .data_models import ParticipationRateModel

from .signatures import *
from . import __version__
from .logsetup import get_logger 
from .dev_modes import CAPTURE_CLIENT_OUTPUTS_TO_DISK, CAPTURE_WS_CLIENT_OUTPUTS, PROFILE_ARCHIVE_CLIENT, ENABLE_BALANCES, ENABLE_DELEGATION

if  CAPTURE_WS_CLIENT_OUTPUTS:
    from copy import deepcopy


BOOT_TIME = datetime.now().isoformat()
WORKER_ID = str(randint(0, 100000000000000000)) # just a big number to avoid collissions.

glogr = get_logger('global')

glogr.info(f"{WORKER_ID=}")
glogr.info(f"{BOOT_TIME=}")

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
INCLUDE_BALANCES = ERC20 and NORMAL_STYLE and ENABLE_BALANCES

########################################################################

WorkerManager.THRESHOLD = 600 * 45 # 45 minutes

class ClientSequencer:
    def __init__(self, clients):
        self.clients = clients
        self.num = len(clients)
        self.pos = 0
        self.lock = asyncio.Lock()

    def set_abis(self, abis):
        for client in self.clients:
            client.set_abis(abis)
    
    def __iter__(self):
        self.pos = 0
        return self

    def __next__(self): 

        self.pos += 1
        if self.pos <= self.num:
            return self.pos, self.clients[self.pos - 1]

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
                return self.pos,client
            
            self.pos = 0  # Reset for reuse
            raise StopAsyncIteration

    def get_async_iterator(self):
        return self 
    
    def plan(self, *signal_meta):
        for client in self.clients:
            try:
                client.plan(*signal_meta)
            except Exception as e:
                logr.info(f"E188250528 - Failed to plan [{signal_meta}] for {client}: {e}")

    
class Feed:
    def __init__(self):
        self.block = 0
        self.booting = True
        self.meta = []
        self.profiler = Profiler()
        self.capture_counter = defaultdict(int)

        self.event_history = [] # this one is for diagnostics, can be disabled after we're stable.

        self.event_history_dict = defaultdict(list) # this one is for deduplicating events we've heard.  #TODO - prune this if we're stable fore more than a day.
        self.event_history_tracking_lock = asyncio.Lock()
    
        self.archive_signal_counts = defaultdict(int)
        self.realtime_signal_counts = defaultdict(int)
        self.total_signal_counts = defaultdict(int)

    def set_client_sequencer(self, client_sequencer):
        self.cs = client_sequencer

        for signal_meta in self.meta:
            self.cs.plan(*signal_meta)
    
    def plan_block(self, chain_id):
        self.meta.append(('block', (chain_id,)))

    def plan_event(self, chain_id, address, signature):
        self.meta.append(('event', (chain_id, address, signature)))
    
    def set_abis(self, abis):
        self.cs.set_abis(abis)

    def read_archive(self):

        for i, client in self.cs:

            if client.timeliness == 'archive':

                self.block = max(self.block, client.get_fallback_block())

                start = time.perf_counter()

                emoji = random.choice(['😀', '🎉', '🚀', '🐍', '🔥', '🌈', '💡', '😎'])

                logr.info(f"{emoji} Reading from client #{i} of type {type(client).__name__} from block {self.block}")

                reader = client.read(after=self.block)

                cnt = 0
 
                for event, signal, new_signal in reader:
                    cnt += 1

                    # TODO - make the archive produce a block-history, per tenant, not per chain
                    # as is, the event-feed won't line up.
                    if 'blocks' not in signal:
                        self.block = max(self.block, int(event['block_number']))

                    self.archive_signal_counts[signal] += 1
                    self.total_signal_counts[signal] += 1

                    if CAPTURE_CLIENT_OUTPUTS_TO_DISK:
                        self.capture_client_output_to_disk(event, client_type=type(client))                        

                    if PROFILE_ARCHIVE_CLIENT:
                        with self.profiler(signal):
                            yield event, signal, new_signal
                    else:
                        yield event, signal, new_signal

                end = time.perf_counter()

                dur = end - start

                if PROFILE_ARCHIVE_CLIENT:
                    self.profiler.report()

                logr.info(f"{emoji} Done reading {cnt} block-headers and event-logs as of block {self.block}.  Took {dur:.2f} seconds.")
            
            self.block = self.block + 1

    def capture_ws_client_output(self, event):
        self.event_history.append(event)
        
    def capture_client_output_to_disk(self, event, client_type):

        if 'timestamp' in event:
            loc = f"tests/client_outputs/blocks/{client_type.__name__}"
            fname = f"{loc}/{event['block_number']}.json"
        else:
            signature = event.get('signature')
            loc = f"tests/client_outputs/{signature}/{client_type.__name__}"
            fname = f"{loc}/{event['block_number']}-{event['transaction_index']}-{event['log_index']}.json"

        if self.capture_counter[loc] > 10:
            return
            
        os.makedirs(loc, exist_ok=True)

        self.capture_counter[loc] +=1

        logr.info(f"Writing to {fname}")
       
        with open(fname, "w") as f:
            try:
                j.dump(event, f, indent=2)
            except:
                logr.info("Couldn't serialize this object:")
                print(event)
                pass


    async def realtime_async_read(self, rt_client_num=0):

        async for i, client in self.cs.get_async_iterator():

            if client.timeliness in ('realtime', 'polling') and rt_client_num == i:

                logr.info(f"Reading from client #{i} of type {type(client)}")

                if self.block is None:
                    raise Exception("Unexpected configuration.  Please provide at least one archive, or send a PR to support archive-free mode!")

                async for event in client.read():

                    block_num = int(event['block_number'])
                    self.block = max(self.block, block_num)

                    # This right here, makes it possible to have multiple 
                    # competing web sockets.
                    # And then, for each block number, we track events
                    # we've processed.
                    # If we've heard it once before, we skip it.
                    try:
                        pair = event['transaction_index'], event['log_index']
                    except:
                        pair = -1, -1 # block

                    async with self.event_history_tracking_lock:
                        if pair in self.event_history_dict[block_num]:
                            continue                    
                        self.event_history_dict[block_num].append(pair)

                    self.realtime_signal_counts[event['signal']] += 1
                    self.total_signal_counts[event['signal']] += 1
                    
                    if CAPTURE_CLIENT_OUTPUTS_TO_DISK:
                        self.capture_client_output_to_disk(event, client_type=type(client))
                    
                    if CAPTURE_WS_CLIENT_OUTPUTS:

                        self.capture_ws_client_output(deepcopy(event))
                        try:
                            del event['removed']
                            del event['txhash']
                        except:
                            pass

                    yield event



class DataProductContext:
    def __init__(self):

        self.dps = defaultdict(list)
        self.dps_names = defaultdict(list)
        self.feed = Feed()

    def register(self, chain_id_contract_signature, data_product):

        if 'blocks' in chain_id_contract_signature:
            self.feed.plan_block(chain_id=int(chain_id_contract_signature.split('.')[0]))
        else:
            chain_id, address, signature = chain_id_contract_signature.split('.')
            self.feed.plan_event(chain_id=int(chain_id), address=address, signature=signature)

        self.dps[chain_id_contract_signature].append(data_product)
        
        setattr(self, data_product.name, data_product)
    
    def register_model(self, model):
        setattr(self, model.name, model)

    def set_signal_context(self, chain_id_contract_signature):
        self.signal_context = self.dps[chain_id_contract_signature]

    def dispatch_from_archive(self, event):
        for data_product in self.signal_context:
            data_product.handle(event)


    async def dispatch_from_realtime(self, event):

        chain_id_contract_signature = event['signal']
        del event['signal']

        dps = self.dps[chain_id_contract_signature]

        for data_product in dps:
            data_product.handle(event)  

    
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

    # We intentionally removed this.  Votes need to be sorted, and cache
    # behaviour is different.  And there are likeely client
    # features that do things per-vote, that make consuming this 
    # entire data set unlikely.
    
    # voting_record = app.ctx.votes.proposal_vote_record[proposal_id]
    # proposal['voting_record'] = voting_record

    return json({'proposal' : proposal})

VOTE_RECORD_DEFAULT_PAGE_SIZE = 100
VOTE_RECORD_DEFAULT_OFFSET = 0

@app.route('/v1/vote_record/<proposal_id>')
@openapi.tag("Proposal State")
@openapi.summary("Voting history for a specific delegate")
@openapi.parameter(
    "sort_by", 
    str, 
    location="query", 
    required=False, 
    default='BN',
    description="Sort by either block number ('BN') or voting power of the vote that was cast ('VP')"
)
@openapi.parameter(
    "page_size", 
    int, 
    location="query", 
    required=False, 
    default=VOTE_RECORD_DEFAULT_PAGE_SIZE,
    description="Number of records to return in one response."
)
@openapi.parameter(
    "offset", 
    int, 
    location="query", 
    required=False, 
    default=VOTE_RECORD_DEFAULT_OFFSET,
    description="Number of records to skip (ie zero-indexed) from the start."
)
@openapi.parameter(
    "reverse", 
    bool, 
    location="query", 
    required=False, 
    default=False,
    description="To sort descending (largest value first) set to true.  Defaults to false."
)
@openapi.parameter(
    "full", 
    bool, 
    location="query", 
    required=False, 
    default=False,
    description="Ignore pagination parameters and return the full vote record."
)
@measure
async def vote_record(request, proposal_id):

    return await vote_record_handler(app, request, proposal_id)

async def vote_record_handler(app, request, proposal_id):

    sort_by = request.args.get("sort_by", "BN")
    reverse = request.args.get("reverse", "false").lower() == "true"
    full = request.args.get("full", "false").lower() == "true"

    if sort_by == 'BN':
        if reverse:
            vr = deepcopy(app.ctx.votes.proposal_vote_record[proposal_id])
            vr.sort(key=lambda x: int(x['bn']), reverse=True)
        else:
            # Since the events are ordered, we don't need to take a 
            # deep copy nor sort.  This reduces the API call from 35 ms to 1 ms 
            # when loading a chart in chronological order.
            vr = app.ctx.votes.proposal_vote_record[proposal_id]
    elif sort_by == 'VP':
        vr = deepcopy(app.ctx.votes.proposal_vote_record[proposal_id])
        key = 'weight' if 'weight' in vr[0] else 'votes'    
        vr.sort(key=lambda x: x[key], reverse=reverse)
    else:
        raise Exception(f"Invalid sort_by: {sort_by}")

    offset = int(request.args.get("offset", VOTE_RECORD_DEFAULT_OFFSET))
    page_size = int(request.args.get("page_size", VOTE_RECORD_DEFAULT_PAGE_SIZE))

    if full:
        has_more = False
    else:
        vr = vr[offset:]
        has_more = len(vr) > page_size
        vr = vr[:page_size]

    proposal_type = app.ctx.proposals.proposals[proposal_id].get_proposal_type(app.ctx.proposal_types.proposal_types)
    return json({'vote_record' : vr,
                 'has_more' : has_more,
                 'proposal_type' : proposal_type})

@app.route('/v1/vote')
@openapi.tag("Proposal State")
@openapi.summary("Vote cast by a specific delegate")
@openapi.parameter(
    "proposal_id", 
    str, 
    location="query", 
    required=True, 
)
@openapi.parameter(
    "voter", 
    str, 
    location="query", 
    required=True, 
)
@measure
async def vote(request):
    return await vote_handler(app, request)

async def vote_handler(app, request):

    proposal_id = request.args.get("proposal_id")
    voter = request.args.get("voter")

    votes = app.ctx.votes.voter_history[voter]
    vote = [v for v in votes if v['proposal_id'] == proposal_id]
    return json({'vote' : vote})

@app.route('/v1/voter_history/<voter>')
@openapi.tag("Proposal State")
@openapi.summary("Voting history for a specific delegate")
@measure
async def voter_history(request, voter):
    return await voter_history_handler(app, request, voter)

async def voter_history_handler(app, request, voter):

    # Super torn on wether to make the client
    # do the enrichment or not.
    # It's sub 2ms for a 12-vote delegate without scopes.
    # This spares a delegate's page from two lookups,
    # for something that it may or may not have cached.
    # While it seems like a waste of compute, if it's
    # a problem, we we can always move it to the client later.
    # I don't think it will be, because a delegate's page
    # doesn't get bombarded with requests.
    vh = deepcopy(app.ctx.votes.voter_history[voter])

    out = []
    for v in vh:
        v['proposal_type'] = app.ctx.proposals.proposals[v['proposal_id']].get_proposal_type(app.ctx.proposal_types.proposal_types)
        out.append(v)
    
    return json({'voter_history' : out})


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
@openapi.parameter(
    "delegator",
    str,
    location="query",
    required=False,
    description="Filter the list to show only the delegate to whom the specified delegator address has delegated their votes. If provided, the list will typically contain zero or one delegate or more if partial delegation is used."
)
@measure
async def delegates(request):
    return await delegates_handler(app, request)

# Helper function to get the sort value for a single delegate
def _get_delegate_sort_value(app_ctx, delegate_address: str, sort_by: str):
    if sort_by == 'VP':
        return app_ctx.delegations.delegatee_vp.get(delegate_address, 0)
    elif sort_by == 'MRD':
        event = app_ctx.delegations.delegatee_latest_event.get(delegate_address)
        return int(event.get('block_number', 0)) if event else 0 # TODO: this .get() is a bit of a hack, it should be able to be strict.  I think there is an integrity issue somewhere, that causes a delegate_address to be missing, and at which point break the entire endpoint.
    elif sort_by == 'PR':
        return app_ctx.participation_rate_model.get_rate(delegate_address)
    elif sort_by == 'OLD':
        event = app_ctx.delegations.delegatee_oldest_event.get(delegate_address)
        return int(event.get('block_number', 100000000000000000000000000)) if event else 100000000000000000000000000 # TODO: this .get() is a bit of a hack, it should be able to be strict.  I think there is an integrity issue somewhere, that causes a delegate_address to be missing, and at which point break the entire endpoint.
    elif sort_by == 'DC':
        return app_ctx.delegations.delegatee_cnt.get(delegate_address, 0)
    elif sort_by == 'LVB':
        return int(app_ctx.votes.latest_vote_block.get(delegate_address, 0))
    elif sort_by == 'VPC':
        return app_ctx.delegations.delegate_seven_day_vp_change(delegate_address)

async def delegates_handler(app, request):

    sort_by = request.args.get("sort_by", 'VP')
    
    sort_by_vp  = sort_by == 'VP'  # Voting Power
    sort_by_dc  = sort_by == 'DC'  # Delegator Count
    sort_by_pr  = sort_by == 'PR'  # Partipcipation Rate ( Not supported yet) 
    sort_by_lvb = sort_by == 'LVB' # Last Vote Block
    sort_by_mrd = sort_by == 'MRD' # Most Recent Delegation
    sort_by_old = sort_by == 'OLD' # Oldest Delegation
    sort_by_vpc = sort_by == 'VPC' # 7-day Voting Power Change


    offset = int(request.args.get("offset", DEFAULT_OFFSET))
    page_size = int(request.args.get("page_size", DEFAULT_PAGE_SIZE))

    reverse = request.args.get("reverse", "true").lower() == "true"

    include = request.args.get("include", 'DC,PR').split(",")
    delegator_address_filter = request.args.get("delegator")

    add_delegator_count = 'DC' in include or sort_by_dc
    add_participation_rate = 'PR' in include or sort_by_pr
    add_voting_power = 'VP' in include or sort_by_vp
    add_last_vote_block = 'LVB' in include or sort_by_lvb
    add_most_recent_delegation = 'MRD' in include or sort_by_mrd
    add_oldest_delegation = 'OLD' in include or sort_by_old
    add_seven_day_vp_change = 'VPC' in include or sort_by_vpc

    if sort_by_pr or add_participation_rate:
        # TODO - figure out all the edge cases that could require this,
        # and then call it at the right time, rather than here.
        if ENABLE_DELEGATION:
            app.ctx.participation_rate_model.refresh_if_necessary(app.ctx.proposals, app.ctx.votes, app.ctx.delegations)

    out = []
    if delegator_address_filter:
        delegator_address_filter_lower = delegator_address_filter.lower()
        target_delegatee_addresses = app.ctx.delegations.delegator_delegate[delegator_address_filter_lower]
        
        if target_delegatee_addresses:
            for delegate_addr in target_delegatee_addresses:
                delegate_addr_lower = delegate_addr
                sort_val = _get_delegate_sort_value(app.ctx, delegate_addr_lower, sort_by)
                
                # Apply LVB specific pruning if sorting by LVB
                if sort_by_lvb and sort_val == 0:
                    continue
                out.append((delegate_addr, sort_val))
    # Get the initial list based on sort criteria
    else:
        if sort_by_vp:
            out = list(app.ctx.delegations.delegatee_vp.items())
        elif sort_by_mrd:
            out = [(addr, int(event['block_number'])) 
                   for addr, event in app.ctx.delegations.delegatee_latest_event.items()]
        elif sort_by_pr:
            out = list(app.ctx.participation_rate_model.rates())
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

    voting_power_func = lambda x, y: str(app.ctx.delegations.delegatee_vp[x])
    from_cnt_func = lambda x, y: app.ctx.delegations.delegatee_cnt[x]
    participation_func = lambda x, y: app.ctx.participation_rate_model.get_rate(x)
    last_vote_block_func = lambda x, y: app.ctx.votes.latest_vote_block.get(x, 0)
    most_recent_delegation_func = lambda x, y: app.ctx.delegations.delegatee_latest_event[x].get('block_number', 0) # TODO: this .get() is a bit of a hack, it should be able to be strict.  I think there is an integrity issue somewhere, that causes a delegate_address to be missing, and at which point break the entire endpoint.
    oldest_delegation_func = lambda x, y: app.ctx.delegations.delegatee_oldest_event[x].get('block_number', 100000000000000000000000000) # TODO: this .get() is a bit of a hack, it should be able to be strict.  I think there is an integrity issue somewhere, that causes a delegate_address to be missing, and at which point break the entire endpoint.
    seven_day_vp_change_func = lambda x, y: str(app.ctx.delegations.delegate_seven_day_vp_change(x))

    use_sort_key = lambda x, y: y
    addr_func = lambda x, y: x
    
    transformers = [('addr', addr_func)]

    if add_voting_power:
        transformers.append(('VP',  use_sort_key if sort_by_vp else voting_power_func))
    if add_delegator_count:
        transformers.append(('DC',  use_sort_key if sort_by_dc else from_cnt_func))
    if add_participation_rate:
        transformers.append(('PR',  use_sort_key if sort_by_pr else participation_func))
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

    addr = addr.lower()

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

    participation = app.ctx.participation_rate_model.get_fraction(addr)

    return json({'delegate' : 
                {'addr' : addr,
                'from_cnt' : app.ctx.delegations.delegatee_cnt[addr],
                'from_list' : from_list_with_info,
                'voting_power' : str(app.ctx.delegations.delegatee_vp[addr]),
                'history' : app.ctx.delegations.delegatee_vp_history[addr],
                'participation' : participation}})

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

    vp, history = app.ctx.delegations.delegatee_vp_at_block(addr, block_number, include_history=True)

    return json({'voting_power' : vp,
                 'delegate' : addr,
                 'block_number' : block_number,
                 'history' : history})


#################################################################################################################################################

def detect_any_bytes(obj):
    if isinstance(obj, bytes):
        return True
    if isinstance(obj, list):
        return any(detect_any_bytes(x) for x in obj)
    if isinstance(obj, dict):
        return any(detect_any_bytes(x) for x in obj.values())
    return False

"""

Optimism (in kb)

{
"app.ctx.feed": 142,
"app.ctx.feed.event_history": 0,
"app.ctx.balances": 441705,
"app.ctx.delegations": 2191977,
"app.ctx.proposals": 1079,
"app.ctx.votes": 858982
}
"""

@app.route('/v1/ram/<worker_id>')
@openapi.tag("Diagnostics")
@openapi.summary("Diagnostics")
@measure
async def ram(request, worker_id):

    if worker_id != WORKER_ID:
        return json({'error' : 'worker_id required'})

    def sizeof(obj):
        return int(asizeof.asizeof(obj) / 1024)
    
    return json({'app.ctx.feed' : sizeof(app.ctx.feed),
                 'app.ctx.feed.event_history' : sizeof(app.ctx.feed.event_history),
                 'app.ctx.balances' : sizeof(app.ctx.balances),
                 'app.ctx.delegations' : sizeof(app.ctx.delegations),
                 'app.ctx.proposals' : sizeof(app.ctx.proposals),
                 'app.ctx.votes' : sizeof(app.ctx.votes)
                })


def check_addresses(addr_iter, expecting='lower'):

    cnts = defaultdict(int)

    for addr in addr_iter:
        if addr == addr.lower():
            cnts['lower'] += 1
        elif addr == to_checksum_address(addr):
            cnts['checksum'] += 1
        else:
            cnts['problem'] += 1
    
    not_expecting = 'lower' if expecting == 'checksum' else 'checksum'

    test = ('problem' not in cnts) and (not_expecting not in cnts)

    cnts['pass'] = test

    return cnts

def check_uints_as_strs(uint_iter):

    cnts = defaultdict(int)

    for uint in uint_iter:
        if uint == str(uint):
            cnts['str'] += 1
        elif isinstance(uint, int):
            cnts['int'] += 1
        else:
            cnts['problem'] += 1
    
    test = ('problem' not in cnts) and ('int' not in cnts)

    cnts['pass'] = test
        
    return cnts

@app.route('/v1/integrity')
@openapi.tag("Diagnostics")
@openapi.summary("Integrity")
@measure
async def integrity(request):

    out = {}

    check = check_uints_as_strs
    out['votes.proposal_vote_record'] = check(app.ctx.votes.proposal_vote_record)
    out['votes.proposal_aggregations'] = check(app.ctx.votes.proposal_aggregations)

    check = check_addresses
    out['votes.participated'] = check(app.ctx.votes.participated)
    out['votes.voter_history'] = check(app.ctx.votes.voter_history)
    out['votes.latest_vote_block'] = check(app.ctx.votes.latest_vote_block)

    out['delegations.delegatee_list'] = check(app.ctx.delegations.delegatee_list)
    out['delegations.delegatee_vp'] = check(app.ctx.delegations.delegatee_vp)
    out['delegations.delegatee_vp_history'] = check(app.ctx.delegations.delegatee_vp_history)
    out['delegations.delegatee_cnt'] = check(app.ctx.delegations.delegatee_cnt)
    out['delegations.delegatee_vp'] = check(app.ctx.delegations.delegatee_vp)
    
    out['pass'] = all([v['pass'] for v in out.values()])
    
    return json(out)

@app.route('/v1/diagnostics/<safe_mode>')
@openapi.tag("Diagnostics")
@openapi.summary("Diagnostics")
@measure
async def diagnostics(request, safe_mode: bool):

    safe_mode = safe_mode == 'true'

    if safe_mode:
        events = [(str(e), detect_any_bytes(e)) for e in app.ctx.feed.event_history if 'timestamp' not in e]
    else:
        events = app.ctx.feed.event_history

    return json({
                 'event_history' : events,
                 'boot_time' : BOOT_TIME,
                 'worker_id' : WORKER_ID,
                 'git_commit_sha' : GIT_COMMIT_SHA,
                 })

@app.route('/v1/progress')
@openapi.tag("Diagnostics")
@openapi.summary("Current Block Header + Num of Events Processed")
@measure
async def progress(request):

    return json({
                 'block' : app.ctx.feed.block,
                 'realtime_counts' : app.ctx.feed.realtime_signal_counts,
                 'archive_counts' : app.ctx.feed.archive_signal_counts,
                 'total_counts' : app.ctx.feed.total_signal_counts,
                 'boot_time' : BOOT_TIME,
                 'worker_id' : WORKER_ID,
                 'git_commit_sha' : GIT_COMMIT_SHA,
                 })

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
    if ENABLE_DELEGATION:
        return json({'voting_power' : str(app.ctx.delegations.voting_power)})
    else:
        return json({'voting_power' : '10000000000000000000'})



#################################################################################
#
# ⏫ 🌎 BOOT SEQUENCE
#
################################################################################

NUM_ARCHIVE_CLIENTS = 2
NUM_REALTIME_CLIENTS = 2
NUM_POLLING_CLIENTS = 1

@app.before_server_start(priority=0)
async def bootstrap_data_feeds(app, loop):

    #################################################################################
    # ⚡️ 📀 Client Setup

    clients = []

    csvc = CSVClient(DAO_NODE_DATA_PATH)
    if csvc.is_valid():
        clients.append(csvc)

    rpcc = JsonRpcHistHttpClient(ARCHIVE_NODE_HTTP_URL)
    if rpcc.is_valid():
       clients.append(rpcc)

    for i in range(NUM_REALTIME_CLIENTS):
        jwsc = JsonRpcRtWsClient(REALTIME_NODE_WS_URL, f"RTWS{i}")
        if jwsc.is_valid():
           clients.append(jwsc)

    for i in range(NUM_POLLING_CLIENTS):
        jwhc = JsonRpcRtHttpClient(ARCHIVE_NODE_HTTP_URL, f"POLL{i}")
        if jwhc.is_valid():
           clients.append(jwhc)

    # Create a sequence of clients to pull events from.  Each with their own standards for comms, drivers, API, etc. 
    dcqs = ClientSequencer(clients) 

    #################################################################################
    # 👀 🕹️ ABI Setup - Load the ABIs relevant for the DAO.  

    # Get a full picture of all available contracts relevant for this DAO.
    chain_id = int(deployment['chain_id'])
    AGORA_GOV = public_config['governor_spec']['name'] == 'agora'

    abi_list = []
    logr.info(f"deployment={deployment}")

    if 'token' in deployment:
        token_addr = deployment['token']['address'].lower()
        logr.info(f"Using {token_addr=}")
        token_abi = ABI.from_internet('token', token_addr, chain_id=chain_id, implementation=True)
        abi_list.append(token_abi)

    
    if 'gov' in deployment:
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
    dcqs.set_abis(abis)

    #################################################################################
    # 🎪 🧠 Instantiate "Data Products".  These are the singletons that store data 
    #      in RAM, that need to be maintained for every event.

    if ENABLE_BALANCES:
        balances = Balances(token_spec=public_config['token_spec'])
        app.ctx.register(f'{chain_id}.{token_addr}.{TRANSFER}', balances)

    if 'token' in deployment:

        if ENABLE_DELEGATION:
            delegations = Delegations()
            app.ctx.register(f'{chain_id}.blocks', delegations)
            app.ctx.register(f'{chain_id}.{token_addr}.{DELEGATE_VOTES_CHANGE}', delegations)

            if 'IVotesPartialDelegation' in public_config['token_spec'].get('interfaces', []):
                app.ctx.register(f'{chain_id}.{token_addr}.{DELEGATE_CHANGED_2}', delegations)
            else:
                app.ctx.register(f'{chain_id}.{token_addr}.{DELEGATE_CHANGED_1}', delegations)

    if 'ptc' in deployment:
        proposal_types = ProposalTypes()

        for prop_type_set_signature in [PROP_TYPE_SET_1, PROP_TYPE_SET_2, PROP_TYPE_SET_3, PROP_TYPE_SET_4]:
            if abis.get_by_signature(prop_type_set_signature):
                app.ctx.register(f'{chain_id}.{ptc_addr}.{prop_type_set_signature}', proposal_types)
        
        if AGORA_GOV and public_config['governor_spec']['version'] >= 1.1:
            app.ctx.register(f'{chain_id}.{ptc_addr}.{SCOPE_CREATED}' , proposal_types)
            app.ctx.register(f'{chain_id}.{ptc_addr}.{SCOPE_DISABLED}', proposal_types)
            app.ctx.register(f'{chain_id}.{ptc_addr}.{SCOPE_DELETED}' , proposal_types)

    proposals = Proposals(governor_spec=public_config['governor_spec'])
    votes = Votes(governor_spec=public_config['governor_spec'])

    gov_spec_name = public_config['governor_spec']['name']
    if gov_spec_name in ('compound', 'ENSGovernor'):
        PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_1]
    elif gov_spec_name == 'agora' and public_config['governor_spec']['version'] == 0.1:
        PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_1, PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4]
    elif gov_spec_name == 'agora':
        PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_2, PROPOSAL_CREATED_4]
    elif gov_spec_name == 'none':
        PROPOSAL_CREATED_EVENTS = []
    else:
        raise Exception(f"Govenor Unsupported: {gov_spec_name}")

    if PROPOSAL_CREATED_EVENTS:
        PROPOSAL_LIFECYCLE_EVENTS = PROPOSAL_CREATED_EVENTS + [PROPOSAL_CANCELED, PROPOSAL_QUEUED, PROPOSAL_EXECUTED]
        for PROPOSAL_EVENT in PROPOSAL_LIFECYCLE_EVENTS:
            app.ctx.register(f'{chain_id}.{gov_addr}.' + PROPOSAL_EVENT, proposals)

        VOTE_EVENTS = [VOTE_CAST_1]    
        if not (public_config['governor_spec']['name'] in ('compound', 'ENSGovernor')):
            VOTE_EVENTS.append(VOTE_CAST_WITH_PARAMS_1)

        for VOTE_EVENT in VOTE_EVENTS:
            app.ctx.register(f'{chain_id}.{gov_addr}.' + VOTE_EVENT, votes)
        
    pr = ParticipationRateModel()
    app.ctx.register_model(pr)

    # This is so certain endpoints can access empty data-products
    # without a bunch of gymnastics.
    for data_product in [proposals, votes]:
        if not hasattr(app.ctx, data_product.name):
            setattr(app.ctx, data_product.name, data_product)

    app.add_task(read_archive(app, dcqs))

async def index_proposals(app):

    start = time.time()
    app.ctx.proposals.restate_recently_completed_and_counted_proposals()
    
    if ENABLE_DELEGATION:
        app.ctx.participation_rate_model.refresh_if_necessary(app.ctx.proposals, app.ctx.votes, app.ctx.delegations)

    logr.info(f"Indexing participation rates [{time.time() - start:.2f}s]")

async def read_archive(app, dcqs):
    
    app.ctx.feed.set_client_sequencer(dcqs)

    for event, signal, new_signal in app.ctx.feed.read_archive():

        if new_signal:
            app.ctx.set_signal_context(signal)

        app.ctx.dispatch_from_archive(event)

@app.after_server_start
async def subscribe_feeds(app):

    for i in range(NUM_REALTIME_CLIENTS):
        logr.info(f"Realtime client {1 + NUM_ARCHIVE_CLIENTS + i} started")
        app.add_task(read_realtime(app, 1 + NUM_ARCHIVE_CLIENTS + i))

    app.add_task(index_proposals(app))

    for i in range(NUM_POLLING_CLIENTS):
        logr.info(f"Polling client {1 + NUM_ARCHIVE_CLIENTS + NUM_REALTIME_CLIENTS + i} started")
        app.add_task(read_polling(app, 1 + NUM_ARCHIVE_CLIENTS + NUM_REALTIME_CLIENTS + i))

async def read_realtime(app, rt_client_num):
    
    async for event in app.ctx.feed.realtime_async_read(rt_client_num):
        await app.ctx.dispatch_from_realtime(event)

async def read_polling(app, polling_client_num):
    """
    This is a polling client.  It will poll the chain for events, and dispatch them to the data products.

    It's a backup to the web-sockets, in the sense that if the websockets disconnect or miss an event
    during a resubscription, then, this is the fallback.

    If everything is working, none of the events caught in polling, would ever be needed. 

    Note that the "wait_cycle" needs to match the look back in blocks configured in the JsonRpcRtHttpClient.read().

    Note that this blocks for the duration of the wait_cycle, so it should be as short as possible, TODO - make it fully async.
    """

    wait_cycle = 120
    await asyncio.sleep(wait_cycle)
    while True:
        start_time = time.perf_counter()
        cnt = 0
        async for event in app.ctx.feed.realtime_async_read(polling_client_num):
            await app.ctx.dispatch_from_realtime(event)
            cnt += 1
        logr.info(f"Polling client {polling_client_num} [{time.perf_counter() - start_time:.2f}s] [{cnt} events]")
        await asyncio.sleep(wait_cycle)

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

