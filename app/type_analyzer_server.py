"""
Type Analyzer Server - Analyzes event field types across different clients.

This server boots similarly to the main DAO Node server, but instead of
processing events through data products, it analyzes the types of each field
to help identify and unify type differences between clients.

Usage:
    python -m app.type_analyzer_server

Environment Variables:
    AGORA_CONFIG_FILE: Path to the tenant config YAML
    DAO_NODE_DATA_PATH: Path to CSV data directory
    DAO_NODE_ARCHIVE_NODE_HTTP: HTTP URL for archive node
    DAO_NODE_REALTIME_NODE_WS: WebSocket URL for realtime node
    TYPE_ANALYZER_PERSIST_TO_DISK: Set to 'true' to persist to disk
    TYPE_ANALYZER_OUTPUT_DIR: Directory for JSON output files
"""

from dotenv import load_dotenv
load_dotenv()

import os
import yaml
import asyncio
from pathlib import Path
from collections import defaultdict

from sanic import Sanic
from sanic.response import json, html
from sanic.log import logger as logr
from sanic.worker.manager import WorkerManager

from abifsm import ABI, ABISet

from .clients_csv import CSVClient
from .clients_httpjson import JsonRpcHistHttpClient, JsonRpcRtHttpClient
from .clients_wsjson import JsonRpcRtWsClient

from .type_analysis_store import TypeAnalysisStore, get_store
from .signatures import *


# Set ABI URL for abifsm library
os.environ['ABI_URL'] = 'https://storage.googleapis.com/agora-abis/v2'

# Load configuration
CONTRACT_DEPLOYMENT = os.getenv('CONTRACT_DEPLOYMENT', 'main')

DAO_NODE_DATA_PATH = Path(os.getenv('DAO_NODE_DATA_PATH', './data'))

def secret_text(t, n):
    if len(t) > ((2 * n) + 3):
        return t[:n] + "..." + t[-1 * n:]
    else:
        return t[:n] + "***..."

# Archive node HTTP URL
DAO_NODE_ARCHIVE_NODE_HTTP = os.getenv('DAO_NODE_ARCHIVE_NODE_HTTP', None)
ARCHIVE_NODE_HTTP_URL = None
if DAO_NODE_ARCHIVE_NODE_HTTP:
    ARCHIVE_NODE_HTTP_URL = DAO_NODE_ARCHIVE_NODE_HTTP
    if 'alchemy.com' in DAO_NODE_ARCHIVE_NODE_HTTP:
        ARCHIVE_NODE_HTTP_URL = ARCHIVE_NODE_HTTP_URL + os.getenv('ALCHEMY_API_KEY', '')
        logr.info(f"Using alchemy for Archive: {secret_text(ARCHIVE_NODE_HTTP_URL, 6)}")
    if 'quiknode.pro' in DAO_NODE_ARCHIVE_NODE_HTTP:
        ARCHIVE_NODE_HTTP_URL = ARCHIVE_NODE_HTTP_URL + os.getenv('QUICKNODE_API_KEY', '')
        logr.info(f"Using quiknode.pro for Archive: {secret_text(ARCHIVE_NODE_HTTP_URL, 6)}")

# Realtime node WebSocket URL
DAO_NODE_REALTIME_NODE_WS = os.getenv('DAO_NODE_REALTIME_NODE_WS', None)
REALTIME_NODE_WS_URL = None
if DAO_NODE_REALTIME_NODE_WS:
    REALTIME_NODE_WS_URL = DAO_NODE_REALTIME_NODE_WS
    if 'alchemy.com' in DAO_NODE_REALTIME_NODE_WS:
        REALTIME_NODE_WS_URL = REALTIME_NODE_WS_URL + os.getenv('ALCHEMY_API_KEY', '')
        logr.info(f"Using alchemy for Web Socket: {secret_text(REALTIME_NODE_WS_URL, 6)}")
    if 'quiknode.pro' in DAO_NODE_REALTIME_NODE_WS:
        REALTIME_NODE_WS_URL = REALTIME_NODE_WS_URL + os.getenv('QUICKNODE_API_KEY', '')
        logr.info(f"Using quiknode.pro for Web Socket: {secret_text(REALTIME_NODE_WS_URL, 6)}")

# Load tenant configuration
try:
    AGORA_CONFIG_FILE = Path(os.getenv('AGORA_CONFIG_FILE', '/app/config.yaml'))
    with open(AGORA_CONFIG_FILE, 'r') as f:
        config = yaml.safe_load(f)
    
    logr.info(f"Loaded config: {config}")
    public_config = {k: config.get(k) for k in ['governor_spec', 'token_spec', 'module_spec']}
    deployment = config['deployments'][CONTRACT_DEPLOYMENT]
    del config['deployments']
except Exception as e:
    logr.error(f"Failed to load config: {e}")
    config = {
        'friendly_short_name': 'Unknown',
        'deployments': {},
        'features': {}
    }
    public_config = {}
    deployment = {'chain_id': 1, 'token': {'address': '0x0000000000000000000000000000000000000000'}}

# Increase worker timeout for long boot sequences
WorkerManager.THRESHOLD = 600 * 45  # 45 minutes


class TypeAnalyzerClientSequencer:
    """Manages multiple clients for type analysis."""
    
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
    
    def plan(self, *signal_meta):
        for client in self.clients:
            try:
                client.plan(*signal_meta)
            except Exception as e:
                logr.info(f"Failed to plan [{signal_meta}] for {client}: {e}")


class TypeAnalyzerContext:
    """Application context for type analysis."""
    
    def __init__(self):
        self.store = get_store()
        self.subscription_meta = []
        self.block = 0
    
    def plan_block(self, chain_id):
        self.subscription_meta.append(('block', (chain_id,)))
    
    def plan_event(self, chain_id, address, signature):
        self.subscription_meta.append(('event', (chain_id, address, signature)))


# Create Sanic app
app = Sanic('TypeAnalyzerServer', ctx=TypeAnalyzerContext())


######################################################################
#
# HTTP Endpoints for Type Analysis
#
######################################################################

@app.get('/')
async def index(request):
    """Landing page with links to type analysis endpoints."""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Type Analyzer Server</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; 
                   max-width: 800px; margin: 50px auto; padding: 20px; }
            h1 { color: #333; }
            ul { list-style: none; padding: 0; }
            li { margin: 10px 0; }
            a { color: #0066cc; text-decoration: none; }
            a:hover { text-decoration: underline; }
            .endpoint { font-family: monospace; background: #f5f5f5; padding: 2px 6px; border-radius: 3px; }
        </style>
    </head>
    <body>
        <h1>Type Analyzer Server</h1>
        <p>This server analyzes event field types across different Ethereum clients.</p>
        
        <h2>Endpoints</h2>
        <ul>
            <li><a href="/types"><span class="endpoint">GET /types</span></a> - Full summary of all detected types</li>
            <li><a href="/types/Transfer(address,address,uint256)"><span class="endpoint">GET /types/&lt;signature&gt;</span></a> - Side-by-side comparison for one event type</li>
            <li><a href="/differences"><span class="endpoint">GET /differences</span></a> - Only show mismatches between clients</li>
            <li><a href="/hashes"><span class="endpoint">GET /hashes</span></a> - Schema hashes for quick diff detection</li>
            <li><a href="/health"><span class="endpoint">GET /health</span></a> - Health check</li>
        </ul>
        
        <h2>Status</h2>
        <p>Check <a href="/types">/types</a> to see recorded data.</p>
    </body>
    </html>
    """
    return html(html_content)


@app.get('/types')
async def get_all_types(request):
    """Get full summary of all detected types per client."""
    return json(app.ctx.store.get_full_summary())


@app.get('/types/<signature:path>')
async def get_types_for_signature(request, signature: str):
    """Get side-by-side comparison for one event type across all clients."""
    # URL decode the signature
    signature = signature.replace('_', '(').replace(')', ')').replace(',', ',')
    
    # Find the chain_id for this signature from the store
    chain_ids = set()
    for client_data in app.ctx.store.entries.values():
        for chain_id, chain_data in client_data.items():
            if signature in chain_data:
                chain_ids.add(chain_id)
    
    if not chain_ids:
        return json({'error': f'Signature not found: {signature}'}, status=404)
    
    # Return comparisons for all chains where this signature exists
    comparisons = []
    for chain_id in chain_ids:
        comparison = app.ctx.store.get_comparison(chain_id, signature)
        comparisons.append(comparison)
    
    return json({'signature': signature, 'comparisons': comparisons})


@app.get('/differences')
async def get_differences(request):
    """Get only the comparisons that have type mismatches."""
    differences = app.ctx.store.get_differences_only()
    return json({
        'count': len(differences),
        'differences': differences
    })


@app.get('/hashes')
async def get_hashes(request):
    """Get schema hashes for quick diff detection."""
    return json(app.ctx.store.get_hashes_summary())


@app.get('/health')
async def health_check(request):
    """Health check endpoint."""
    summary = app.ctx.store.get_full_summary()
    return json({
        'status': 'healthy',
        'clients': summary['clients'],
        'total_entries': summary['total_entries'],
        'differences_count': summary['differences_count']
    })


######################################################################
#
# Boot Sequence - Mirrors server.py
#
######################################################################

NUM_ARCHIVE_CLIENTS = int(os.getenv('NUM_ARCHIVE_CLIENTS', 2))
NUM_REALTIME_CLIENTS = int(os.getenv('NUM_REALTIME_CLIENTS', 2))
NUM_POLLING_CLIENTS = int(os.getenv('NUM_POLLING_CLIENTS', 1))


@app.before_server_start(priority=0)
async def bootstrap_type_analyzer(app, loop):
    """Set up clients and subscriptions for type analysis."""
    
    logr.info("Starting Type Analyzer bootstrap...")
    
    #################################################################################
    # Client Setup
    
    clients = []
    
    csvc = CSVClient(DAO_NODE_DATA_PATH)
    if csvc.is_valid():
        clients.append(csvc)
        logr.info("CSV client added")
    
    if ARCHIVE_NODE_HTTP_URL:
        rpcc = JsonRpcHistHttpClient(ARCHIVE_NODE_HTTP_URL)
        if rpcc.is_valid():
            clients.append(rpcc)
            logr.info("HTTP archive client added")
    
    if REALTIME_NODE_WS_URL:
        for i in range(NUM_REALTIME_CLIENTS):
            jwsc = JsonRpcRtWsClient(REALTIME_NODE_WS_URL, f"RTWS{i}")
            if jwsc.is_valid():
                clients.append(jwsc)
                logr.info(f"WebSocket realtime client {i} added")
    
    if ARCHIVE_NODE_HTTP_URL:
        for i in range(NUM_POLLING_CLIENTS):
            jwhc = JsonRpcRtHttpClient(ARCHIVE_NODE_HTTP_URL, f"POLL{i}")
            if jwhc.is_valid():
                clients.append(jwhc)
                logr.info(f"HTTP polling client {i} added")
    
    dcqs = TypeAnalyzerClientSequencer(clients)
    app.ctx.client_sequencer = dcqs
    
    #################################################################################
    # ABI Setup
    
    chain_id = int(deployment['chain_id'])
    AGORA_GOV = public_config.get('governor_spec', {}).get('name') == 'agora'
    
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
    
    if 'voting_module' in deployment:
        voting_module_addr = deployment['voting_module']['address'].lower()
        logr.info(f"Using {voting_module_addr=}")
        voting_module_abi = ABI.from_internet('voting_module', voting_module_addr, chain_id=chain_id, implementation=True)
        abi_list.append(voting_module_abi)
    
    abis = ABISet('daonode', abi_list)
    dcqs.set_abis(abis)
    app.ctx.abis = abis
    
    #################################################################################
    # Plan Subscriptions - Same as main server
    
    if 'token' in deployment:
        token_addr = deployment['token']['address'].lower()
        
        # Transfer events
        app.ctx.plan_event(chain_id, token_addr, TRANSFER)
        
        # Delegation events
        app.ctx.plan_event(chain_id, token_addr, DELEGATE_VOTES_CHANGE)
        
        if 'IVotesPartialDelegation' in public_config.get('token_spec', {}).get('interfaces', []):
            app.ctx.plan_event(chain_id, token_addr, DELEGATE_CHANGED_2)
        else:
            app.ctx.plan_event(chain_id, token_addr, DELEGATE_CHANGED_1)
        
        # Block headers for delegation tracking
        app.ctx.plan_block(chain_id)
    
    if 'ptc' in deployment:
        ptc_addr = deployment['ptc']['address'].lower()
        
        for prop_type_set_signature in [PROP_TYPE_SET_1, PROP_TYPE_SET_2, PROP_TYPE_SET_3, PROP_TYPE_SET_4]:
            if abis.get_by_signature(prop_type_set_signature):
                app.ctx.plan_event(chain_id, ptc_addr, prop_type_set_signature)
        
        if AGORA_GOV and public_config['governor_spec'].get('version', 0) >= 1.1 and public_config['governor_spec'].get('version', 0) < 2.0:
            app.ctx.plan_event(chain_id, ptc_addr, SCOPE_CREATED)
            app.ctx.plan_event(chain_id, ptc_addr, SCOPE_DISABLED)
            app.ctx.plan_event(chain_id, ptc_addr, SCOPE_DELETED)
        elif AGORA_GOV and public_config['governor_spec'].get('version', 0) >= 2.0:
            app.ctx.plan_event(chain_id, ptc_addr, SCOPE_CREATED)
            app.ctx.plan_event(chain_id, ptc_addr, SCOPE_DISABLED_2)
            app.ctx.plan_event(chain_id, ptc_addr, SCOPE_DELETED_2)
    
    if 'gov' in deployment:
        gov_addr = deployment['gov']['address'].lower()
        gov_spec_name = public_config.get('governor_spec', {}).get('name', '')
        
        if gov_spec_name in ('compound', 'ENSGovernor'):
            PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_1]
        elif gov_spec_name == 'agora' and public_config['governor_spec'].get('version') == 0.1:
            PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_1, PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4]
        elif gov_spec_name == 'agora' and public_config['governor_spec'].get('version') == 2.0:
            PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_1, PROPOSAL_CREATED_MODULE]
        elif gov_spec_name == 'agora':
            PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_2, PROPOSAL_CREATED_4]
        elif gov_spec_name == 'none':
            PROPOSAL_CREATED_EVENTS = []
        else:
            PROPOSAL_CREATED_EVENTS = [PROPOSAL_CREATED_1, PROPOSAL_CREATED_2]
        
        if PROPOSAL_CREATED_EVENTS:
            PROPOSAL_LIFECYCLE_EVENTS = PROPOSAL_CREATED_EVENTS + [PROPOSAL_CANCELED, PROPOSAL_QUEUED, PROPOSAL_EXECUTED]
            for PROPOSAL_EVENT in PROPOSAL_LIFECYCLE_EVENTS:
                if PROPOSAL_EVENT == PROPOSAL_CREATED_MODULE and 'voting_module' in deployment:
                    app.ctx.plan_event(chain_id, deployment['voting_module']['address'].lower(), PROPOSAL_EVENT)
                else:
                    app.ctx.plan_event(chain_id, gov_addr, PROPOSAL_EVENT)
            
            # Vote events
            VOTE_EVENTS = [VOTE_CAST_1]
            if gov_spec_name not in ('compound', 'ENSGovernor'):
                VOTE_EVENTS.append(VOTE_CAST_WITH_PARAMS_1)
            
            for VOTE_EVENT in VOTE_EVENTS:
                app.ctx.plan_event(chain_id, gov_addr, VOTE_EVENT)
    
    # Apply subscription plans to client sequencer
    for signal_meta in app.ctx.subscription_meta:
        dcqs.plan(*signal_meta)
    
    logr.info(f"Planned {len(app.ctx.subscription_meta)} subscriptions")
    
    # Start archive reading task
    app.add_task(read_archive(app, dcqs))


async def read_archive(app, dcqs):
    """Read from archive clients and analyze types."""
    
    for i, client in dcqs:
        if client.timeliness == 'archive':
            client_name = type(client).__name__
            logr.info(f"Reading from archive client #{i}: {client_name}")
            
            app.ctx.block = max(app.ctx.block, client.get_fallback_block())
            
            reader = client.read(after=app.ctx.block)
            event_count = 0
            schema_changes = 0
            
            for event, signal, new_signal in reader:
                event_count += 1
                
                # Parse signal: chain_id.address.signature or chain_id.blocks
                parts = signal.split('.')
                if 'blocks' in signal:
                    chain_id = int(parts[0])
                    # Record block events too
                    changed = app.ctx.store.record_event(
                        client_name=client_name,
                        chain_id=chain_id,
                        address='blocks',
                        signature='block',
                        event=event
                    )
                else:
                    chain_id = int(parts[0])
                    address = parts[1]
                    signature = parts[2]
                    
                    changed = app.ctx.store.record_event(
                        client_name=client_name,
                        chain_id=chain_id,
                        address=address,
                        signature=signature,
                        event=event
                    )
                
                if changed:
                    schema_changes += 1
                
                if 'blocks' not in signal:
                    app.ctx.block = max(app.ctx.block, int(event['block_number']))
                
                if event_count % 10000 == 0:
                    logr.info(f"  Processed {event_count} events from {client_name}...")
            
            logr.info(f"Finished {client_name}: {event_count} events, {schema_changes} schema changes")
            app.ctx.block = app.ctx.block + 1
    
    # Print summary after archive reading
    app.ctx.store.print_console_summary()
    
    # Save to disk if enabled
    app.ctx.store.save_comparison_summary()


@app.after_server_start
async def subscribe_realtime(app):
    """Subscribe to realtime feeds after archive is loaded."""
    
    for i in range(NUM_REALTIME_CLIENTS):
        client_idx = 1 + NUM_ARCHIVE_CLIENTS + i
        logr.info(f"Starting realtime client {client_idx}")
        app.add_task(read_realtime(app, client_idx))
    
    for i in range(NUM_POLLING_CLIENTS):
        client_idx = 1 + NUM_ARCHIVE_CLIENTS + NUM_REALTIME_CLIENTS + i
        logr.info(f"Starting polling client {client_idx}")
        app.add_task(read_polling(app, client_idx))


async def read_realtime(app, rt_client_num):
    """Read from a realtime WebSocket client."""
    
    dcqs = app.ctx.client_sequencer
    
    for i, client in dcqs:
        if client.timeliness in ('realtime', 'polling') and i == rt_client_num:
            client_name = type(client).__name__
            logr.info(f"Realtime reading from client #{i}: {client_name}")
            
            async for event in client.read():
                signal = event.get('signal', '')
                if signal:
                    del event['signal']
                    
                    parts = signal.split('.')
                    if 'blocks' in signal:
                        chain_id = int(parts[0])
                        app.ctx.store.record_event(
                            client_name=client_name,
                            chain_id=chain_id,
                            address='blocks',
                            signature='block',
                            event=event
                        )
                    else:
                        chain_id = int(parts[0])
                        address = parts[1]
                        signature = parts[2]
                        
                        app.ctx.store.record_event(
                            client_name=client_name,
                            chain_id=chain_id,
                            address=address,
                            signature=signature,
                            event=event
                        )


async def read_polling(app, polling_client_num):
    """Read from a polling HTTP client periodically."""
    
    wait_cycle = int(os.getenv('POLLING_WAIT_CYCLE', 120))
    await asyncio.sleep(wait_cycle)
    
    dcqs = app.ctx.client_sequencer
    
    while True:
        for i, client in dcqs:
            if client.timeliness == 'polling' and i == polling_client_num:
                client_name = type(client).__name__
                
                async for event in client.read():
                    signal = event.get('signal', '')
                    if signal:
                        del event['signal']
                        
                        parts = signal.split('.')
                        if 'blocks' not in signal:
                            chain_id = int(parts[0])
                            address = parts[1]
                            signature = parts[2]
                            
                            app.ctx.store.record_event(
                                client_name=client_name,
                                chain_id=chain_id,
                                address=address,
                                signature=signature,
                                event=event
                            )
        
        # Periodically print summary and save
        app.ctx.store.print_console_summary()
        app.ctx.store.save_comparison_summary()
        
        await asyncio.sleep(wait_cycle)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8005, dev=True, debug=True)

