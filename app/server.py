
from sanic import Sanic
from sanic.response import text, html
import multiprocessing
import csv, time, pdb, os
import datetime as dt
from collections import defaultdict
from abifsm import ABI, ABISet
import asyncio
import psycopg2 
import psycopg2.extras
from web3 import AsyncWeb3
from web3.providers.persistent import (
    AsyncIPCProvider,
    WebSocketProvider,
)

from sanic.worker.manager import WorkerManager

WorkerManager.THRESHOLD = 600 * 10 # 2 minutes

os.environ['ABI_URL'] = 'https://storage.googleapis.com/agora-abis/checked/'

from auth.auth import protected
from auth.login import login
# from configs.config import get_config
from google.cloud import storage

DEBUG = False

from web3 import Web3

from abc import ABC, abstractmethod

class DataProduct(ABC):

    @abstractmethod
    def handle(self, event):
        pass

    @property
    def name(self):
        return self.__class__.__name__.lower()
    

class Balances(DataProduct):

    def __init__(self):
        self.balances = defaultdict(int)

    def handle(self, event):
        self.balances[event['from']] -= event['value']
        self.balances[event['to']] += event['value']
    
    def balance_of(self, address):
        return self.balances[address]

    def top(self, k):

        values = list(self.balances.items())
        values.sort(key=lambda x:x[-1])
        return [v[0] for v in values[-1 * int(k):]]

class TransferCounts(DataProduct):

    def __init__(self):
        self.counts = defaultdict(int)

    def handle(self, event):
        self.counts[event['to']] += 1
    
    def count(self, address):
        return self.counts[address]


class GCSClient:
    timeliness = 'archive'

    def __init__(self, bucket):
        self.bucket = bucket
    
    def read(self, chain_id, address, signature, abi_frag, after):
        pass

class CSVClient:
    timeliness = 'archive'

    def __init__(self, path):
        self.path = path
    
    def read(self, chain_id, address, signature, abis, after=0):

        abi_frag = abis.get_by_signature(signature)

        fname = self.path + f'/{chain_id}/{address}/{signature}.csv'

        int_fields = [o['name'] for o in abi_frag.inputs if 'int' in o['type']]

        cnt = 0

        if after == 0:
            
            reader = csv.DictReader(open(fname))

            for row in reader:

                row['block_number'] = int(row['block_number'])
                row['log_index'] = int(row['log_index'])
                row['transaction_index'] = int(row['transaction_index'])

                for int_field in int_fields:
                    row[int_field] = int(row[int_field])

                yield row


                cnt += 1
                
                if DEBUG and (cnt == 100000):
                    break

def test_csv_client():
    csvc = CSVClient('/Users/jm/code/dao_node_disk')

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

class JsonRpcHistClient:
    timeliness = 'archive'

    def __init__(self, url):
        self.url = url

    def get_paginated_logs(self, w3, contract_address, event_signature_hash, start_block, end_block, step, abi):

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
            logs = w3.eth.get_logs(event_filter)

            EVENT_NAME = abi['name']            
            contract_events = w3.eth.contract(abi=[abi]).events
            processor = getattr(contract_events, EVENT_NAME)().process_log

            all_logs.extend(map(processor, logs)) 

            print(f"Fetched logs from block {from_block} to {to_block}. Total logs: {len(all_logs)}")

            if (len(all_logs) > 4000) and DEBUG:
                break
            
        return all_logs


    def read(self, chain_id, address, signature, abis, after):

        w3 = Web3(Web3.HTTPProvider(self.url))

        if w3.is_connected():
            print("Connected to Ethereum")
        else:
            raise Exception(f"Could not connect to {self.url}")

        event = abis.get_by_signature(signature)
        
        abi = event.literal

        # TODO make sure inclusivivity is handled properly.    
        start_block = after
        end_block = w3.eth.block_number
        step = 2000

        # Fetch logs with pagination
        logs = self.get_paginated_logs(w3, address, event.topic, start_block, end_block, step, abi)

        # Parse and print logs
        for log in logs:

            out = {}
            
            out['block_number'] = log['blockNumber']
            out['transaction_index'] = log['transactionIndex']
            out['log_index'] = log['logIndex']

            args = log['args']
            
            out.update(**args)
            
            yield out
            

class JsonRpcWsClient:
    timeliness = 'realtime'

    def __init__(self, url):
        self.url = url

    async def read(self, chain_id, address, signature, abis, after):

        event = abis.get_by_signature(signature)
        
        abi = event.literal

        # Connect to Ethereum node (via Infura or your own node)
        async with AsyncWeb3(WebSocketProvider(self.url)) as w3:
            

            EVENT_NAME = abi['name']            
            contract_events = w3.eth.contract(abi=[abi]).events
            processor = getattr(contract_events, EVENT_NAME)().process_log

            event_filter = {
                "address": address,
                "topics": ["0x" + event.topic]  # Filter by event signature
            }

            subscription_id = await w3.eth.subscribe("logs", event_filter)
            async for response in w3.socket.process_subscriptions():

                decoded_response = processor(response['result'])

                out = {}
                out['block_number'] = decoded_response['blockNumber']
                out['log_index'] = decoded_response['logIndex']
                out['transaction_index'] = decoded_response['transactionIndex']
                out.update(**decoded_response['args'])

                yield out

class ClientSequencer:
    def __init__(self, clients):
        self.clients = clients
        self.num = len(clients)
        self.pos = 0
    
    def __iter__(self):
        return self

    def __next__(self): 

        self.pos += 1
        if self.pos <= self.num:
            return self.clients[self.pos - 1]

        self.pos = 0

        raise StopIteration

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

        for client in self.cs:

            if client.timeliness == 'realtime':

                reader = client.read(self.chain_id, self.address, self.signature, self.abis, after=self.block)

                async for event in reader:

                    self.block = max(self.block, event['block_number'])

                    yield event

    async def boot(self, app):
        
        cnt = 0

        start = dt.datetime.now()

        print("Booting...")

        data_product_dispatchers = app.ctx.dps[f"{self.chain_id}.{self.address}.{self.signature}"]

        for event in self.archive_read():
            cnt += 1
            for data_product_dispatcher in data_product_dispatchers:
                data_product_dispatcher.handle(event)

            if (cnt % 1_000_000) == 0:
                print(f"loaded {cnt} so far {( dt.datetime.now() - start).total_seconds()}")
        
        end = dt.datetime.now()

        print(f"Done booting {cnt} records in {(end - start).total_seconds()} seconds.")
        
        await asyncio.sleep(.01)

        return 
    
    async def run(self, app):
        async for event in self.realtime_async_read():
            await app.dispatch(f"{self.chain_id}.{self.address}.{self.signature}", context=event)


class DataProductContext:
    def __init__(self):

        self.dps = defaultdict(list)
        self.event_feeds = []
    
    def handle_dispatch(self, chain_id_contract_signature, context):
        for data_product in self.dps[chain_id_contract_signature]:
            data_product.handle(context)

    def register(self, signature, data_product):
        self.dps[signature].append(data_product)

        setattr(self, data_product.name, data_product)


    def add_event_feed(self, event_feed):
        self.event_feeds.append(event_feed)
    
app = Sanic('DaoNode', ctx=DataProductContext())


@app.signal("<chain_id_contract_signature>")
async def log_event_signal_handler(chain_id_contract_signature, **context):
    app.ctx.handle_dispatch(chain_id_contract_signature, context)


@app.route('/balance/<addr>')
async def balances(request, addr):
	return text(str(app.ctx.balances.balance_of(addr)))

@app.route('/top/<k>')
async def top(request, k):
	return text(str(app.ctx.balances.top(k)))


@app.before_server_start(priority=0)
async def bootstrap_event_feeds(app, loop):

    gcsc = GCSClient('gs://eth-event-feed')
    csvc = CSVClient('/Users/jm/code/dao_node_disk')
    sqlc = PostGresClient('postgres://postgres:...:5432/prod')
    rpcc = JsonRpcHistClient('http://localhost:8545')
    jwsc = JsonRpcWsClient('ws://localhost:8545')


    ##########################
    # Create a sequence of clients to pull events from.  Each with their own standards for comms, drivers, API, etc. 
    dcqs = ClientSequencer([csvc, sqlc, rpcc, jwsc]) 

    ##########################
    # Get a full picture of all available contracts relevant for this app.

    abi = ABI.from_internet('token', '0x4200000000000000000000000000000000000042')
    abis = ABISet('optimism', [abi])

    ##########################
    # Instatiate an "EventFeed", for every...
    #   - network, contract, and relevant event signature.
    #   - a fully-qualified ABI for all contracts in use globally across the app.
    #   - an ordered list of clients where we should pull history of, ideally starting with archive/bulk and ending with JSON-RPC

    ev = EventFeed(10, '0x4200000000000000000000000000000000000042', 'Transfer(address,address,uint256)', abis, dcqs)

    ##########################
    # Instatiate a "Data Product", that would need to be maintained given one or more events.

    app.ctx.register('10.0x4200000000000000000000000000000000000042.Transfer(address,address,uint256)', Balances())
    app.ctx.register('10.0x4200000000000000000000000000000000000042.Transfer(address,address,uint256)', TransferCounts())

    app.ctx.add_event_feed(ev)

    app.add_task(ev.boot(app))

@app.after_server_start
async def subscribe_event_fees(app, loop):

    print("Adding signal handler for each event feed.")
    for ev in app.ctx.event_feeds:
        app.add_task(ev.run(app))

    

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=7656, dev=True, debug=True)
    #app.run(host="0.0.0.0", port=7654, dev=True, workers=1, access_log=True, debug=True)

