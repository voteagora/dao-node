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
from abifsm import ABISet

from collections import defaultdict
from .utils import camel_to_snake

from .signatures import TRANSFER

# from .profiling import Profiler


csv.field_size_limit(sys.maxsize)

from .utils import camel_to_snake

INT_TYPES = [f"uint{i}" for i in range(8, 257, 8)]
INT_TYPES.append("uint")

class CSVClientCaster:
    
    def __init__(self, abis):
        self.abis = abis
    
    def lookup(self, signature):

        abi_frag = self.abis.get_by_signature(signature)

        def caster_maker():

            int_fields = [camel_to_snake(o['name']) for o in abi_frag.inputs if o['type'] in INT_TYPES]

            def caster_fn(event):
                for int_field in int_fields:
                    try:
                        event[int_field] = int(event[int_field])
                    except ValueError:
                        print(f"E182250323 - Problem with casting {int_field} to int, from file {fname}.")
                    except KeyError:
                        print(f"E184250323 - Problem with getting {int_field} from file {fname}.")
                return event

            return caster_fn
        
        if signature == TRANSFER:
            
            amount_field = abi_frag.fields[2]
            
            def caster_fn(event):
                event[amount_field] = int(event[amount_field])
                return event
            
            return caster_fn

        else:
            caster_fn = caster_maker()

        return caster_fn

class SubscriptionPlannerMixin:

    def init(self):
        self.subscription_meta = []

        self.abis_set = False

    def set_abis(self, abi_set: ABISet):

        assert not self.abis_set

        self.abis_set = True
        self.abis = abi_set
        self.caster = self.casterCls(self.abis)
    
    def plan(self, signal_type, signal_meta):

        if signal_type == 'event':
            self.plan_event(*signal_meta)
        elif signal_type == 'block':
            self.plan_block(*signal_meta)
        else:
            raise Exception(f"Unknown signal type: {signal_type}")
        

class CSVClient(SubscriptionPlannerMixin):
    timeliness = 'archive'

    def __init__(self, path):

        if not isinstance(path, Path):
            self.path = Path(path)
        else:
            self.path = path
        self.init()
        self.casterCls = CSVClientCaster
        
    def plan_event(self, chain_id, address, signature):

        fname = self.events_fname(chain_id, address, signature)

        abi_frag = self.abis.get_by_signature(signature)

        caster_fn = self.caster.lookup(signature)

        if not os.path.exists(fname):
            raise FileNotFoundError(f"CSV file not found: {fname}")
        else:
            self.subscription_meta.append(('event', (fname, chain_id, address, signature, abi_frag, caster_fn)))

    def plan_block(self, chain_id):

        fname = self.blocks_fname(chain_id)

        if not os.path.exists(fname):
            raise FileNotFoundError(f"CSV file not found: {fname}")
        else:
            self.subscription_meta.append(('block', fname))
    
    def is_valid(self):
        
        if os.path.exists(self.path):
            print(f"The path '{self.path}' exists, this client is valid.")
            return True
        else:
            print(f"The path '{self.path}' does not exist, this client is not valid.")
            return False

    def plan(self, signal_type, signal_meta):

        if signal_type == 'event':
            self.plan_event(*signal_meta)
        elif signal_type == 'block':
            self.plan_block(*signal_meta)
        else:
            raise Exception(f"Unknown signal type: {signal_type}")
        
    def plan_event(self, chain_id, address, signature):

        fname = self.events_fname(chain_id, address, signature)

        abi_frag = self.abis.get_by_signature(signature)

        caster_fn = self.caster.lookup(signature)

        if not os.path.exists(fname):
            raise FileNotFoundError(f"CSV file not found: {fname}")
        else:
            self.subscription_meta.append(('event', (fname, chain_id, address, signature, abi_frag, caster_fn)))

    def plan_block(self, chain_id):

        fname = self.blocks_fname(chain_id)

        if not os.path.exists(fname):
            raise FileNotFoundError(f"CSV file not found: {fname}")
        else:
            self.subscription_meta.append(('block', fname))


    def read(self, after):

        assert after == 0
        
        for event_or_block, subscription_meta in self.subscription_meta:

            new_signal = True

            if event_or_block == 'event':
                fname, chain_id, address, signature, abi_frag, caster_fn = subscription_meta

                signal = f"{chain_id}.{address}.{signature}"

                for event in self.read_events(fname, signature, abi_frag, caster_fn):
                    yield event, signal, new_signal
                    new_signal = False

            elif event_or_block == 'block':
                fname = subscription_meta

                signal = f"{chain_id}.blocks"

                for block in self.read_blocks(fname):
                    yield block, signal, new_signal
                    new_signal = False
            else:
                raise Exception(f"Unknown event_or_block: {event_or_block}")



    def get_fallback_block(self, signature):
        return 0

    def events_fname(self, chain_id, address, signature):
        return self.path / f'{chain_id}/{address}/{signature}.csv'

    def blocks_fname(self, chain_id):
        return self.path / f'{chain_id}/blocks.csv'

    def read_blocks(self, fname):

        with open(fname, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['timestamp'] = int(row['timestamp'])
                row['block_number'] = int(row['block_number'])
                yield row


    def read_events(self, fname, signature, abi_frag, caster_fn):

        fs = open(fname)
        reader = csv.DictReader(fs)
    
        for row in reader:

            row['log_index'] = int(row['log_index'])
            row['transaction_index'] = int(row['transaction_index'])

            row['signature'] = signature
            row['sighash'] = abi_frag.topic

            row = caster_fn(row)

            yield row



if __name__ == '__main__':

    from abifsm import ABI, ABISet
    os.environ['ABI_URL'] = 'https://storage.googleapis.com/agora-abis/v2'

    abi_list = []

    chain_id = 10

    token_addr = '0x4200000000000000000000000000000000000042'
    token_abi = ABI.from_internet('token', token_addr, chain_id=chain_id, implementation=True)
    abi_list.append(token_abi)

    abis = ABISet('daonode', abi_list)

    client = CSVClient('/Users/jm/code/dao_node/data')
    assert client.is_valid()

    client.set_abis(abis)

    client.plan(chain_id=10, address=token_addr, signature='DelegateChanged(address,address,address)')
    client.plan(chain_id=10, address=token_addr, signature='DelegateVotesChanged(address,uint256,uint256)')
    client.plan(chain_id=10, address=token_addr, signature='Transfer(address,address,uint256)')

    profiler = Profiler()
    count = defaultdict(int)


    reader = client.read()

    first_loop = True

    while True:

        if first_loop:
            event = next(reader)
            signature = event['signature']
            first_loop = False

        with profiler(signature):
            try:
                event = next(reader)
            except StopIteration:
                break
        
        signature = event['signature']
        
        count[signature] += 1