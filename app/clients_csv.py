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
from .signatures import CSVClientCaster
from .profiling import Profiler


csv.field_size_limit(sys.maxsize)


class CSVClient:
    timeliness = 'archive'

    def __init__(self, path):

        if not isinstance(path, Path):
            self.path = Path(path)
        else:
            self.path = path
        
        self.subscription_meta = []

        self.abis_set = False

    def set_abis(self, abi_set: ABISet):

        assert not self.abis_set

        self.abis_set = True
        self.abis = abi_set
        self.casterCls = CSVClientCaster(self.abis)

    def is_valid(self):
        
        if os.path.exists(self.path):
            print(f"The path '{self.path}' exists, this client is valid.")
            return True
        else:
            print(f"The path '{self.path}' does not exist, this client is not valid.")
            return False
        
    def plan(self, chain_id, address, signature):

        fname = self.events_fname(chain_id, address, signature)

        abi_frag = self.abis.get_by_signature(signature)

        caster_fn = self.casterCls.lookup(signature)

        if not os.path.exists(fname):
            raise FileNotFoundError(f"CSV file not found: {fname}")
        else:
            self.subscription_meta.append((fname, signature, abi_frag, caster_fn))

    def read(self):
        
        for fname, signature, abi_frag, caster_fn in self.subscription_meta:

            for event in self.read_events(fname, signature, abi_frag, caster_fn):

                yield event
            

    def get_fallback_block(self, signature):
        return 0

    def events_fname(self, chain_id, address, signature):
        return self.path / f'{chain_id}/{address}/{signature}.csv'

    def blocks_fname(self, chain_id):
        return self.path / f'{chain_id}/blocks.csv'

    def read_blocks(self, chain_id, after=0):

        if after != 0:
            raise Exception("'After' block != 0, is not yet supported.  Instead, CSVs are just expected to only be last 8 days.")

        fname = self.blocks_fname(chain_id)

        with open(fname, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                row['timestamp'] = int(row['timestamp'])
                row['block_number'] = int(row['block_number'])
                yield row


    def read_events(self, fname, signature, abi_frag, caster_fn, after=0):

        if after == 0:
            
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