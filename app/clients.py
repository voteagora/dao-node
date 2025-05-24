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

from .utils import camel_to_snake
from .profiling import Profiler
from collections import defaultdict 

csv.field_size_limit(sys.maxsize)

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

DEBUG = False

INT_TYPES = [f"uint{i}" for i in range(8, 257, 8)]
INT_TYPES.append("uint")

class CSVClient:
    timeliness = 'archive'

    def __init__(self, path):

        if not isinstance(path, Path):
            self.path = Path(path)
        else:
            self.path = path

    def is_valid(self):
        
        if os.path.exists(self.path):
            print(f"The path '{self.path}' exists, this client is valid.")
            return True
        else:
            print(f"The path '{self.path}' does not exist, this client is not valid.")
            return False

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


    def read(self, chain_id, address, signature, abis, after=0):

        abi_frag = abis.get_by_signature(signature)

        if abi_frag is None:
            raise KeyError(f"Signature `{signature}` Not Found")

        fname = self.events_fname(chain_id, address, signature)

        int_fields = [camel_to_snake(o['name']) for o in abi_frag.inputs if o['type'] in INT_TYPES]

        cnt = 0

        if after == 0:
            
            try:
                fs = open(fname)
                reader = csv.DictReader(fs)
            
                for row in reader:

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
                            print(f"E182250323 - Problem with casting {int_field} to int, from file {fname}.")
                        except KeyError:
                            print(f"E184250323 - Problem with getting {int_field} from file {fname}.")

                    yield row

                    cnt += 1
                    
                    if DEBUG and (cnt == 1000000):
                        break
            except FileNotFoundError:
                raise FileNotFoundError(f"CSV file not found: {fname}")


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

    profiler = Profiler()
    count = defaultdict(int)

    for signature in ['DelegateChanged(address,address,address)', 'DelegateVotesChanged(address,uint256,uint256)', 'Transfer(address,address,uint256)']:
        reader = client.read(chain_id=10, address=token_addr, signature=signature, abis=abis)

        while True:
            with profiler(signature):
                try:
                    event = next(reader)
                except StopIteration:
                    break
            
            count[signature] += 1

