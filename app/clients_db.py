import csv, os, sys

from collections import defaultdict

from abifsm import ABISet

from .utils import camel_to_snake
from .signatures import TRANSFER, PROPOSAL_CREATED_1, PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4, PROPOSAL_CREATED_MODULE, DELEGATE_CHANGED_2


INT_TYPES = [f"uint{i}" for i in range(8, 257, 8)]
INT_TYPES.append("uint")

BYTE_TYPES = [f"bytes{i}" for i in range(1, 33)]
BYTE_TYPES.append("bytes")

csv.field_size_limit(sys.maxsize)


CHAIN_ID_TO_BLOCK_TABLE = {
    1: "ethereum",
    11155111: "ethereum_sepolia",
    10: "optimism",
    11155420: "optimism_sepolia",
    8453: "base",
    7560: "cyber",
    534352: "scroll",
    901: "derive",
    957: "derive-testnet",
    59144: "linea",
    59141: "linea-sepolia",
    42161: "arbitrum",
    421614: "arbitrum-sepolia"
}

def cast(event, fields, func):

    for field in fields:
        try:
            event[field] = func(event[field])
        except ValueError:
            print(f"E182250323 - Problem with casting {field} to {func.__name__}.")
        except KeyError:
            print(f"E184250323 - Problem with getting {field} to {func.__name__}.")
    
    return event

class SubscriptionPlannerMixin:

    def init(self):
        self.subscription_meta = []

        self.abis_set = False

    def set_abis(self, abi_set: ABISet):

        assert not self.abis_set

        self.abis_set = True
        self.abis = abi_set
        # self.caster = self.casterCls(self.abis)
    
    def plan(self, signal_type, signal_meta):

        if signal_type == 'event':
            self.plan_event(*signal_meta)
        elif signal_type == 'block':
            self.plan_block(*signal_meta)
        else:
            raise Exception(f"Unknown signal type: {signal_type}")
        


class DbHistClient(SubscriptionPlannerMixin):
    timeliness = 'archive'

    def __init__(self, url):

        self.url = url
        self.init()
    
    def add_pool(self, pool):
        self.pool = pool
    
    def is_valid(self):

        async def check_db():
            try:
                conn = await asyncpg.connect(self.url)
            except Exception as e:
                print(f"Failed to connect to database ({self.url}): ")
                print(e)
                return False
            
            rows = await conn.fetch("SELECT 1;")
            for row in rows:
                print(row)
            await conn.close()
            return True
        
        return asyncio.run(check_db())
    
    def check_table(self, table_name):

        print(f"Checking table {table_name}")

        async def check_t():
            try:
                conn = await asyncpg.connect(self.url)
            except Exception as e:
                print(f"Failed to connect to database ({self.url}): ")
                print(e)
                return False
            
            qry = f"""SELECT
                            column_name,
                            data_type
                        FROM information_schema.columns
                        WHERE table_schema = 'auazure'
                        AND table_name = '{table_name}_events';"""
        
            rows = await conn.fetch(qry)

            for row in rows:
                print(row['column_name'], row['data_type'])
            
            await conn.close()
            return True
        
        return asyncio.run(check_t())
    
    def plan_event(self, chain_id, address, signature):

        table_name = self.abis.get_pgtable_by_signature(signature)

        # caster_fn = self.caster.lookup(signature)

        if not self.check_table(table_name):
            raise Exception(f"Table not found: {table_name}")
        else:
            self.subscription_meta.append(('event', (table_name, chain_id, address, signature, None, None))) # , abi_frag, caster_fn)))

    def plan_block(self, chain_id):

        table_name = CHAIN_ID_TO_BLOCK_TABLE[chain_id]

        if not self.check_table(table_name):
            raise Exception(f"Table not found: {table_name}")
        else:
            self.subscription_meta.append(('block', (table_name, chain_id)))
    

    def read(self, after):

        assert after == 0 
        
        for event_or_block, subscription_meta in self.subscription_meta:

            new_signal = True

            if event_or_block == 'event':
                table_name, chain_id, address, signature, abi_frag, caster_fn = subscription_meta

                signal = f"{chain_id}.{address}.{signature}"

                for event in self.read_events(table_name, chain_id, address, signature, abi_frag, caster_fn, after):
                    yield event, signal, new_signal
                    new_signal = False

            elif event_or_block == 'block':
                table_name, chain_id = subscription_meta

                signal = f"{chain_id}.blocks"

                for block in self.read_blocks(table_name, after):
                    yield block, signal, new_signal
                    new_signal = False
            else:
                raise Exception(f"Unknown event_or_block: {event_or_block}")



    def get_fallback_block(self):
        return 0

    def events_fname(self, chain_id, address, signature):
        return self.path / f'{chain_id}/{address}/{signature}.csv'

    def blocks_fname(self, chain_id):
        return self.path / f'{chain_id}/blocks.csv'

    def read_events(self, table_name, chain_id, address, signature, abi_frag, caster_fn, after):

        async def read():
            try:
                conn = await asyncpg.connect(self.url)
            except Exception as e:
                print(f"Failed to connect to database ({self.url}): ")
                print(e)
                return False
            
            # TODO - figure out if this should be > or >= :rea
            print("! OPEN TODO...")

            qry = f"""SELECT * FROM auazure.{table_name}_events WHERE address='{address}' AND chain_id={chain_id} AND block_number >= {after};"""
        
            rows = await conn.fetch(qry)
            
            await conn.close()

            return rows
        
        return asyncio.run(read())

    def read_blocks(self, table_name, after):

        async def read():
            conn = await asyncpg.connect(self.url)

            # TODO - figure out if this should be > or >= :rea
            print("! OPEN TODO...")
            qry = f"""SELECT * FROM auazure.blocks_{table_name} WHERE block_number >= {after};"""
        
            rows = await conn.fetch(qry)

            for row in rows:
                yield row
            
            await conn.close()
            
        async def collect():
            async for r in read():
                yield r
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        agen = read()

        while True:
            try:
                yield loop.run_until_complete(agen.__anext__())
            except StopAsyncIteration:
                break




if __name__ == '__main__':

    import asyncio
    import asyncpg

    from abifsm import ABI, ABISet
    os.environ['ABI_URL'] = 'https://storage.googleapis.com/agora-abis/v2'

    abi_list = []

    chain_id = 10

    token_addr = '0x4200000000000000000000000000000000000042'
    token_abi = ABI.from_internet('token', token_addr, chain_id=chain_id, implementation=True)
    abi_list.append(token_abi)

    abis = ABISet('optimism', abi_list)

    client = DbHistClient('postgres://postgres:MumEGVmUDXhiYfJiNFLwx6G2qx9CYxg6AddUe8utgsR7YGM6Af@34.168.197.190/prod')
    
    assert client.is_valid()

    client.set_abis(abis)

    client.plan('block', (10,))

    # client.plan('event', (10, token_addr, 'DelegateChanged(address,address,address)'))
    # client.plan('event', (10, token_addr, 'DelegateVotesChanged(address,uint256,uint256)'))
    # client.plan('event', (10, token_addr, 'Transfer(address,address,uint256)'))

    # profiler = Profiler()
    # count = defaultdict(int)


    reader = client.read(0)

    first_loop = True

    while True:

        event = next(reader)
        print(event)
    
    """

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
    """