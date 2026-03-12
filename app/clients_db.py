import csv, json, os, sys

from collections import defaultdict

import psycopg2
import psycopg2.extras
import asyncpg

from abifsm import ABISet

from .utils import camel_to_snake
from .signatures import TRANSFER, PROPOSAL_CREATED_1, PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4, PROPOSAL_CREATED_MODULE, DELEGATE_CHANGED_2, VOTE_CAST_WITH_PARAMS_1

DB_SCHEMA = os.getenv('DB_SCHEMA', 'public')
TABLE_PREFIX = os.getenv('TABLE_PREFIX', 'multi_')

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

def _parse_json_if_str(obj):
    if isinstance(obj, str):
        return json.loads(obj)
    return obj


class DbClientCaster:

    def __init__(self, abis):
        self.abis = abis

    def lookup(self, signature):

        abi_frag = self.abis.get_by_signature(signature)

        int_fields = [camel_to_snake(o['name']) for o in abi_frag.inputs if o['type'] in INT_TYPES]

        if signature == TRANSFER:
            
            amount_field = camel_to_snake(abi_frag.fields[2])
            
            def caster_fn(event):
                event[amount_field] = int(event[amount_field])
                return event
            
            return caster_fn
        
        if signature == DELEGATE_CHANGED_2:

            def caster_fn(event):
                for field in ('old_delegatees', 'new_delegatees'):
                    val = event.get(field)
                    if val is None:
                        continue
                    val = _parse_json_if_str(val)
                    event[field] = [(addr.lower(), int(num)) for addr, num in val]
                return event

            return caster_fn

        if signature == VOTE_CAST_WITH_PARAMS_1:

            def caster_fn(event):
                params = event.get('params')
                if isinstance(params, (bytes, memoryview)):
                    event['params'] = bytes(params).hex()
                return event

            return caster_fn

        if signature in (PROPOSAL_CREATED_1, PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4):

            def caster_fn(event):
                for field in ('values', 'targets', 'calldatas', 'signatures'):
                    val = event.get(field, Ellipsis)
                    if val is Ellipsis:
                        continue
                    event[field] = _parse_json_if_str(val)
                return event

            return caster_fn

        if signature == PROPOSAL_CREATED_MODULE:

            def caster_fn(event):
                for field in ('settings', 'options'):
                    val = event.get(field, Ellipsis)
                    if val is Ellipsis:
                        continue
                    event[field] = _parse_json_if_str(val)
                return event

            return caster_fn

        # Default: DB types are already correct, no-op.
        def caster_fn(event):
            return event

        return caster_fn


class SubscriptionPlannerMixin:

    def init(self):
        self.subscription_meta = []

        self.abis_set = False

    def set_abis(self, abi_set: ABISet):

        assert not self.abis_set

        self.abis_set = True
        self.abis = abi_set
        if hasattr(self, 'casterCls'):
            self.caster = self.casterCls(self.abis)

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
        self.casterCls = DbClientCaster
        self.init()

    def add_pool(self, pool):
        self.pool = pool

    async def create_pool(self): 
        self.pool = await asyncpg.create_pool(
                dsn=self.url,
                min_size=5,
                max_size=50
            )

    def _sync_connect(self):
        return psycopg2.connect(self.url)

    def is_valid(self, url=None):

        target_url = url or self.url

        if target_url in ('', 'ignored', None):
            return False

        try:
            conn = psycopg2.connect(target_url)
            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Failed to connect to database ({target_url}): {e}")
            return False

    def check_table(self, table_name):

        try:
            conn = self._sync_connect()
            cur = conn.cursor()

            cur.execute("""SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = %s
                        AND table_name = %s;""", (DB_SCHEMA, table_name,))

            rows = cur.fetchall()

            print(f"Found Table Schema `{table_name}`: " + ",".join([f"{col_name}:{data_type}" for col_name, data_type in rows]))

            cur.close()
            conn.close()
            return len(rows) > 0
        except Exception as e:
            print(f"Failed to check table {table_name}: {e}")
            return False

    def plan_event(self, chain_id, address, signature):

        table_name = TABLE_PREFIX + self.abis.get_pgtable_by_signature(signature)

        abi_frag = self.abis.get_by_signature(signature)
        sighash = abi_frag.topic
        caster_fn = self.caster.lookup(signature)

        if not self.check_table(table_name):
            raise Exception(f"Table not found: {table_name}")
        else:
            self.subscription_meta.append(('event', (table_name, chain_id, address, signature, sighash, caster_fn)))

    def plan_block(self, chain_id):

        table_name = "blocks_" + CHAIN_ID_TO_BLOCK_TABLE[chain_id]

        if not self.check_table(table_name):
            raise Exception(f"Table not found: {table_name}")
        else:
            self.subscription_meta.append(('block', (table_name, chain_id)))


    def read(self, after):

        for event_or_block, subscription_meta in self.subscription_meta:

            new_signal = True

            if event_or_block == 'event':
                table_name, chain_id, address, signature, sighash, caster_fn = subscription_meta

                signal = f"{chain_id}.{address}.{signature}"

                for event in self.read_events(table_name, chain_id, address, signature, sighash, caster_fn, after):
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

    def read_events(self, table_name, chain_id, address, signature, sighash, caster_fn, after):

        conn = self._sync_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(
            f"""SELECT * FROM {DB_SCHEMA}.{table_name}
                WHERE address = %s AND chain_id = %s AND block_number >= %s
                ORDER BY block_number, transaction_index, log_index;""",
            (address, chain_id, after)
        )

        for row in cur:
            event = dict(row)
            event['block_number'] = str(event['block_number'])
            event['transaction_index'] = int(event['transaction_index'])
            event['log_index'] = int(event['log_index'])
            event['signature'] = signature
            event['sighash'] = sighash
            event = caster_fn(event)
            yield event

        cur.close()
        conn.close()

    def read_blocks(self, table_name, after):

        conn = self._sync_connect()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        cur.execute(
            f"""SELECT * FROM {DB_SCHEMA}.{table_name}
                WHERE block_number >= %s
                ORDER BY block_number;""",
            (after,)
        )

        for row in cur:
            block = dict(row)
            block['timestamp'] = int(block['timestamp'])
            block['block_number'] = int(block['block_number'])
            yield block

        cur.close()
        conn.close()




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
