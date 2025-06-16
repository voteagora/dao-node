import csv, os, sys, json

from pathlib import Path
from collections import defaultdict

from abifsm import ABISet

from .utils import camel_to_snake
from .signatures import TRANSFER, PROPOSAL_CREATED_1, PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4, PROPOSAL_CREATED_MODULE, DELEGATE_CHANGED_2


INT_TYPES = [f"uint{i}" for i in range(8, 257, 8)]
INT_TYPES.append("uint")

BYTE_TYPES = [f"bytes{i}" for i in range(1, 33)]
BYTE_TYPES.append("bytes")

csv.field_size_limit(sys.maxsize)


def cast(event, fields, func):

    for field in fields:
        try:
            event[field] = func(event[field])
        except ValueError:
            print(f"E182250323 - Problem with casting {field} to {func.__name__}.")
        except KeyError:
            print(f"E184250323 - Problem with getting {field} to {func.__name__}.")
    
    return event


class CSVClientCaster:
    
    def __init__(self, abis):
        self.abis = abis
    
    def lookup(self, signature):

        abi_frag = self.abis.get_by_signature(signature)

        int_fields = [camel_to_snake(o['name']) for o in abi_frag.inputs if o['type'] in INT_TYPES]

        # bytes_fields = [camel_to_snake(o['name']) for o in abi_frag.inputs if o['type'] in BYTE_TYPES]

        # DEFAULT CASE...

        def caster_maker():

            def caster_fn(event):

                event = cast(event, int_fields, int)
                # event = cast(event, bytes_fields, lambda x: x.hex())
                
                return event

            return caster_fn
        
        # SPECIFIC CASES...
        
        if signature == TRANSFER:
            
            amount_field = camel_to_snake(abi_frag.fields[2])
            
            def caster_fn(event):
                event[amount_field] = int(event[amount_field])
                return event
            
            return caster_fn
        
        if signature == DELEGATE_CHANGED_2:

            def _parse_delegate_array(array_str):

                array_str = array_str.strip('"')
                if not array_str or array_str == '[]':
                    return []
                
                delegates = json.loads(array_str)
                return [[addr.lower(), int(amount)] for addr, amount in delegates]

            def caster_fn(event):
                event['old_delegatees'] = _parse_delegate_array(event['old_delegatees'])
                event['new_delegatees'] = _parse_delegate_array(event['new_delegatees'])
                return event

            return caster_fn


        if signature == PROPOSAL_CREATED_MODULE:
            
            def caster_fn(event):
                event = cast(event, int_fields, int)
                
                obj = event.get('settings', Ellipsis)
                if obj is not Ellipsis:
                    if isinstance(obj, str):
                        obj = json.loads(obj)
                    event['settings'] = obj

                obj = event.get('options', Ellipsis)
                if obj is not Ellipsis:
                    if isinstance(obj, str):
                        obj = obj.replace('"', '')
                        obj = obj[1:-1]
                        obj = obj.split(',')
                    event['options'] = obj

                return event
            
            return caster_fn

        if signature in [PROPOSAL_CREATED_1, PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4]:
            
            def caster_fn(event):

                event = cast(event, int_fields, int)
                # event = cast(event, bytes_fields, str)
            
                # This was in the old implementation, but it should be caught in the above.

                obj = event.get('values', Ellipsis)
                if obj is not Ellipsis:
                    if isinstance(obj, str):
                        obj = obj[1:-1]
                        obj = obj.split(',')
                        obj = [int(x) for x in obj]
                    event['values'] = obj

                obj = event.get('targets', Ellipsis)
                if obj is not Ellipsis:
                    if isinstance(obj, str):
                        obj = obj.replace('"', '')
                        obj = obj[1:-1]
                        obj = obj.split(',')
                    event['targets'] = obj

                obj = event.get('calldatas', Ellipsis)
                if obj is not Ellipsis:
                    if isinstance(obj, str):
                        obj = obj.replace('"', '')
                        obj = obj[1:-1]
                        obj = obj.split(',')
                    event['calldatas'] = obj

                obj = event.get('signatures', Ellipsis)
                if obj is not Ellipsis:
                    if isinstance(obj, str):
                        obj = obj[2:-2]
                        obj = obj.split('","')
                    event['signatures'] = obj

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
    
    def is_valid(self):
        
        if os.path.exists(self.path):
            print(f"The path '{self.path}' exists, this client is valid for {self.__class__.__name__}")
            return True
        else:
            print(f"The path '{self.path}' does not exist, this client is not valid for {self.__class__.__name__}")
            return False
        
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
            self.subscription_meta.append(('block', (fname, chain_id)))
    

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
                fname, chain_id = subscription_meta

                signal = f"{chain_id}.blocks"

                for block in self.read_blocks(fname):
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