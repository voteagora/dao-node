from collections import defaultdict
from abc import ABC, abstractmethod

from eth_abi.abi import decode as decode_abi
from .utils import camel_to_snake
from copy import copy

class DataProduct(ABC):

    @abstractmethod
    def handle(self, event):
        pass

    @property
    def name(self):
        return camel_to_snake(self.__class__.__name__)
    

class Balances(DataProduct):

    def __init__(self, token_spec):
        self.balances = defaultdict(int)

        self.erc20 = token_spec['name'] == 'erc20'

        # U is for uniswap, for lack of a better framing.
        if token_spec['version'] == 'U':
            self.value_field_name = 'amount'
        else:
            self.value_field_name = 'value'

    def handle(self, event):

        field = self.value_field_name

        self.balances[event['from']] -= event[field]
        self.balances[event['to']] += event[field]
    
    def balance_of(self, address):
        return self.balances[address]

    def top(self, k):

        values = list(self.balances.items())
        values.sort(key=lambda x:x[-1])
        return [v[0] for v in values[-1 * int(k):]]

class ProposalTypes(DataProduct):
    def __init__(self):
        self.proposal_types = {}
        self.proposal_types_history = defaultdict(list)

    def handle(self, event):

        proposal_type_info = {k : event[k] for k in ['quorum', 'approval_threshold', 'name']}

        proposal_type_id = event['proposal_type_id']

        self.proposal_types[proposal_type_id] = proposal_type_info
        self.proposal_types_history[proposal_type_id].append(event)
    
    def get_historic_proposal_type(self, proposal_type_id, block_number):

        proposal_type_history = self.proposal_types_history[proposal_type_id]

        pit_proposal_type = None

        for proposal_type in proposal_type_history:
            if proposal_type['block_number'] > block_number:
                break
            pit_proposal_type = proposal_type

        return {k : pit_proposal_type[k] for k in ['quorum', 'approval_threshold', 'name']}

class Scopes(DataProduct):
    def __init__(self):
        self.scopes = {}
        self.scopes_history = defaultdict(list)
        self.disabled_scopes = set()
        self.deleted_scopes = set()

    def handle(self, event):
        signature = event['signature']
        proposal_type_id = event['proposal_type_id']
        scope_key = event['scope_key']

        if signature == 'ScopeCreated(uint8,bytes24,bytes4,string)':
            scope_info = {
                'proposal_type_id': proposal_type_id,
                'scope_key': scope_key,
                'selector': event['selector'],
                'description': event['description']
            }
            self.scopes[scope_key] = scope_info
            self.scopes_history[scope_key].append(event)
            # Remove from disabled/deleted sets if it was there
            self.disabled_scopes.discard(scope_key)
            self.deleted_scopes.discard(scope_key)

        elif signature == 'ScopeDisabled(uint8,bytes24)':
            self.disabled_scopes.add(scope_key)

        elif signature == 'ScopeDeleted(uint8,bytes24)':
            self.deleted_scopes.add(scope_key)
            # Remove from disabled set if it was there
            self.disabled_scopes.discard(scope_key)

    def get_scope(self, scope_key):
        if scope_key in self.deleted_scopes:
            return None
        scope = self.scopes.get(scope_key)
        if not scope:
            return None
        return {
            **scope,
            'disabled': scope_key in self.disabled_scopes
        }

    def get_all_scopes(self):
        return [
            self.get_scope(scope_key)
            for scope_key in self.scopes.keys()
            if scope_key not in self.deleted_scopes
        ]

class Delegations(DataProduct):
    def __init__(self):
        self.delegator = defaultdict(None) # owner, doing the delegation
        
        # Data about the delegatee (ie, the delegate's influence)
        self.delegatee_list = defaultdict(list) #  list of delegators
        self.delegatee_cnt = defaultdict(int) #  dele
        
        self.delegatee_vp = defaultdict(int) # delegate, receiving the delegation, this is there most recent VP across all delegators

        self.voting_power = 0

        self.delegatee_vp_history = defaultdict(list)

    def handle(self, event):

        signature = event['signature']
        block_number = event['block_number']

        if signature == 'DelegateChanged(address,address,address)':

            delegator = event['delegator'].lower()

            to_delegate = event['to_delegate'].lower()
            from_delegate = event['from_delegate'].lower()

            self.delegator[delegator] = to_delegate

            self.delegatee_list[to_delegate].append(delegator)

            if (from_delegate != '0x0000000000000000000000000000000000000000'):
                try:
                    self.delegatee_list[from_delegate].remove(delegator)
                    self.delegatee_cnt[from_delegate] = len(self.delegatee_list[from_delegate])
                except ValueError as e:
                    print(f"E109250323 - Problem removing delegator '{delegator}' this is unexpected. ({from_delegate=}, {to_delegate=})")

            self.delegatee_cnt[to_delegate] = len(self.delegatee_list[to_delegate])

        elif signature == 'DelegateVotesChanged(address,uint256,uint256)':

            delegatee = event['delegate'].lower()

            # TODO figure out why optimism's abi encode new_balance/previous_balance,
            # but more modern DAOs seem to rely on new_votes/previous_votes.
            new_votes = int(event.get('new_votes', event.get('new_balance', None)))
            previous_votes = int(event.get('previous_votes', event.get('previous_balance', None)))

            assert new_votes is not None
            assert previous_votes is not None

            self.voting_power += (new_votes - previous_votes)
            self.delegatee_vp[delegatee] = new_votes

            block_number = int(event['block_number'])

            self.delegatee_vp_history[delegatee].append((block_number, new_votes))


LCREATED = len('ProposalCreated')
LQUEUED = len('ProposalQueued')
LEXECUTED = len('ProposalExecuted')
LCANCELED = len('ProposalCanceled')

def decode_proposal_calldata(calldata: str, abi_types):
    """
    Decode Ethereum calldata using provided ABI types
    Args:
        calldata: Hex string of calldata
        abi_types: List of ABI type strings
    Returns:
        Tuple of decoded values
    """
    # Remove '0x' prefix if present
    calldata = calldata.replace('0x', '')
    # Convert to bytes
    calldata_bytes = HexBytes(calldata)
    
    try:
        # First try to decode just the first part (the array of tuples)
        first_type = abi_types[0]
        decoded_first = decode([first_type], calldata_bytes)
        
        if len(abi_types) > 1:
            # If there's a second type, try to decode any remaining data
            # This assumes the second part starts after the first decoded part
            second_type = abi_types[1]
            try:
                # Get the remaining data after the first decode
                remaining_data = calldata_bytes[64:]  # Skip the first dynamic pointer
                decoded_second = decode([second_type], remaining_data)
                return (decoded_first[0], decoded_second[0])
            except Exception as e:
                print(f"Failed to decode second part: {e}")
                return (decoded_first[0], None)
        return decoded_first[0]
    except Exception as e:
        print(f"Failed to decode calldata: {e}")
        return None

def bytes_to_hex(obj):
    if isinstance(obj, bytes):
        return obj.hex()
    elif isinstance(obj, dict):
        return {k: bytes_to_hex(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [bytes_to_hex(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(bytes_to_hex(item) for item in obj)
    else:
        return obj
        
def decode_proposal_data(proposal_type, proposal_data):

    if proposal_type == 'standard':
        return None
    
    if proposal_type == 'approval':
        abi = ["(uint256,address[],uint256[],bytes[],string)[]", "(uint8,uint8,address,uint128,uint128)"]
        abi2 = ["(address[],uint256[],bytes[],string)[]",        "(uint8,uint8,address,uint128,uint128)"] # OP/alligator only? Only for 0xe1a17f4770769f9d77ef56dd3b92500df161f3a1704ab99aec8ccf8653cae400l
    elif proposal_type == 'optimistic':
        abi = ["(uint248,bool)"]
    else:
        raise Exception("Unknown Proposal Type: {}".format(proposal_type))

    if proposal_data[:2] == '0x':
        proposal_data = proposal_data[2:]
    proposal_data = bytes.fromhex(proposal_data)

    try:
        result = decode_abi(abi, proposal_data)
    except:
        result = decode_abi(abi2, proposal_data)
        result = bytes_to_hex(result)

    return result

class Proposal:
    def __init__(self, create_event):
        self.create_event = create_event
        self.canceled = False
        self.queued = False
        self.executed = False

        self.result = defaultdict(nested_default_dict)
    
    def cancel(self, cancel_event):
        self.canceled = True
        self.cancel_event = cancel_event

    def queue(self, queue_event):
        self.queued = True
        self.queue_event = queue_event

    def execute(self, execute_event):
        self.executed = True
        self.execute_event = execute_event

    def to_dict(self):

        out = self.create_event

        if self.canceled:
            out['cancel_event'] = self.cancel_event

        if self.queued:
            out['queue_event'] = self.queue_event

        if self.executed:
            out['execute_event'] = self.execute_event

        return out

    def set_voting_module_name(self, name):
        self.create_event['voting_module_name'] = name
        
    def resolve_voting_module_name(self, modules):
        self.voting_module_name = modules.get(self.voting_module_address, "standard")
        self.create_event['voting_module_name'] = self.voting_module_name

    @property
    def voting_module_address(self):
        addr = self.create_event.get('voting_module', None)
        if addr:
            return addr.lower()

    

def decode_create_event(event) -> Proposal:

    event['description'] = str(event['description']) # Some proposals are just bytes.

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
    
    return Proposal(event)


class Proposals(DataProduct):

    def __init__(self, governor_spec, modules=None):
        self.proposals = {}

        if modules:
            self.modules = modules
        else:
            self.modules = {}

        if governor_spec['name'] == 'compound':
            self.proposal_id_field_name = 'id'
        else:
            self.proposal_id_field_name = 'proposal_id'

        self.gov_spec = governor_spec
    
    def handle(self, event):

        try:
            signature = event['signature']
        except:
            print(f"E187250323 Problem getting signature from event: {event}.")

        proposal_id = str(event[self.proposal_id_field_name])

        del event[self.proposal_id_field_name]
        event['id'] = proposal_id

        del event['signature']
        del event['sighash']

        try:
            if 'ProposalCreated' == signature[:LCREATED]:
                proposal = decode_create_event(event)
                
                if self.gov_spec['name'] == 'agora':
                    proposal.resolve_voting_module_name(self.modules)

                    voting_module_name = proposal.voting_module_name # standard / approval / optimistic

                    if voting_module_name in ('approval','optimistic'):
                        proposal_data = proposal.create_event['proposal_data']
                    
                        proposal.create_event['decoded_proposal_data'] = decode_proposal_data(voting_module_name, proposal_data)
                else:
                    proposal.set_voting_module_name('standard')
                    
                self.proposals[proposal_id] = proposal

            elif 'ProposalQueued' == signature[:LQUEUED]:
                self.proposals[proposal_id].queue(event)
            
            elif 'ProposalExecuted' == signature[:LEXECUTED]:
                self.proposals[proposal_id].execute(event)

            elif 'ProposalCanceled' == signature[:LCANCELED]:
                self.proposals[proposal_id].cancel(event)

        except KeyError as e:
            print(f"E248250323 - Problem with the following proposal_id {proposal_id} and the {signature} event: {e}")
    
    def unfiltered(self, head=-1):
        for proposal in reversed(self.proposals.values()):
            yield proposal

    def active(self, head=-1):
        for proposal in reversed(self.proposals.values()):
            if not proposal.canceled and not proposal.queued and not proposal.executed:
                yield proposal

    def relevant(self, head=-1):
        for proposal in reversed(self.proposals.values()):
            if not proposal.canceled:
                yield proposal

    def completed(self, head=-1):
        for proposal in reversed(self.proposals.values()):
            if not proposal.canceled and proposal.queued:
                if head > 0 or head <= -1:
                    yield proposal
                    head -= 1

def nested_default_dict():
    return defaultdict(int)


class VoteAggregation:
    def __init__(self):
        self.result = defaultdict(nested_default_dict)
    
    def tally(self, event):

        votes = event.get('votes', 0)
        weight = int(event.get('weight', votes))
        
        params = event.get('params', None)
        if params:
            params, = decode_abi(["uint256[]"], bytes.fromhex(params))
            event['params'] = params

            for param in params:
                self.result[param][event['support']] += weight
        else:
            self.result['no-param'][event['support']] += weight
        
        return event

    def totals(self):

        totals = defaultdict(dict)
        
        for okey in self.result.keys():
            for key, value in self.result[okey].items():
                totals[okey].update(**{str(key) : str(value)})
        
        return totals


class Votes(DataProduct):
    def __init__(self, governor_spec):
        self.proposal_aggregations = defaultdict(VoteAggregation)

        self.voter_history = defaultdict(list)
        self.proposal_vote_record = defaultdict(list)

        if governor_spec['name'] == 'compound':
            self.proposal_id_field_name = 'id'
        else:
            self.proposal_id_field_name = 'proposal_id'
    
    def handle(self, event):

        PROPOSAL_ID_FIELD = self.proposal_id_field_name

        try:
            proposal_id = str(event['proposal_id'])
        except KeyError as e:
            print(f"E292250323 - Problem with the following event {event}.")

        event = self.proposal_aggregations[proposal_id].tally(event)

        event_cp = copy(event)

        del event_cp['sighash']
        del event_cp['signature']

        self.voter_history[event['voter']].append(event_cp)

        event_cp = copy(event_cp)
        del event_cp['proposal_id']

        self.proposal_vote_record[proposal_id].append(event_cp)

class ParticipationModel:
    """
    This participation model looks at the 10 most recent completed non-cancelled votes
    
    And then checks to see if a specific delegate voted in those relevant proposals.

    A logical enhancement from here, would be Creating a DataProduct that calculated 
    this for all delegates at the time of the completion for any proposal.  This would 
    move the calc from the endpoint to the data product step.
    """

    def __init__(self, proposals_dp : Proposals, votes_dp : Votes):
        self.proposals = proposals_dp
        self.votes = votes_dp

        self.relevant_proposals = [int(p.create_event['id']) for p in self.proposals.completed(head=10)]
    
    def calculate(self, addr):

        num = 0
        den = 0

        historic_proposal_ids = [vote['proposal_id'] for vote in self.votes.voter_history[addr]]
        recent_proposal_ids = set(historic_proposal_ids[:10])

        for proposal_id in self.relevant_proposals:
            if proposal_id in recent_proposal_ids:
                den += 1
            num += 1
        
        return den / num
