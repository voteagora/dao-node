from copy import copy
from collections import defaultdict
from sortedcontainers import SortedDict
from abc import ABC, abstractmethod
import json, time

from eth_abi.abi import decode as decode_abi
from .utils import camel_to_snake

from .signatures import *

class ToDo(NotImplementedError):
    pass

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
        self.proposal_types = defaultdict(dict)
        self.proposal_types_history = defaultdict(list)

    def handle(self, event):

        signature = event['signature']

        proposal_type_id = event['proposal_type_id']

        if 'ProposalTypeSet' in signature:
            proposal_type_info = {k : event.get(k, None) for k in ['quorum', 'approval_threshold', 'name', 'module']}

            self.proposal_types[proposal_type_id].update(**proposal_type_info)

            if not 'scopes' in self.proposal_types[proposal_type_id].keys():
                self.proposal_types[proposal_type_id]['scopes'] = []
            self.proposal_types_history[proposal_type_id].append(event)

        elif 'Scope' in signature:
            
            event = copy(event)
            scope_key = event['scope_key']

            del event['signature']
            del event['sighash']

            if 'Created' in signature:
                del event['proposal_type_id']
                event['status'] = 'created'
                event['disabled_event'] = {}
                event['deleted_event'] = {}
                self.proposal_types[proposal_type_id]['scopes'].append(event)
            elif 'Disabled' in signature:
                # Will disable all scopes with the scope_key
                for scope in self.proposal_types[proposal_type_id]['scopes']:
                    if scope['scope_key'] == scope_key:
                        scope['disabled_event'] = event
                        scope['status'] = 'disabled'
            elif 'Deleted' in signature:
                # Will delete all scopes with the scope_key
                for scope in self.proposal_types[proposal_type_id]['scopes']:
                    if scope['scope_key'] == scope_key:
                        scope['deleted_event'] = event
                        scope['status'] = 'deleted'
            else:
                raise Exception(f"Event signature {signature} not handled.")
        
        else:
            raise Exception(f"Event signature {signature} not handled.")


    def get_historic_proposal_type(self, proposal_type_id, block_number):

        proposal_type_history = self.proposal_types_history[proposal_type_id]

        pit_proposal_type = None

        for proposal_type in proposal_type_history:
            if int(proposal_type['block_number']) > int(block_number):
                break
            pit_proposal_type = proposal_type

        return {k : pit_proposal_type[k] for k in ['quorum', 'approval_threshold', 'name']}

def round_to_hour(ts):
    return ts - (ts % 3600)

def seven_days_ago(ts):
    return ts - (7 * 24 * 60 * 60)

def round_and_seven_days_ago(ts):
    tmp = round_to_hour(ts)
    return seven_days_ago(tmp)

class Delegations(DataProduct):
    def __init__(self):
        self.delegator = defaultdict(None) # owner, doing the delegation
        
        # Data about the delegatee (ie, the delegate's influence)
        self.delegatee_list = defaultdict(SortedDict) #  list of delegators
        self.delegatee_cnt = defaultdict(int) #  dele

        self.delegatee_vp = defaultdict(int) # delegate, receiving the delegation, this is there most recent VP across all delegators
        self.delegation_amounts = defaultdict(dict)

        self.voting_power = 0

        self.delegatee_vp_history = defaultdict(list)

        # Track the oldest and latest delegation events for each delegate
        self.delegatee_oldest_event = defaultdict(dict)
        self.delegatee_latest_event = defaultdict(dict)
        
        # Track the oldest and latest delegation events for each delegate
        self.delegatee_oldest = {}
        self.delegatee_latest = {}

        self.timestamp_to_block = SortedDict()
        self.delegatee_vp_recent_history = defaultdict(SortedDict)

        self.current_block_number = 0
        self.current_ts = 0
        self.current_rounded_ts = 0

        self.seven_day_block_number = 0
        self.seven_day_ts = 0

        self.cached_seven_day_vp = defaultdict(lambda: (0, 0))

    def _parse_delegate_array(self, array_str):

        try:
            # This is indirectly checking to see if this is string-like
            array_str = array_str.strip('"')
        except AttributeError:
            # "SO THIS SUCKS...  
            # it appears as if the JSON-RPC selectively decodes these arrays of tuples willy nilly...
            # and returns them as an attribute dict like this:
            # [AttributeDict({'_delegatee': '0x7B0befc5B043148Cd7bD5cFeEEf7BC63D28edEC0', '_numerator': 302}), 
            #  AttributeDict({'_delegatee': '0x9870DE32a48f4F721D8e866b23F7E9D4581FCc2f', '_numerator': 155})]
            if isinstance(array_str, list):
                return [(x['_delegatee'].lower(), x['_numerator']) for x in array_str]
            raise
    
        if not array_str or array_str == '[]':
            return []
        
        delegates = json.loads(array_str)
        return [[addr.lower(), int(amount)] for addr, amount in delegates]
        
    def handle_block(self, event):

        timestamp = event['timestamp']
        block_number = event['block_number']

        assert isinstance(block_number, int)
        assert isinstance(timestamp, int)

        self.current_block_number = block_number
        self.current_ts = timestamp

        self.timestamp_to_block[timestamp] = block_number

        rounded_ts = round_to_hour(timestamp)
        self.rounded_seven_day_ts = seven_days_ago(rounded_ts)

        if self.current_rounded_ts != rounded_ts:
            self.current_rounded_ts = rounded_ts

            # bisect_right returns the first position after seven_days_ago, 
            # so the -1 gets the previous block
            index = self.timestamp_to_block.bisect_right(self.rounded_seven_day_ts)
            if index != 0:
                closest_key = self.timestamp_to_block.keys()[index - 1]
                self.seven_day_block_number = self.timestamp_to_block[closest_key]
            else:
                pass # print("No timestamp found that is older than 7 days.")

    def handle(self, event):

        signature = event['signature']
        block_number = event['block_number']
        transaction_index = event['transaction_index']

        if signature == DELEGATE_CHANGED_1:

            delegator = event['delegator'].lower()
            to_delegate = event['to_delegate'].lower()
            from_delegate = event['from_delegate'].lower()

            self.delegator[delegator] = to_delegate
            self.delegatee_list[to_delegate][delegator] = (block_number, transaction_index)

            if not self.delegatee_oldest_event.get(to_delegate):
                self.delegatee_oldest_event[to_delegate] = {
                    'block_number': block_number,
                    'delegator': delegator,
                    # 'from_delegate': from_delegate, don't think we actually need this...
                }
            
            self.delegatee_latest_event[to_delegate] = {
                'block_number': block_number,
                'delegator': delegator,
                # 'from_delegate': from_delegate, don't think we actually need this...
            }

            if not to_delegate in self.delegatee_oldest:
                self.delegatee_oldest[to_delegate] = block_number

            self.delegatee_latest[to_delegate] = block_number

            if (from_delegate != '0x0000000000000000000000000000000000000000'):
                if from_delegate != to_delegate:
                    try:
                        if delegator in self.delegatee_list[from_delegate]:
                            del self.delegatee_list[from_delegate][delegator]                
                    except KeyError as e:
                        print(f"E251250520 - Problem removing delegator '{delegator}' this is unexpected. ({from_delegate=}, {to_delegate=})")

            self.delegatee_cnt[to_delegate] = len(self.delegatee_list[to_delegate])

        elif signature == DELEGATE_CHANGED_2:
            delegator = event['delegator'].lower()
            
            # Parse old and new delegations
            old_delegatees = self._parse_delegate_array(event.get('old_delegatees', '[]'))
            new_delegatees = self._parse_delegate_array(event.get('new_delegatees', '[]'))
            
            # Handle old delegations removal
            for old_delegation in old_delegatees:
                old_delegate = old_delegation[0].lower()
                amount = old_delegation[1]

                if old_delegate in self.delegatee_list:
                    if delegator in self.delegatee_list[old_delegate]:
                        del self.delegatee_list[old_delegate][delegator]
                        
                    cnt = len(self.delegatee_list[old_delegate])
                    if cnt == 0:
                        del self.delegatee_list[old_delegate]

                    self.delegation_amounts[old_delegate].pop(delegator, None)
                    
                    # Update voting power
                    self.delegatee_vp[old_delegate] -= amount

            # Handle new delegations addition
            for new_delegation in new_delegatees:
                to_delegate = new_delegation[0].lower()
                amount = new_delegation[1]
                
                if not self.delegatee_oldest_event.get(to_delegate):
                    self.delegatee_oldest_event[to_delegate] = {
                        'block_number': block_number,
                        'delegator': delegator
                    }
            
                self.delegatee_latest_event[to_delegate] = {
                    'block_number': block_number,
                    'delegator': delegator,
                }

                self.delegatee_list[to_delegate][delegator] = (block_number, transaction_index)
                self.delegatee_cnt[to_delegate] = len(self.delegatee_list[to_delegate])
                self.delegation_amounts[to_delegate][delegator] = amount
                
                # Update voting power
                self.delegatee_vp[to_delegate] += amount
                

        elif signature == DELEGATE_VOTES_CHANGE:

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

            recent_history = self.delegatee_vp_recent_history[delegatee]

            recent_history[block_number] = new_votes

            if len(recent_history) > 1:
                pos = recent_history.bisect_right(self.seven_day_block_number)
                pos = max(pos - 1, 0)
                prune_block = recent_history.keys()[pos]

                if pos != 0:
                    recent_history = SortedDict((key, recent_history[key]) for key in recent_history.irange(minimum=prune_block))
    
            self.delegatee_vp_recent_history[delegatee] = recent_history

    def get_seven_day_vp(self, delegatee):

        vp, block_number = self.cached_seven_day_vp[delegatee]

        if block_number == self.seven_day_block_number:
            return vp

        recent_history = self.delegatee_vp_recent_history[delegatee]

        if len(recent_history) == 0:
            return 0

        k = recent_history.bisect_left(self.seven_day_block_number) - 1

        vp = recent_history[recent_history.keys()[k]]

        self.cached_seven_day_vp[delegatee] = (vp, self.seven_day_block_number)

        return vp

    def delegate_seven_day_vp_change(self, delegatee):
        
        cur_vp = self.delegatee_vp[delegatee]
        old_vp = self.get_seven_day_vp(delegatee)
        delta = cur_vp - old_vp

        return delta 
        
                




LCREATED = len('ProposalCreated')
LQUEUED = len('ProposalQueued')
LEXECUTED = len('ProposalExecuted')
LCANCELED = len('ProposalCanceled')

def reverse_engineer_module(signature, proposal_data):

    if signature in (PROPOSAL_CREATED_3, PROPOSAL_CREATED_4):
        crit = '00000000000000000000000000000000000000000000000000000000000000c'
        if proposal_data.startswith(crit):
            return 'approval'
        else:
            return 'optimistic'

    elif signature in (PROPOSAL_CREATED_1, PROPOSAL_CREATED_2):
        return 'standard'
    else:
        raise Exception(f"Unrecognized signature '{signature}'")


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

    if proposal_data[:2] == '0x':
        proposal_data = proposal_data[2:]
    proposal_data = bytes.fromhex(proposal_data)

    if proposal_type == 'optimistic':
        abi = ["(uint248,bool)"]
        decoded = decode_abi(abi, proposal_data)
        return bytes_to_hex(decoded)
    
    if proposal_type == 'approval':
        abi = ["(uint256,address[],uint256[],bytes[],string)[]", "(uint8,uint8,address,uint128,uint128)"]
        abi2 = ["(address[],uint256[],bytes[],string)[]",        "(uint8,uint8,address,uint128,uint128)"] # OP/alligator only? Only for 0xe1a17f4770769f9d77ef56dd3b92500df161f3a1704ab99aec8ccf8653cae400l

        try:
            decoded = decode_abi(abi, proposal_data)
        except Exception as err:
            decoded = decode_abi(abi2, proposal_data)

        decoded = bytes_to_hex(decoded)
        
        return decoded

    raise Exception("Unknown Proposal Type: {}".format(proposal_type))


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
        self.voting_module_name = name
        self.create_event['voting_module_name'] = name
        
    def resolve_voting_module_name(self, modules):
        voting_module_name = modules.get(self.voting_module_address, "standard")
        self.set_voting_module_name(voting_module_name)
    
    def reverse_engineer_module_name(self, signature, proposal_data):
        voting_module_name = reverse_engineer_module(signature, proposal_data)
        self.set_voting_module_name(voting_module_name)

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

                proposal_data = proposal.create_event.get('proposal_data', None)

                if self.gov_spec['name'] == 'agora' and self.gov_spec['version'] > 1.1:
                    raise ToDo("Old Govenors are using newer PTCs, and so using gov version here doesn't work perfectly for this check.  So the first one that upgrades, is going to trip this reminder.  Plus, PTC upgrades happen without changing gov versions Eg. Optimism.")
                    proposal.resolve_voting_module_name(self.modules)
                elif self.gov_spec['name'] == 'agora':
                    # Older PTC Contracts didn't fully describe themselves, so we 
                    # this is a hack.
                    proposal.reverse_engineer_module_name(signature, proposal_data)
                else:
                    proposal.set_voting_module_name('standard')

                if self.gov_spec['name'] == 'agora':
                    
                    voting_module_name = proposal.voting_module_name # standard / approval / optimistic

                    if voting_module_name in ('approval', 'optimistic'):
                        proposal.create_event['decoded_proposal_data'] = decode_proposal_data(voting_module_name, proposal_data)                
                    
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
    
    def counted(self, head=-1):
        for proposal in reversed(self.proposals.values()):
            if not proposal.canceled:
                counted = 1 if (proposal.queued or proposal.executed) else 0
                if head > 0 or head <= -1:
                    yield proposal, counted
                    head -= counted


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
        
        self.latest_vote_block = defaultdict(int)

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
        event_cp['proposal_id'] = str(event_cp['proposal_id'])

        self.voter_history[event['voter']].append(event_cp)

        event_cp = copy(event_cp)
        del event_cp['proposal_id']

        self.proposal_vote_record[proposal_id].append(event_cp)
        
        voter = event['voter'].lower()
        block_number = int(event['block_number']) if isinstance(event['block_number'], str) else event['block_number']
        if block_number > self.latest_vote_block[voter]:
            self.latest_vote_block[voter] = block_number

class ParticipationModel:
    """
    This participation model looks back at the 10 most recent non-cancelled completed proposals, and any active proposals.

    We let T be the # of non-cancelleted completed proposals plus any active propsals.  
    
    T >= 10
    
    We let NC be the # of not yet completed proposals.

    NC <= T

    And...

    Therefore C = T - NC, ie any completed proposals.

    The model's numerator is the sum of any votes from all T.
    
    The model's denominator is the sum of any  checks to see if a specific delegate voted in any of those relevant proposals.

    The numerator of the participation rate is the count of T proposals, if the delegate has voted.

    The denominator of the participation rate is the count of C proposals, plus the count of any NC if the delegated has voted. 

    Such that, numerator >= denominator. 

    A logical enhancement from here, would be creating a DataProduct that calculated 
    this for all delegates at the time of the completion for any proposal.  
    
    This would move the calc from the endpoint to the data product step.
    """

    def __init__(self, proposals_dp : Proposals, votes_dp : Votes):
        self.proposals = proposals_dp
        self.votes = votes_dp

        self.relevant_and_active_proposals = [(proposal.create_event['id'], counted) for proposal, counted in self.proposals.counted(head=10)]
        self.tot_considered = -1 * len(self.relevant_and_active_proposals)
    
    def calculate(self, addr):

        historic_proposal_ids = [vote['proposal_id'] for vote in self.votes.voter_history[addr]]

        recent_proposal_ids = set(historic_proposal_ids[self.tot_considered:])

        den = 0
        num = 0

        for proposal_id, counted in self.relevant_and_active_proposals:
            if counted:
                den += 1
                if (proposal_id in recent_proposal_ids):
                    num += 1
            else:
                if (proposal_id in recent_proposal_ids):
                    num += 1
                    den += 1

        return num / den
