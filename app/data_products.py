from copy import copy
from collections import defaultdict
from sortedcontainers import SortedDict
from abc import ABC, abstractmethod
import json, time
from bisect import bisect_left
from copy import deepcopy

from eth_abi.abi import decode as decode_abi

from .signatures import *
from .abcs import DataProduct

class ToDo(NotImplementedError):
    pass

class Balances(DataProduct):

    def __init__(self, token_spec):
        self.balances = defaultdict(int)

        self.erc20 = token_spec['name'] == 'erc20'
        self.erc721 = token_spec['name'] == 'erc721'

        if self.erc20:            
            if token_spec['version'] == 'U':
                self.value_field_name = 'amount'
            else: 
                self.value_field_name = 'value'
        
        elif self.erc721:
            self.handle = self.handle_erc721

    def handle_erc721(self, event):

        self.balances[event['from']] -= 1
        self.balances[event['to']] += 1

    def handle(self, event): # ERC20

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

        self.delegator_delegate = defaultdict(set)

        self.timestamp_to_block = SortedDict()
        self.delegatee_vp_recent_history = defaultdict(SortedDict)

        self.current_block_number = 0
        self.current_ts = 0
        self.current_rounded_ts = 0

        self.seven_day_block_number = 0
        self.seven_day_ts = 0

        self.cached_seven_day_vp = defaultdict(lambda: (0, 0))
        
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

        if 'timestamp' in event:
            self.handle_block(event)
            return

        signature = event['signature']
        block_number = event['block_number']
        transaction_index = event['transaction_index']
        
        ZERO_ADDRESS = '0x0000000000000000000000000000000000000000'

        if signature == DELEGATE_CHANGED_1:

            delegator = event['delegator'].lower()
            to_delegate = event['to_delegate'].lower()
            from_delegate = event['from_delegate'].lower()

            # If this is an undelegation (to_delegate is zero address)
            if to_delegate == ZERO_ADDRESS:
                # Remove delegator from delegator_delegate if present
                if delegator in self.delegator_delegate:
                    del self.delegator_delegate[delegator]
            else:
                # If not undelegation, update delegator_delegate
                self.delegator_delegate[delegator] = {to_delegate}
                # Only update delegatee_list if not zero address
                self.delegatee_list[to_delegate][delegator] = (block_number, transaction_index)

                if not self.delegatee_oldest_event.get(to_delegate):
                    self.delegatee_oldest_event[to_delegate] = {
                        'block_number': block_number,
                        'delegator': delegator,
                    }
                self.delegatee_latest_event[to_delegate] = {
                    'block_number': block_number,
                    'delegator': delegator,
                }
                if not to_delegate in self.delegatee_oldest:
                    self.delegatee_oldest[to_delegate] = block_number
                self.delegatee_latest[to_delegate] = block_number
                self.delegatee_cnt[to_delegate] = len(self.delegatee_list[to_delegate])

            # Remove delegator from previous delegatee if needed
            if (from_delegate != ZERO_ADDRESS):
                if from_delegate != to_delegate:
                    try:
                        if delegator in self.delegatee_list[from_delegate]:
                            del self.delegatee_list[from_delegate][delegator]
                            # If previous delegatee has no more delegators, remove the key
                            if len(self.delegatee_list[from_delegate]) == 0:
                                del self.delegatee_list[from_delegate]
                                if from_delegate in self.delegatee_cnt:
                                    del self.delegatee_cnt[from_delegate]
                    except KeyError as e:
                        print(f"E251250520 - Problem removing delegator '{delegator}' this is unexpected. ({from_delegate=}, {to_delegate=})")

        elif signature == DELEGATE_CHANGED_2:
            delegator = event['delegator'].lower()
            
            # Parse old and new delegations
            old_delegatees = event.get('old_delegatees')
            new_delegatees = event.get('new_delegatees')
            
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
                
                if delegator in self.delegator_delegate:
                    self.delegator_delegate[delegator].discard(old_delegate)
                    if not self.delegator_delegate[delegator]:
                        del self.delegator_delegate[delegator]

            # Handle new delegations addition
            for new_delegation in new_delegatees:
                to_delegate = new_delegation[0].lower()
                amount = new_delegation[1]
                
                if to_delegate != ZERO_ADDRESS:  
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
                    self.delegatee_vp[to_delegate] += amount
                    self.delegator_delegate[delegator].add(to_delegate)

        elif signature == DELEGATE_VOTES_CHANGE:

            delegatee = event['delegate'].lower()
            
            # Skip processing if delegatee is zero address
            if delegatee == ZERO_ADDRESS:
                return

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

   
   
   
   
    def delegatee_vp_at_block(self, addr, block_number, include_history=False):
        vp_history = [(0, 0)] + self.delegatee_vp_history[addr]
        index = bisect_left(vp_history, (block_number,)) - 1

        index = max(index, 0)

        try:
            vp = vp_history[index][1]
        except:
            vp = 0

        if include_history:
            return vp, vp_history[index:]
        else:
            return vp

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

        assert isinstance(self.create_event['description'], str)
        # self.create_event['description'] = str(self.create_event['description']) # <- not sure if we really need this, but it was in the old code.

        self.canceled = False
        self.queued = False
        self.executed = False

        self.result = defaultdict(nested_default_dict)

    def get_proposal_type(self, proposal_types):
        prop_type_id = self.create_event['proposal_type']
        out = deepcopy(proposal_types.get(prop_type_id))
        out['id'] = prop_type_id
        return out
    
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

class ParticipationRateStateTracker:

    def __init__(self):
        self.ending_in_future_proposals_valid_until_block = float('inf')

        self.flag_ending_in_future_proposals_has_changed = False
        self.ending_in_future_proposals = [] # list of (proposal_id, block_number)

        self.flag_recently_completed_and_counted_has_changed = False
        self.recently_completed_and_counted_proposals = [] # list of (proposal_id, block_number)

        self.MAX_RECENTLY_COMPLETED_AND_COUNTED = 10

    def track_new_proposal(self, proposal_id, start_block, end_block):
        # print(f"track_new_proposal({proposal_id=}, {start_block=}, {end_block=})")
        self.ending_in_future_proposals.append((proposal_id, start_block, end_block))
        self.ending_in_future_proposals_valid_until_block = min(self.ending_in_future_proposals_valid_until_block, end_block)
        self.flag_ending_in_future_proposals_has_changed = True

    def update_recently_completed_and_counted_proposal(self, proposal_ids_and_block_number: list[tuple[str, int]]):
        #print(f"update_recently_completed_and_counted_proposal({proposal_ids_and_block_number=})")
        proposal_ids_and_block_number.sort(key=lambda x: x[1])

        if len(proposal_ids_and_block_number) > self.MAX_RECENTLY_COMPLETED_AND_COUNTED:
            proposal_ids_and_block_number = proposal_ids_and_block_number[-self.MAX_RECENTLY_COMPLETED_AND_COUNTED:]
        
        change_detected = self.recently_completed_and_counted_proposals != proposal_ids_and_block_number
        self.flag_recently_completed_and_counted_has_changed = change_detected or self.flag_recently_completed_and_counted_has_changed
        self.recently_completed_and_counted_proposals = proposal_ids_and_block_number

    def append_recently_completed_and_counted_proposal(self, proposal_ids_and_block_number: list[tuple[str, int]]):
        # print(f"append_recently_completed_and_counted_proposal({proposal_ids_and_block_number=})")
        both = list(set(self.recently_completed_and_counted_proposals + proposal_ids_and_block_number))
        both.sort(key=lambda x: x[1])
        if len(both) > self.MAX_RECENTLY_COMPLETED_AND_COUNTED:
            both = both[-self.MAX_RECENTLY_COMPLETED_AND_COUNTED:]
    
        change_detected = self.recently_completed_and_counted_proposals != both
        self.flag_recently_completed_and_counted_has_changed = change_detected or self.flag_recently_completed_and_counted_has_changed
        self.recently_completed_and_counted_proposals = both

    
    def roll_ending_in_future_to_recently_completed_and_counted(self, block_number):
        # print(f"roll_ending_in_future_to_recently_completed_and_counted({block_number=})")

        if block_number > self.ending_in_future_proposals_valid_until_block:

            new_ending_in_future_proposals = [proposal for proposal in self.ending_in_future_proposals if proposal[1] > block_number]
            newly_recently_completed_proposals = [proposal for proposal in self.ending_in_future_proposals if proposal[1] <= block_number]
            
            if len(new_ending_in_future_proposals):
                self.ending_in_future_proposals_valid_until_block = min([proposal[1] for proposal in new_ending_in_future_proposals])
                self.flag_ending_in_future_proposals_has_changed = True
            else:
                # There are no more future proposals at the moment
                self.ending_in_future_proposals_valid_until_block = float('inf')
            
            self.ending_in_future_proposals = new_ending_in_future_proposals
            
            self.append_recently_completed_and_counted_proposal(newly_recently_completed_proposals)
    
    def drop_cancelled_from_future_proposals(self, proposal_id):
        self.ending_in_future_proposals = [proposal for proposal in self.ending_in_future_proposals if proposal[0] != proposal_id]
        self.flag_ending_in_future_proposals_has_changed = True
    
    def check_integrity(self):

        completed_proposal_ids = set([proposal_id for proposal_id, _, _ in self.recently_completed_and_counted_proposals])
        future_proposal_ids = set([proposal_id for proposal_id, _, _ in self.ending_in_future_proposals])

        assert completed_proposal_ids.isdisjoint(future_proposal_ids), "Proposal IDs in both recently completed and future proposals"

        assert len(completed_proposal_ids) <= self.MAX_RECENTLY_COMPLETED_AND_COUNTED, "Too many recently completed proposals"


class Proposals(DataProduct):

    def __init__(self, governor_spec, modules=None):
        self.proposals = {}

        self.block_number = 0

        if modules:
            self.modules = modules
        else:
            self.modules = {}

        if governor_spec['name'] == 'compound':
            self.proposal_id_field_name = 'id'
        else:
            self.proposal_id_field_name = 'proposal_id'

        self.gov_spec = governor_spec

        self.prst = ParticipationRateStateTracker()
    
    
    def handle(self, event):

        # handle_block() # equivalent, without the extra lookup.
        if 'timestamp' in event:
            block_number = event['block_number']
            self.block_number = block_number
            self.prst.roll_ending_in_future_to_recently_completed_and_counted(block_number)
            return
        
        self.block_number = int(event['block_number'])
        self.prst.roll_ending_in_future_to_recently_completed_and_counted(self.block_number)

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
                proposal = Proposal(event)

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

                if proposal.voting_module_name != 'optimistic':
                    self.prst.track_new_proposal(proposal_id, proposal.create_event['start_block'], proposal.create_event['end_block'])

            elif 'ProposalQueued' == signature[:LQUEUED]:
                self.proposals[proposal_id].queue(event)

            elif 'ProposalExecuted' == signature[:LEXECUTED]:
                self.proposals[proposal_id].execute(event)

            elif 'ProposalCanceled' == signature[:LCANCELED]:
                self.proposals[proposal_id].cancel(event)
                self.prst.drop_cancelled_from_future_proposals(proposal_id)
                self.restate_recently_completed_and_counted_proposals()

        except KeyError as e:
            print(f"E248250323 - Problem with the following proposal_id {proposal_id} and the {signature} event: {e}")
            raise
    
    def restate_recently_completed_and_counted_proposals(self):
        fresh_completed_proposals = []
        
        for proposal_id, proposal in self.proposals.items():
            end_block = proposal.create_event['end_block']
            start_block = proposal.create_event['start_block']
            if (end_block < self.block_number) and (not proposal.canceled) and (not proposal.voting_module_name == 'optimistic'):
                fresh_completed_proposals.append((proposal_id, start_block, end_block))
        self.prst.update_recently_completed_and_counted_proposal(fresh_completed_proposals)
    
        self.prst.check_integrity()

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

def check_weight_and_votes_are_int(event):
    if "weight" in event:
        return isinstance(event['weight'], int)
    if 'votes' in event:
        return isinstance(event['votes'], int)
    else:
        raise Exception(f"weight or votes is missing from event: {event}")

class Votes(DataProduct):
    def __init__(self, governor_spec):
        self.proposal_aggregations = defaultdict(VoteAggregation)

        self.voter_history = defaultdict(list)
        self.proposal_vote_record = defaultdict(list)
        
        self.latest_vote_block = defaultdict(int)

        self.participated = defaultdict(lambda : defaultdict(lambda : False))

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

        if "reason" in event_cp:
            if event_cp['reason'] == "":
                del event_cp['reason']

        # This is basically just a RAM & I/O optimization.
        event_cp['bn'] = event_cp['block_number']
        del event_cp['block_number']
        event_cp['tid'] = event_cp['transaction_index']
        del event_cp['transaction_index']
        event_cp['lid'] = event_cp['log_index']
        del event_cp['log_index']

        event_cp['proposal_id'] = str(event_cp['proposal_id'])

        assert check_weight_and_votes_are_int(event_cp)
        voter = event['voter']
        
        self.voter_history[voter].append(event_cp)

        self.participated[voter][proposal_id] = True

        event_cp = copy(event_cp)
        del event_cp['proposal_id']

        self.proposal_vote_record[proposal_id].append(event_cp)
        
        voter = event['voter'].lower()
        block_number = int(event['block_number']) if isinstance(event['block_number'], str) else event['block_number']
        if block_number > self.latest_vote_block[voter]:
            self.latest_vote_block[voter] = block_number