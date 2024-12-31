
from collections import defaultdict
from abc import ABC, abstractmethod

from utils import camel_to_snake
from copy import copy

class DataProduct(ABC):

    @abstractmethod
    def handle(self, event):
        pass

    @property
    def name(self):
        return camel_to_snake(self.__class__.__name__)
    

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

        if signature == 'DelegateChanged(address,address,address)':

            delegator = event['delegator']

            to_delegate = event['to_delegate']
            self.delegator[delegator] = to_delegate
            
            self.delegatee_list[to_delegate].append(delegator)

            from_delegate = event['from_delegate']

            if from_delegate != '0x0000000000000000000000000000000000000000':
                self.delegatee_list[from_delegate].remove(delegator)

            self.delegatee_cnt[to_delegate] = len(self.delegatee_list[to_delegate])

        else: # DelegateVotesChanged(address,uint256,uint256)
            new_votes = int(event['new_votes'])
            previous_votes = int(event['previous_votes'])

            self.voting_power += (new_votes - previous_votes)
            self.delegatee_vp[event['delegate']] = new_votes

            block_number = int(event['block_number'])

            self.delegatee_vp_history[event['delegate']].append((block_number, new_votes))


LCREATED = len('ProposalCreated')
LQUEUED = len('ProposalQueued')
LEXECUTED = len('ProposalExecuted')
LCANCELED = len('ProposalCanceled')

class Proposal:
    def __init__(self, creation_event):
        self.create_event = creation_event
        self.canceled = False
        self.queued = False
        self.executed = False
    
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

class Proposals(DataProduct):

    def __init__(self):
        self.proposals = {}
    
    def handle(self, event):

        signature = event['signature']

        # TODO - should we be working with proposal_ids as numerical or strings? 
        #        For now, we store as strings.

        proposal_id = str(event['proposal_id'])
        event['proposal_id'] = proposal_id

        del event['signature']
        del event['sighash']

        if 'ProposalCreated' == signature[:LCREATED]:
            self.proposals[proposal_id] = Proposal(event)

        elif 'ProposalQueued' == signature[:LQUEUED]:
            self.proposals[proposal_id].queue(event)
        
        elif 'ProposalExecuted' == signature[:LEXECUTED]:
            self.proposals[proposal_id].execute(event)

        elif 'ProposalCanceled' == signature[:LCANCELED]:
            self.proposals[proposal_id].cancel(event)
    
    def unfiltered(self):
        for proposal_id, proposal in self.proposals.items():
            yield proposal.to_dict()

    def active(self):
        for proposal_id, proposal in self.proposals.items():
            if not proposal.canceled and not proposal.queued and not proposal.executed:
                yield proposal.to_dict()

def nested_default_dict():
    return defaultdict(int)

class Votes(DataProduct):
    def __init__(self):
        self.proposal_aggregation = defaultdict(nested_default_dict)
        self.voter_history = defaultdict(list)
        self.proposal_vote_record = defaultdict(list)
    
    def handle(self, event):

        proposal_id = str(event['proposal_id'])
        weight = event['weight']

        self.proposal_aggregation[proposal_id][event['support']] += weight

        event_cp = copy(event)

        del event_cp['sighash']
        del event_cp['signature']

        self.voter_history[event['voter']].append(event_cp)

        event_cp = copy(event_cp)
        del event_cp['proposal_id']

        self.proposal_vote_record[proposal_id].append(event_cp)
