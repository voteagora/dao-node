from collections import defaultdict
from abc import ABC, abstractmethod

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

    def __init__(self, governor_spec):
        self.proposals = {}

        if governor_spec['name'] == 'compound':
            self.proposal_id_field_name = 'id'
        else:
            self.proposal_id_field_name = 'proposal_id'
    
    def handle(self, event):

        try:
            signature = event['signature']
        except:
            print(f"E187250323 Problem getting signature from event: {event}.")

        # TODO - should we be working with proposal_ids as numerical or strings? 
        #        For now, we store as strings.

        PROPOSAL_ID_FIELD = self.proposal_id_field_name

        proposal_id = str(event[PROPOSAL_ID_FIELD])
        event[PROPOSAL_ID_FIELD] = proposal_id

        del event['signature']
        del event['sighash']

        try:
            if 'ProposalCreated' == signature[:LCREATED]:
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


                self.proposals[proposal_id] = Proposal(event)

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

class Votes(DataProduct):
    def __init__(self, governor_spec):
        self.proposal_aggregation = defaultdict(nested_default_dict)
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

        weight = int(event.get('weight', 0))
        votes = int(event.get('votes', 0))

        self.proposal_aggregation[proposal_id][event['support']] += weight + votes

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

        self.proposal_id_field_name = self.proposals.proposal_id_field_name

        self.relevant_proposals = [int(p.create_event[self.proposal_id_field_name]) for p in self.proposals.completed(head=10)]
    
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
