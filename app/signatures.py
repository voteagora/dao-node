TRANSFER = 'Transfer(address,address,uint256)'
DELEGATE_VOTES_CHANGE = 'DelegateVotesChanged(address,uint256,uint256)'
DELEGATE_CHANGED_1 = 'DelegateChanged(address,address,address)'
DELEGATE_CHANGED_2 = 'DelegateChanged(address,(address,uint96)[],(address,uint96)[])'

PROPOSAL_CREATED_1 = 'ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string)'
PROPOSAL_CREATED_2 = 'ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string,uint8)'

PROPOSAL_CREATED_3 = 'ProposalCreated(uint256,address,address,bytes,uint256,uint256,string)'
PROPOSAL_CREATED_4 = 'ProposalCreated(uint256,address,address,bytes,uint256,uint256,string,uint8)'

PROPOSAL_CANCELED = 'ProposalCanceled(uint256)'
PROPOSAL_QUEUED   = 'ProposalQueued(uint256,uint256)'
PROPOSAL_EXECUTED = 'ProposalExecuted(uint256)'

PROP_TYPE_SET_1 = 'ProposalTypeSet(uint8,uint16,uint16,string)'
PROP_TYPE_SET_2 = 'ProposalTypeSet(uint256,uint16,uint16,string)'
PROP_TYPE_SET_3 = 'ProposalTypeSet(uint8,uint16,uint16,string,string)'
PROP_TYPE_SET_4 = 'ProposalTypeSet(uint8,uint16,uint16,string,string,address)'

VOTE_CAST_1 = 'VoteCast(address,uint256,uint8,uint256,string)'
VOTE_CAST_WITH_PARAMS_1 = 'VoteCastWithParams(address,uint256,uint8,uint256,string,bytes)'

SCOPE_CREATED  = 'ScopeCreated(uint8,bytes24,bytes4,string)'
SCOPE_DELETED  = 'ScopeDeleted(uint8,bytes24)'
SCOPE_DISABLED = 'ScopeDisabled(uint8,bytes24)'

from .utils import camel_to_snake

INT_TYPES = [f"uint{i}" for i in range(8, 257, 8)]
INT_TYPES.append("uint")

class CSVClientCaster:
    
    def __init__(self, abis):
        self.abis = abis
    
    def lookup(self, signature):

        abi_frag = self.abis.get_by_signature(signature)

        def caster_maker():

            int_fields = [camel_to_snake(o['name']) for o in abi_frag.inputs if o['type'] in INT_TYPES]

            def caster_fn(event):
                for int_field in int_fields:
                    try:
                        event[int_field] = int(event[int_field])
                    except ValueError:
                        print(f"E182250323 - Problem with casting {int_field} to int, from file {fname}.")
                    except KeyError:
                        print(f"E184250323 - Problem with getting {int_field} from file {fname}.")
                return event

            return caster_fn
        
        if signature == TRANSFER:

            amount_field = abi_frag.fields[2]
            def caster_fn(event):
                event[amount_field] = int(event[amount_field])
                return event

            return caster_fn

        else:
            caster_fn = caster_maker()

        return caster_fn

        

        

if __name__ == '__main__':

    from web3 import Web3 as w3
    
    local_vars = list(locals().items())

    for var, val in local_vars:

        if isinstance(val, str) and "__" not in var:
            print("     " + var)

            print("0x" + w3.keccak(text=val).hex(), " -> ", val)

