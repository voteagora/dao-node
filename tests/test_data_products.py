import pytest
from app.data_products import Balances, Delegations, Proposals, Votes, ProposalTypes
from app.clients import CSVClient
import csv
import os
from abifsm import ABI, ABISet
from app.signatures import *

####################################
#
#  Test business logic of the Data Products, against specific edge cases and scenarios in the data.
#
#

def test_Balances_from_dict():

    balances = Balances(token_spec={'name' : 'erc20', 'version' : '?'})

    data = [
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 0,  'from': '0x0000000000000000000000000000000000000000', 'to': '0xcfbcda93bee60e1f4865783e141b1dd913d219df', 'value': 1000000000000000,   'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 2,  'from': '0x0000000000000000000000000000000000000000', 'to': '0xe368d397eae44f1f7f6b922877564d4e592d63b8', 'value': 1000000000000000,   'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 4,  'from': '0x0000000000000000000000000000000000000000', 'to': '0x465c63680f2a0b4277d9b4cecc3f3310e531a77f', 'value': 1968807210960000,   'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 6,  'from': '0x0000000000000000000000000000000000000000', 'to': '0xb054902bc6260f3e733b78adf736b17783158953', 'value': 3000000000000000,   'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 8,  'from': '0x0000000000000000000000000000000000000000', 'to': '0xb86faa020274ae3fc3a883293f041f23793f698e', 'value': 36722878065972185,  'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 10, 'from': '0x0000000000000000000000000000000000000000', 'to': '0xfdbf50bfc69a2d6d400ae6e4d18624a534a6980f', 'value': 82871585988544595,  'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 12, 'from': '0x0000000000000000000000000000000000000000', 'to': '0x0b0df332d1126851f5fb9394e4d8aaae714833cf', 'value': 100000000000000000, 'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 14, 'from': '0x0000000000000000000000000000000000000000', 'to': '0x16a3c50f1ec275335cf2feaf96738de54c6ae9a2', 'value': 100000000000000000, 'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456013, 'transaction_index': 1, 'log_index': 16, 'from': '0x0000000000000000000000000000000000000000', 'to': '0x2643c742ce701a8ab2394c0debfdac0d6cbb3010', 'value': 100000000000000000, 'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'},
             {'block_number': 2456014, 'transaction_index': 2, 'log_index': 1,  'from': '0x0000000000000000000000000000000000000000', 'to': '0x0b0df332d1126851f5fb9394e4d8aaae714833cf', 'value': 100000000000000000, 'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'}, # Fudged record for testing purposes.
             {'block_number': 2456015, 'transaction_index': 2, 'log_index': 1,  'from': '0x0b0df332d1126851f5fb9394e4d8aaae714833cf', 'to': '0x2643c742ce701a8ab2394c0debfdac0d6cbb3010', 'value': 50000000000000000,  'signature': 'Transfer(address,address,uint256)', 'sighash': 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'}   # Fudged record for testing purposes.
           ] 

    for record in data:
        balances.handle(record)

    assert balances.balance_of('0xfdbf50bfc69a2d6d400ae6e4d18624a534a6980f') == 82871585988544595
    assert balances.balance_of('0x0b0df332d1126851f5fb9394e4d8aaae714833cf') == 150000000000000000

def test_Delegations_from_dict():

    delegations = Delegations()

    data = [
            {'block_number': 79335962, 'transaction_index': 0, 'log_index': 0, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x0000000000000000000000000000000000000000', 'to_delegate': '0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 92356698, 'transaction_index': 0, 'log_index': 0, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5', 'to_delegate': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 95086878, 'transaction_index': 0, 'log_index': 0, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'to_delegate': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 111126198, 'transaction_index': 6, 'log_index': 160, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'to_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 115182714, 'transaction_index': 8, 'log_index': 7, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'to_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 115988830, 'transaction_index': 13, 'log_index': 161, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'to_delegate': '0x3eee61b92c36e97be6319bf9096a1ac3c04a1466', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'}, # This row is fudged, because Center was missing events at time of test creation.
            {'block_number': 123076144, 'transaction_index': 7, 'log_index': 29, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x3eee61b92c36e97be6319bf9096a1ac3c04a1466', 'to_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 126484128, 'transaction_index': 21, 'log_index': 79, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'to_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'}
            ] 

    for record in data:
        delegations.handle(record)

    assert delegations.delegator['0xded7e867cc42114f1cffa1c5572f591e8711771d'] == '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0'
    assert delegations.delegatee_cnt['0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0'] == 1
    assert delegations.delegatee_list['0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0'][0] == '0xded7e867cc42114f1cffa1c5572f591e8711771d'

def test_Delegations_with_vote_events():
    
    delegations = Delegations()

    delegation_events = [
        {'block_number': 79335962, 'transaction_index': 0, 'log_index': 0, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x0000000000000000000000000000000000000000', 'to_delegate': '0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
        {'block_number': 92356698, 'transaction_index': 0, 'log_index': 0, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5', 'to_delegate': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
    ]

    for event in delegation_events:
        delegations.handle(event)

    assert delegations.delegator['0xded7e867cc42114f1cffa1c5572f591e8711771d'] == '0xded7e867cc42114f1cffa1c5572f591e8711771d'
    
    # Create a Votes data product to test vote tracking
    votes = Votes(governor_spec={'name': 'compound'})
    
    vote_events = [
        {'block_number': 100000000, 'transaction_index': 5, 'log_index': 10, 'voter': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'proposal_id': '42', 'support': 1, 'votes': 100, 'reason': '', 'signature': 'VoteCast(address,uint256,uint8,uint256,string)', 'sighash': '8bd10c2c5c6c2693aef5a24259d241d27c33b5c753d92f752137b77ba70c198a'},
        {'block_number': 100500000, 'transaction_index': 3, 'log_index': 7, 'voter': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'proposal_id': '43', 'support': 0, 'votes': 100, 'reason': '', 'signature': 'VoteCast(address,uint256,uint8,uint256,string)', 'sighash': '8bd10c2c5c6c2693aef5a24259d241d27c33b5c753d92f752137b77ba70c198a'},
        {'block_number': 100200000, 'transaction_index': 2, 'log_index': 5, 'voter': '0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5', 'proposal_id': '42', 'support': 1, 'votes': 200, 'reason': '', 'signature': 'VoteCast(address,uint256,uint8,uint256,string)', 'sighash': '8bd10c2c5c6c2693aef5a24259d241d27c33b5c753d92f752137b77ba70c198a'},
        {'block_number': 100300000, 'transaction_index': 1, 'log_index': 3, 'voter': '0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5', 'proposal_id': '43', 'support': 0, 'votes': 200, 'params': '', 'signature': 'VoteCastWithParams(address,uint256,uint8,uint256,string,bytes)', 'sighash': '8c587a7e1b8e1d28b2139c4e9c8a2b7ded5f0b53bc3bf92266f609f7df943628'},
    ]

    for event in vote_events:
        votes.handle(event)
    
    # Test direct access to latest_vote_block instead of using get_last_vote_block
    assert votes.latest_vote_block['0xded7e867cc42114f1cffa1c5572f591e8711771d'] == 100500000
    assert votes.latest_vote_block['0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5'] == 100300000
    assert votes.latest_vote_block['0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0'] == 0
    
    # Out of order vote event
    earlier_vote_event = {
        'block_number': 99000000, 
        'transaction_index': 1, 
        'log_index': 3, 
        'voter': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 
        'proposal_id': '41', 
        'support': 1, 
        'votes': 100, 
        'reason': '', 
        'signature': 'VoteCast(address,uint256,uint8,uint256,string)', 
        'sighash': '8bd10c2c5c6c2693aef5a24259d241d27c33b5c753d92f752137b77ba70c198a'
    }
    
    votes.handle(earlier_vote_event)
    
    # Last vote block should still be the highest block number
    assert votes.latest_vote_block['0xded7e867cc42114f1cffa1c5572f591e8711771d'] == 100500000

####################################
#
#  Test basic business logic of the data products, in the context of a specific Client and production like data.
#

def test_Proposals_for_compound_governor_from_csv(compound_governor_abis):
    
    proposals = Proposals(governor_spec={'name': 'compound'})
        
    csvc = CSVClient('tests/data/1000-all-uniswap-to-PID83')
    chain_id = 1
    for row in csvc.read(chain_id, '0x408ed6354d4973f66138c91495f2f2fcbd8724c3', 'ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string)', compound_governor_abis):
        proposals.handle(row)
    
    # Get the first proposal from the data product
    first_proposal = next(proposals.unfiltered())
    
    # Basic assertions to verify the data was loaded
    assert first_proposal is not None
    assert isinstance(first_proposal.create_event['id'], str)  # IDs are stored as strings
    assert 'description' in first_proposal.create_event

    assert 'targets' in first_proposal.create_event
    assert first_proposal.create_event['targets'][0] == '0x1f9840a85d5af5bf1d1762f925bdaddc4201f984'
    
    assert 'values' in first_proposal.create_event
    assert first_proposal.create_event['values'][0] == 0
    
    assert 'calldatas' in first_proposal.create_event
    assert first_proposal.create_event['calldatas'][0] == 'a9059cbb0000000000000000000000005069a64bc6616dec1584ee0500b7813a9b680f7e00000000000000000000000000000000000000000010cf035cc2441ead340000'
    
    assert 'signatures' in first_proposal.create_event
    assert first_proposal.create_event['signatures'][0] == ''
    
    assert 'start_block' in first_proposal.create_event
    assert first_proposal.create_event['start_block'] == 22039575

    assert 'end_block' in first_proposal.create_event
    assert first_proposal.create_event['end_block'] == 22079895

    assert len(list(proposals.unfiltered())) == 83

    assert proposals.proposals['83'].create_event['id'] == '83'
    assert proposals.proposals['1'].create_event['id'] == '1'

def test_Proposals_one_op_approval_from_csv(op_governor_abis):
    
    modules = {'0x8060b18290f48fc0bf2149eeb2f3c280bde7674f': 'approval'}
    gov_spec = {'name': 'agora', 'version': 0.1}

    proposals = Proposals(gov_spec, modules)
        
    csvc = CSVClient('tests/data/3000-op-approval-PID31049')
    chain_id = 10
    for row in csvc.read(chain_id, '0xcdf27f107725988f2261ce2256bdfcde8b382b10', 'ProposalCreated(uint256,address,address,bytes,uint256,uint256,string,uint8)', op_governor_abis):
        proposals.handle(row)
    
    # Get the first proposal from the data product
    first_proposal = next(proposals.unfiltered())
    
    # Basic assertions to verify the data was loaded
    assert first_proposal is not None
    assert isinstance(first_proposal.create_event['id'], str)  # IDs are stored as strings

    assert 'description' in first_proposal.create_event

    del first_proposal.create_event['description']

    assert first_proposal.create_event['proposal_type'] == 3
    assert first_proposal.create_event['voting_module_name'] == 'approval'

    assert first_proposal.create_event['decoded_proposal_data'] == (((0, (), (), (), 'World Foundation'), (0, (), (), (), 'Andrey Petrov'), (0, (), (), (), 'OP Labs'), (0, (), (), (), 'L2BEAT'), (0, (), (), (), 'Alchemy'), (0, (), (), (), 'Maggie Love'), (0, (), (), (), 'Gauntlet'), (0, (), (), (), 'Test in Prod'), (0, (), (), (), 'Yoav Weiss'), (0, (), (), (), 'ml_sudo'), (0, (), (), (), 'Kris Kaczor'), (0, (), (), (), 'Martin Tellechea'), (0, (), (), (), 'Ink'), (0, (), (), (), 'Coinbase'), (0, (), (), (), 'troy')), (15, 1, '0x0000000000000000000000000000000000000000', 6, 0))

def test_Votes_one_op_approval_from_csv(op_governor_abis):
    
    gov_spec = {'name': 'agora', 'version': 0.1}

    votes = Votes(gov_spec)
        
    csvc = CSVClient('tests/data/3000-op-approval-PID31049')
    chain_id = 10
    for row in csvc.read(chain_id, '0xcdf27f107725988f2261ce2256bdfcde8b382b10', 'VoteCast(address,uint256,uint8,uint256,string)', op_governor_abis):
        votes.handle(row)

    for row in csvc.read(chain_id, '0xcdf27f107725988f2261ce2256bdfcde8b382b10', 'VoteCastWithParams(address,uint256,uint8,uint256,string,bytes)', op_governor_abis):
       votes.handle(row)

    aggregations = votes.proposal_aggregations['31049359136632781771607732021569520613741907517136820917236339424553298132866']
     
    assert aggregations.result['no-param'][2] == 1087007682453656513592020
    assert aggregations.result[0][1] == 32371488932049684561146389
    assert aggregations.result[14][1] == 10504614280025813357834598

    assert len(aggregations.result) == 16

def test_ProposalTypes_proposal_type_set_with_one_scope_created(pguild_ptc_abi):

    pt = ProposalTypes()

    csvc = CSVClient('tests/data/4000-pguild-ptc-w-scopes')
    chain_id = 1115511

    for row in csvc.read(chain_id, '0xb7687e62d6b2cafb3ed3c3c81b0b6cf0a3884602', PROP_TYPE_SET_4, pguild_ptc_abi):
        pt.handle(row)
    for row in csvc.read(chain_id, '0xb7687e62d6b2cafb3ed3c3c81b0b6cf0a3884602', SCOPE_CREATED, pguild_ptc_abi):
        pt.handle(row)
    
    assert len(pt.proposal_types) == 3
    assert pt.proposal_types[2]['quorum'] == 3300
    assert pt.proposal_types[2]['approval_threshold'] == 5100

    assert pt.proposal_types[1]['scopes'][0]['description'] == 'Distribute splits contract'
