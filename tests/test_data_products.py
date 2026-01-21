import pytest
from app.data_products import Balances, Delegations, NonIVotesVP, Proposals, Votes, ProposalTypes, Proposal, VoteAggregation
from app.clients_csv import CSVClient
import csv
import os
import heapq
import time
from collections import Counter
from abifsm import ABI, ABISet
from app.signatures import *
import glob

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


    assert delegations.delegator_delegate['0xded7e867cc42114f1cffa1c5572f591e8711771d'] == {'0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0'}
    assert delegations.delegatee_cnt['0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0'] == 1
    assert delegations.delegatee_list['0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0']['0xded7e867cc42114f1cffa1c5572f591e8711771d'] == (126484128, 21)

def test_Delegations_with_vote_events():
    
    delegations = Delegations()

    delegation_events = [
        {'block_number': 79335962, 'transaction_index': 0, 'log_index': 0, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x0000000000000000000000000000000000000000', 'to_delegate': '0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
        {'block_number': 92356698, 'transaction_index': 0, 'log_index': 0, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5', 'to_delegate': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
    ]

    for event in delegation_events:
        delegations.handle(event)

    assert delegations.delegator_delegate['0xded7e867cc42114f1cffa1c5572f591e8711771d'] == {'0xded7e867cc42114f1cffa1c5572f591e8711771d'}
    
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

def test_Delegations_last_event():
    delegations = Delegations()

    data = [
            {'block_number': 79335962, 'transaction_index': 0, 'log_index': 0, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x0000000000000000000000000000000000000000', 'to_delegate': '0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 92356698, 'transaction_index': 0, 'log_index': 0, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x75536cf4f01c2bfa528f5c74ddc1232db3af3ee5', 'to_delegate': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 95086878, 'transaction_index': 0, 'log_index': 0, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'to_delegate': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 111126198, 'transaction_index': 6, 'log_index': 160, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'to_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 115182714, 'transaction_index': 8, 'log_index': 7, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'to_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 115988830, 'transaction_index': 13, 'log_index': 161, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'to_delegate': '0x3eee61b92c36e97be6319bf9096a1ac3c04a1466', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 123076144, 'transaction_index': 7, 'log_index': 29, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x3eee61b92c36e97be6319bf9096a1ac3c04a1466', 'to_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            {'block_number': 126484128, 'transaction_index': 21, 'log_index': 79, 'delegator': '0xded7e867cc42114f1cffa1c5572f591e8711771d', 'from_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'to_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'},
            # Add a new delegation event for a different delegator
            {'block_number': 130000000, 'transaction_index': 5, 'log_index': 10, 'delegator': '0xabc7e867cc42114f1cffa1c5572f591e8711123e', 'from_delegate': '0x0000000000000000000000000000000000000000', 'to_delegate': '0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0', 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f'}
            ] 

    for record in data:
        delegations.handle(record)

    latest_event = delegations.delegatee_latest.get('0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0')
    
    # Unpack the tuple values
    block_number = latest_event
    assert block_number == 130000000
    
    oldest_event = delegations.delegatee_oldest.get('0x7b0befc5b043148cd7bd5cfeeef7bc63d28edec0')
    block_number = oldest_event
    assert block_number == 111126198
    
    latest_event = delegations.delegatee_oldest.get('0x3eee61b92c36e97be6319bf9096a1ac3c04a1466')
    block_number = latest_event
    assert block_number == 115988830
    
    oldest_event = delegations.delegatee_oldest.get('0x3eee61b92c36e97be6319bf9096a1ac3c04a1466')
    block_number = oldest_event
    assert block_number == 115988830
    
    assert delegations.delegatee_oldest.get('0x1111111111111111111111111111111111111111') is None
    assert delegations.delegatee_latest.get('0x1111111111111111111111111111111111111111') is None

def test_Delegations_partial_delegations():
    delegations = Delegations()

    event = {
        'block_number': 123456,
        'transaction_index': 0,
        'delegator': '0x1234567890123456789012345678901234567890',
        'old_delegatees': [],
        'new_delegatees': [["0xabcdef1234567890123456789012345678901234", 5000], ["0x9876543210987654321098765432109876543210", 7500]],
        'signature': DELEGATE_CHANGED_2,
        'sighash': 'test'
    }
    delegations.handle(event)

    assert '0xabcdef1234567890123456789012345678901234' in delegations.delegatee_list
    assert '0x9876543210987654321098765432109876543210' in delegations.delegatee_list
    assert delegations.delegation_amounts['0xabcdef1234567890123456789012345678901234']['0x1234567890123456789012345678901234567890'] == 5000
    assert delegations.delegation_amounts['0x9876543210987654321098765432109876543210']['0x1234567890123456789012345678901234567890'] == 7500

    event = {
        'block_number': 123457,
        'transaction_index': 0,
        'delegator': '0x1234567890123456789012345678901234567890',
        'old_delegatees': [["0xabcdef1234567890123456789012345678901234", 5000], ["0x9876543210987654321098765432109876543210", 7500]],
        'new_delegatees': [["0xabcdef1234567890123456789012345678901234", 10000]],
        'signature': DELEGATE_CHANGED_2,
        'sighash': 'test'
    }
    delegations.handle(event)

    assert '0x9876543210987654321098765432109876543210' not in delegations.delegatee_list
    assert delegations.delegation_amounts['0xabcdef1234567890123456789012345678901234']['0x1234567890123456789012345678901234567890'] == 10000
    assert '0x1234567890123456789012345678901234567890' not in delegations.delegation_amounts['0x9876543210987654321098765432109876543210']


####################################
#
#  Test basic business logic of the data products, in the context of a specific Client and production like data.
#

def test_Proposals_for_compound_governor_from_csv(compound_governor_abis):
    
    proposals = Proposals(governor_spec={'name': 'compound'})
        
    csvc = CSVClient('tests/data/1000-all-uniswap-to-PID83')
    csvc.set_abis(compound_governor_abis)
    chain_id = 1
    csvc.plan_event(chain_id, '0x408ed6354d4973f66138c91495f2f2fcbd8724c3', 'ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string)')

    for event, signature, signal_edge in csvc.read(after=0):
        proposals.handle(event)
    
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
    csvc.set_abis(op_governor_abis)

    chain_id = 10
    csvc.plan_event(chain_id, '0xcdf27f107725988f2261ce2256bdfcde8b382b10', 'ProposalCreated(uint256,address,address,bytes,uint256,uint256,string,uint8)')

    for event, _, _ in csvc.read(after=0):
        proposals.handle(event)
    
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


def test_Proposals_op_proposal_module_names(op_governor_abis):
    
    modules = {}
    gov_spec = {'name': 'agora', 'version': 0.1}

    proposals = Proposals(gov_spec, modules)
        
    csvc = CSVClient('tests/data/5000-all-optimism-proposalcreated-to-20250425')
    csvc.set_abis(op_governor_abis)

    chain_id = 10
    address = '0xcdf27f107725988f2261ce2256bdfcde8b382b10'

    for signature in [PROPOSAL_CREATED_1, PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4]:
        csvc.plan_event(chain_id, address, signature)
    
    for event, _, _ in csvc.read(after=0):
        proposals.handle(event)

    module_types = [p.voting_module_name for p in proposals.proposals.values()]

    results = Counter(module_types)
    
    assert results['standard'] == 73
    assert results['approval'] == 51
    assert results['optimistic'] == 3

    from sanic.response import json

    # This confirms that the objects can be serialized in a response.
    # The biggest risk is in decoding data incorrectly, and it getting
    # left as bytes.
    for proposal in proposals.proposals:
        json(proposal)
    

def test_Votes_one_op_approval_from_csv(op_governor_abis):
    
    gov_spec = {'name': 'agora', 'version': 0.1}

    votes = Votes(gov_spec)
        
    csvc = CSVClient('tests/data/3000-op-approval-PID31049')
    csvc.set_abis(op_governor_abis)

    chain_id = 10
    csvc.plan_event(chain_id, '0xcdf27f107725988f2261ce2256bdfcde8b382b10', 'VoteCast(address,uint256,uint8,uint256,string)')
    csvc.plan_event(chain_id, '0xcdf27f107725988f2261ce2256bdfcde8b382b10', 'VoteCastWithParams(address,uint256,uint8,uint256,string,bytes)')

    for event, _, _ in csvc.read(after=0):
        votes.handle(event)

    aggregations = votes.proposal_aggregations['31049359136632781771607732021569520613741907517136820917236339424553298132866']
     
    assert aggregations.result['no-param'][2] == 1087007682453656513592020
    assert aggregations.result[0][1] == 32371488932049684561146389
    assert aggregations.result[14][1] == 10504614280025813357834598

    assert len(aggregations.result) == 16

def test_ProposalTypes_proposal_type_set_with_one_scope_created(pguild_ptc_abi):

    pt = ProposalTypes()

    csvc = CSVClient('tests/data/4000-pguild-ptc-w-scopes')
    csvc.set_abis(pguild_ptc_abi)

    chain_id = 1115511
    address = '0xb7687e62d6b2cafb3ed3c3c81b0b6cf0a3884602'

    csvc.plan_event(chain_id, address, PROP_TYPE_SET_4)
    csvc.plan_event(chain_id, address, SCOPE_CREATED)

    for event, _, _ in csvc.read(after=0):
        pt.handle(event)
    
    assert len(pt.proposal_types) == 3
    assert pt.proposal_types[2]['quorum'] == 3300
    assert pt.proposal_types[2]['approval_threshold'] == 5100

    proposal_type_1 = pt.get_proposal_type_with_scopes(1)
    assert proposal_type_1['scopes'][0]['description'] == 'Distribute splits contract'

def test_DelegateVotesChanged_7day_growth_rate():

    d = Delegations()
    
    def read_sorted_dvc():
        with open('tests/data/5500-10Koptimism-dvc-w-blocks/10/0x4200000000000000000000000000000000000042/DelegateVotesChanged(address,uint256,uint256).csv', 'r') as f:
            for row in csv.DictReader(f):
                row['signature'] = 'DelegateVotesChanged(address,uint256,uint256)'
                yield int(row['block_number']), row

    def read_sorted_blocks():
        with open('tests/data/5500-10Koptimism-dvc-w-blocks/10/blocks.csv', 'r') as f:
            for row in csv.DictReader(f):
                row['timestamp'] = int(row['timestamp'])
                row['block_number'] = int(row['block_number'])
                yield row['block_number'], row

    merged = heapq.merge(
        ((block_number, 'votes', row) for block_number, row in read_sorted_dvc()),
        ((block_number, 'block', row) for block_number, row in read_sorted_blocks()),
        key=lambda x: x[0]
    )


    start = time.perf_counter()

    for block_number, source, row in merged:
        
        # print(source)

        if source == 'votes':
            d.handle(row)
        elif source == 'block':
            d.handle_block(row)

    for delegatee in d.delegatee_vp:
        print(f"{delegatee}: {d.delegate_seven_day_vp_change(delegatee)}")

    
    end = time.perf_counter()
    print(f"Time: {end - start}")

def test_ProposalTypes_v2_scope_disabled_by_index(v2_scope_abi):
    
    proposal_types = ProposalTypes()
    
    csvc = CSVClient('tests/data/7000-v2-scope-disabled')
    csvc.set_abis(v2_scope_abi)
    
    chain_id = 1
    address = '0xtest1'
    
    csvc.plan_event(chain_id, address, PROP_TYPE_SET_4)
    csvc.plan_event(chain_id, address, SCOPE_CREATED)
    csvc.plan_event(chain_id, address, SCOPE_DISABLED_2)
    
    for event, _, _ in csvc.read(after=0):
        proposal_types.handle(event)
    
    # Verify all scopes are created
    proposal_type_1 = proposal_types.get_proposal_type_with_scopes(1)
    assert len(proposal_type_1['scopes']) == 3
    for i, scope in enumerate(proposal_type_1['scopes']):
        if i == 1:
            assert scope['status'] == 'disabled'
            assert 'disabled_event' in scope
            assert scope['disabled_event']['block_number'] == '1004'
        else:
            assert scope['status'] == 'created'

def test_ProposalTypes_v2_scope_deleted_by_index(v2_scope_abi):
    
    proposal_types = ProposalTypes()
    
    csvc = CSVClient('tests/data/8000-v2-scope-deleted')
    csvc.set_abis(v2_scope_abi)
    
    chain_id = 1
    address = '0xtest2'
    
    csvc.plan_event(chain_id, address, PROP_TYPE_SET_4)
    csvc.plan_event(chain_id, address, SCOPE_CREATED)
    csvc.plan_event(chain_id, address, SCOPE_DELETED_2)
    
    for event, _, _ in csvc.read(after=0):
        proposal_types.handle(event)
    
    # Check that only the first scope (idx=0) is deleted
    proposal_type_1 = proposal_types.get_proposal_type_with_scopes(1)
    scopes = proposal_type_1['scopes']
    assert scopes[0]['status'] == 'deleted'
    assert scopes[1]['status'] == 'created'
    assert 'deleted_event' in scopes[0]
    assert scopes[0]['deleted_event']['block_number'] == '1003'

def test_ProposalTypes_v1_scope_disabled_all(pguild_ptc_abi):
    
    proposal_types = ProposalTypes()
    
    csvc = CSVClient('tests/data/9000-v1-scope-disabled')
    csvc.set_abis(pguild_ptc_abi)
    
    chain_id = 1
    address = '0xtest3'
    
    csvc.plan_event(chain_id, address, PROP_TYPE_SET_4)
    csvc.plan_event(chain_id, address, SCOPE_CREATED)
    csvc.plan_event(chain_id, address, SCOPE_DISABLED)
    
    for event, _, _ in csvc.read(after=0):
        proposal_types.handle(event)
    
    # Check that all scopes with the same scope_key are disabled
    proposal_type_1 = proposal_types.get_proposal_type_with_scopes(1)
    scopes = proposal_type_1['scopes']
    assert scopes[0]['status'] == 'disabled'
    assert scopes[1]['status'] == 'disabled'
    assert 'disabled_event' in scopes[0]
    assert 'disabled_event' in scopes[1]

def test_Proposal_start_end_block_properties():
    
    # Test with vote_start/vote_end keys
    create_event_old = {
        'proposal_id': 42,
        'proposer': '0x1234567890123456789012345678901234567890',
        'description': 'Test proposal',
        'vote_start': 1000,
        'vote_end': 2000
    }
    
    proposal_old = Proposal(create_event_old)
    assert proposal_old.start_block == 1000
    assert proposal_old.end_block == 2000
    
    # Test with start_block/end_block keys (v2)
    create_event_new = {
        'proposal_id': 43,
        'proposer': '0x1234567890123456789012345678901234567890',
        'description': 'Test proposal v2',
        'start_block': 1500,
        'end_block': 2500
    }
    
    proposal_new = Proposal(create_event_new)
    assert proposal_new.start_block == 1500
    assert proposal_new.end_block == 2500
    
    # Test fallback behavior (start_block takes precedence)
    create_event_mixed = {
        'proposal_id': 44,
        'proposer': '0x1234567890123456789012345678901234567890',
        'description': 'Test proposal mixed',
        'start_block': 3000,
        'end_block': 4000,
        'vote_start': 1000,
        'vote_end': 2000
    }
    
    proposal_mixed = Proposal(create_event_mixed)
    assert proposal_mixed.start_block == 3000
    assert proposal_mixed.end_block == 4000

def test_Proposals_agora_v2_proposal_creation(v2_proposal_abi):
    
    proposals = Proposals(governor_spec={'name': 'agora', 'version': 2.0})
    
    csvc = CSVClient('tests/data/10000-agora-v2-proposals')
    csvc.set_abis(v2_proposal_abi)
    
    chain_id = 1
    address = '0xtest4'
    
    csvc.plan_event(chain_id, address, PROPOSAL_CREATED_1)
    csvc.plan_event(chain_id, address, PROPOSAL_CREATED_MODULE)
    
    for event, _, _ in csvc.read(after=0):
        proposals.handle(event)
    
    # Verify proposal type is extracted from description
    assert '15955855790422721941705776916809127869130159600460608057182735456172624954953' in proposals.proposals
    assert proposals.proposals['15955855790422721941705776916809127869130159600460608057182735456172624954953'].proposal_type == 9
    assert proposals.proposals['15955855790422721941705776916809127869130159600460608057182735456172624954953'].create_event['proposal_type'] == 9
    
    proposal = proposals.proposals['15955855790422721941705776916809127869130159600460608057182735456172624954953']
    assert proposal.voting_module_name == 'standard'
    assert proposal.create_event['voting_module_name'] == 'standard'
    
    proposal = proposals.proposals['87979399618794581401818953625454412059253836401449247822065377110773751412581']
    assert proposal.voting_module_name == 'approval'
    assert proposal.create_event['voting_module_name'] == 'approval'

def test_NonIvotesVp():

    non_ivotes_vp = NonIVotesVP()

    import json

    for fname in glob.glob('./tests/data/nonivotes-syndicate/*.json'):
        payload = json.load(open(fname))
        non_ivotes_vp.handle(payload)
    
    assert non_ivotes_vp.block_number_to_snapshot_block_number(23836539) == 0
    assert non_ivotes_vp.block_number_to_snapshot_block_number(23836540) == 23836540
    assert non_ivotes_vp.block_number_to_snapshot_block_number(23843670) == 23843670
    assert non_ivotes_vp.block_number_to_snapshot_block_number(23843671) == 23843670
    assert non_ivotes_vp.block_number_to_snapshot_block_number(23943065) == 23943065

    assert max(non_ivotes_vp.history_bn_to_pos.keys()) == 23943065

    assert non_ivotes_vp.latest_total == '6583144304669856863236773'

def test_VoteAggregation_no_params():

    agg = VoteAggregation(module_spec=None)

    event = {
        'voter': '0x1234567890123456789012345678901234567890',
        'proposal_id': '1',
        'support': 1,
        'votes': 1000,
        'weight': 1000
    }

    agg.tally(event)

    assert agg.result['no-param'][1] == 1000
    assert agg.num_of_votes == 1
    assert len(agg.result) == 1

def test_VoteAggregation_with_params():

    agg = VoteAggregation(module_spec=None)

    from eth_abi import encode
    params_encoded = encode(['uint256[]'], [[1, 2, 3]]).hex()

    event = {
        'voter': '0x1234567890123456789012345678901234567890',
        'proposal_id': '1',
        'support': 1,
        'votes': 1000,
        'weight': 1000,
        'params': params_encoded
    }
    
    agg.tally(event)
    
    assert agg.result[1][1] == 1000
    assert agg.result[2][1] == 1000
    assert agg.result[3][1] == 1000
    assert agg.result['no-param'][1] == 1000
    assert agg.num_of_votes == 1
    assert len(agg.result) == 4

def test_VoteAggregation_with_empty_params():

    agg = VoteAggregation(module_spec=None)

    from eth_abi import encode
    params_encoded = encode(['uint256[]'], [[]]).hex()

    event = {
        'voter': '0x1234567890123456789012345678901234567890',
        'proposal_id': '1',
        'support': 1,
        'votes': 1000,
        'weight': 1000,
        'params': params_encoded
    }

    agg.tally(event)

    assert agg.result['no-param'][1] == 2000
    assert agg.num_of_votes == 1
    assert len(agg.result) == 1

def test_VoteAggregation_multiple_votes_with_params():

    agg = VoteAggregation(module_spec=None)

    from eth_abi import encode

    event1 = {
        'voter': '0x1234567890123456789012345678901234567890',
        'proposal_id': '1',
        'support': 1,
        'votes': 1000,
        'weight': 1000,
        'params': encode(['uint256[]'], [[1, 2]]).hex()
    }

    event2 = {
        'voter': '0xabcdef1234567890123456789012345678901234',
        'proposal_id': '1',
        'support': 0,
        'votes': 500,
        'weight': 500,
        'params': encode(['uint256[]'], [[2, 3]]).hex()
    }
    
    event3 = {
        'voter': '0x9876543210987654321098765432109876543210',
        'proposal_id': '1',
        'support': 1,
        'votes': 750,
        'weight': 750
    }

    agg.tally(event1)
    agg.tally(event2)
    agg.tally(event3)
    
    assert agg.result[1][1] == 1000
    assert agg.result[2][1] == 1000
    assert agg.result[2][0] == 500
    assert agg.result[3][0] == 500
    assert agg.result['no-param'][1] == 1750
    assert agg.result['no-param'][0] == 500
    assert agg.num_of_votes == 3

def test_VoteAggregation_WorldIDVoting_module():

    agg = VoteAggregation(module_spec={'name': 'WorldIDVoting'})

    from eth_abi import encode
    params_encoded = encode(['uint256', 'uint256', 'uint256[8]', 'uint256[]'], [1, 2, [0]*8, [5, 6, 7]]).hex()

    event = {
        'voter': '0x1234567890123456789012345678901234567890',
        'proposal_id': '1',
        'support': 1,
        'votes': 1000,
        'weight': 1000,
        'params': params_encoded
    }

    agg.tally(event)

    assert agg.result[5][1] == 1
    assert agg.result[6][1] == 1
    assert agg.result[7][1] == 1
    assert agg.result['no-param'][1] == 1
    assert agg.num_of_votes == 1

def test_VoteAggregation_totals():

    agg = VoteAggregation(module_spec=None)

    from eth_abi import encode

    event1 = {
        'voter': '0x1234567890123456789012345678901234567890',
        'proposal_id': '1',
        'support': 1,
        'votes': 1000,
        'weight': 1000,
        'params': encode(['uint256[]'], [[1, 2]]).hex()
    }

    event2 = {
        'voter': '0xabcdef1234567890123456789012345678901234',
        'proposal_id': '1',
        'support': 0,
        'votes': 500,
        'weight': 500
    }

    agg.tally(event1)
    agg.tally(event2)

    totals = agg.totals()

    assert totals[1]['1'] == '1000'
    assert totals[2]['1'] == '1000'
    assert totals['no-param']['1'] == '1000'
    assert totals['no-param']['0'] == '500'

def test_VoteAggregation_weight_defaults_to_votes():

    agg = VoteAggregation(module_spec=None)

    event = {
        'voter': '0x1234567890123456789012345678901234567890',
        'proposal_id': '1',
        'support': 1,
        'votes': 2000
    }

    agg.tally(event)

    assert agg.result['no-param'][1] == 2000
    assert agg.num_of_votes == 1