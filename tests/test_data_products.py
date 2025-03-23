import pytest
from app.data_products import Balances, Delegations, Proposals
import csv
import os

def test_balances():

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

def test_delegations():

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

def test_Proposals_for_compound_governor_from_csv():
    # Initialize the Proposals data product
    proposals = Proposals(governor_spec={'name': 'compound'}) 

    # Path to the CSV file
    csv_path = os.path.join('tests', 'data', '1', '1000-all-uniswap-to-PID83-ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string).csv')
    
    # Read and process each row from the CSV
    with open(csv_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Convert numeric fields
            row['block_number'] = int(row['block_number'])
            row['log_index'] = int(row['log_index'])
            row['id'] = int(row['id'])
            
            # Add signature and sighash (required by the handler)
            row['signature'] = 'ProposalCreated'
            row['sighash'] = '7d84a6263ae0d98d3329bd7b46bb4e8d6f98cd35a7adb45c274c8b7fd5ebd5e0'  # ProposalCreated event sighash
            
            # Process the record
            proposals.handle(row)
    
    # Get the first proposal from the data product
    first_proposal = next(proposals.unfiltered())
    
    # Basic assertions to verify the data was loaded
    assert first_proposal is not None
    assert isinstance(first_proposal.create_event['id'], str)  # IDs are stored as strings
    assert 'description' in first_proposal.create_event
    assert 'targets' in first_proposal.create_event
    assert 'values' in first_proposal.create_event
    
    assert 'calldatas' in first_proposal.create_event
    assert first_proposal.create_event['calldatas'][0] == 'a9059cbb0000000000000000000000005069a64bc6616dec1584ee0500b7813a9b680f7e00000000000000000000000000000000000000000010cf035cc2441ead340000'
    
    assert 'signatures' in first_proposal.create_event
    assert first_proposal.create_event['signatures'][0] == ''

    assert 'start_block' in first_proposal.create_event
    assert first_proposal.create_event['start_block'] == '22039575'

    assert 'end_block' in first_proposal.create_event
    assert first_proposal.create_event['end_block'] == '22079895'
    
    