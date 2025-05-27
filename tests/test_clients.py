import os

import pytest
from dotenv import load_dotenv
from eth_utils import keccak

from app.clients_httpjson import JsonRpcHistHttpClient
from app.signatures import DELEGATE_VOTES_CHANGE, DELEGATE_CHANGED_1, DELEGATE_CHANGED_2
from app.clients_wsjson import JsonRpcRtWsClientCaster
from pprint import pprint

delegate_changed_ws_payload = {'address': '0x27b0031c64f4231f0aff28e668553d73f48125f3', 'topics': ['0x3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f', '0x000000000000000000000000c950b9f32259860f4731d318cb5a28b2db892f88', '0x000000000000000000000000c950b9f32259860f4731d318cb5a28b2db892f88', '0x0000000000000000000000000000000000000000000000000000000000000000'], 'data': '0x', 'blockNumber': '0x7ce8d9', 'transactionHash': '0x6ee9de9644f6c75d83d598240067d1f20466b10c8721a61a8870a040efbafad8', 'transactionIndex': '0x1f', 'blockHash': '0x81f0810aa58f1f30ac28e10aa9680c9b4247fc38a8b6a273f847f1fc06c92365', 'logIndex': '0x32', 'removed': False}
delegate_votes_changed_ws_payload = {'address': '0x27b0031c64f4231f0aff28e668553d73f48125f3', 'topics': ['0xdec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724', '0x000000000000000000000000c950b9f32259860f4731d318cb5a28b2db892f88'], 'data': '0x00000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000', 'blockNumber': '0x7ce8d9', 'transactionHash': '0x6ee9de9644f6c75d83d598240067d1f20466b10c8721a61a8870a040efbafad8', 'transactionIndex': '0x1f', 'blockHash': '0x81f0810aa58f1f30ac28e10aa9680c9b4247fc38a8b6a273f847f1fc06c92365', 'logIndex': '0x33', 'removed': False}

delegate_changed_expected_event = {'block_number': '8186073', 'log_index': 50, 'transaction_index': 31, 'signature': 'DelegateChanged(address,address,address)', 'sighash': '3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f', 'delegator': '0xc950b9f32259860f4731d318cb5a28b2db892f88', 'from_delegate': '0xc950b9f32259860f4731d318cb5a28b2db892f88', 'to_delegate': '0x0000000000000000000000000000000000000000'}
delegate_votes_changed_expected_event = {'block_number': '8186073', 'log_index': 51, 'transaction_index': 31, 'signature': 'DelegateVotesChanged(address,uint256,uint256)', 'sighash': 'dec2bacdd2f05b59de34da9b523dff8be42e5e38e818c82fdb0bae774387a724', 'delegate': '0xc950b9f32259860f4731d318cb5a28b2db892f88', 'previous_votes': 1, 'new_votes': 0}

test_cases = [(delegate_changed_ws_payload,       DELEGATE_CHANGED_1,    delegate_changed_expected_event),
              (delegate_votes_changed_ws_payload, DELEGATE_VOTES_CHANGE, delegate_votes_changed_expected_event)]
    
@pytest.mark.parametrize(
    "ws_payload, signature, expected_event",
    test_cases,
)
def test_web_socket_response_serialization_1(pguild_token_abi, ws_payload, signature, expected_event):

    event = pguild_token_abi.get_by_signature(signature)

    topic = event.topic 
    
    caster = JsonRpcRtWsClientCaster(pguild_token_abi) 
    
    out = caster.lookup(signature)(ws_payload)

    # These are added by the client...not the caster.  Meh.
    out['signature'] = signature
    out['sighash'] = topic.replace("0x", "")

    assert out == expected_event

"""

Example of an object seen by caster_fn from HTTP:

AttributeDict({'address': '0xd29687c813D741E2F938F4aC377128810E217b1b', 
               'topics': [HexBytes('0x327464c976c7451e477f8f5e678ddde081fa6ec7db71881b63f8d989951b8a9b'), HexBytes('0x0000000000000000000000004d51a39b4b74502cc5016e15e9106327936e3c5c')], 
               'data': HexBytes('0x0000000000000000000000000000000000000000000000000000000000000040000000000000000000000000000000000000000000000000000000000000006000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000000000000000000000000b35659cbac913d5e4119f2af47fd490a45e2c8260000000000000000000000000000000000000000000000000000000000002710'), 
               'blockNumber': 15778592, 
               'transactionHash': HexBytes('0x4edce1af65dd151c482c748e51478f670c87351f2991489484b62f039f74df97'), 
               'transactionIndex': 0, 
               'blockHash': HexBytes('0x6a408d462c76c7e4e22a2f06c0df5b0127789bb78da5b07030d233cb1210513d'), 
               'logIndex': 1, 
               'removed': False})
"""

delegate_changed_2_ws_payload = {'address': '0xd29687c813d741e2f938f4ac377128810e217b1b', 
                                 'topics': ['0x327464c976c7451e477f8f5e678ddde081fa6ec7db71881b63f8d989951b8a9b', '0x000000000000000000000000a622279f76ddbed4f2cc986c09244262dba8f4ba'], 
                                 'data': '0x000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000010000000000000000000000001b686ee8e31c5959d9f5bbd8122a58682788eead0000000000000000000000000000000000000000000000000000000000000a4b0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000010dc5440ad49f9ec0dd325b622d9fd225944ee40000000000000000000000000000000000000000000000000000000000000a65', 
                                 'blockNumber': '0xf3406e', 
                                 'transactionHash': '0xca3374e9d25e59705f0b066c28c6619e0e5c65c83dfdcbebd67cea41450420a1', 
                                 'transactionIndex': '0x0', 
                                 'blockHash': '0xe4c42b2f634c14ac370b83e7052440e3499480f8cefe8c1d64c63c1b989faa34', 
                                 'logIndex': '0x2', 
                                 'removed': False}

""""
DEBUGGING:
indexed_inputs:
[{'indexed': True,
  'internalType': 'address',
  'name': 'delegator',
  'type': 'address'}]
non_indexed_inputs:
[{'components': [{'internalType': 'address',
                  'name': '_delegatee',
                  'type': 'address'},
                 {'internalType': 'uint96',
                  'name': '_numerator',
                  'type': 'uint96'}],
  'indexed': False,
  'internalType': 'struct PartialDelegation[]',
  'name': 'oldDelegatees',
  'type': 'tuple[]'},
 {'components': [{'internalType': 'address',
                  'name': '_delegatee',
                  'type': 'address'},
                 {'internalType': 'uint96',
                  'name': '_numerator',
                  'type': 'uint96'}],
  'indexed': False,
  'internalType': 'struct PartialDelegation[]',
  'name': 'newDelegatees',
  'type': 'tuple[]'}]
log_topics:
['0x327464c976c7451e477f8f5e678ddde081fa6ec7db71881b63f8d989951b8a9b',
 '0x000000000000000000000000a622279f76ddbed4f2cc986c09244262dba8f4ba']
log_data:
'0x000000000000000000000000000000000000000000000000000000000000004000000000000000000000000000000000000000000000000000000000000000a000000000000000000000000000000000000000000000000000000000000000010000000000000000000000001b686ee8e31c5959d9f5bbd8122a58682788eead0000000000000000000000000000000000000000000000000000000000000a4b0000000000000000000000000000000000000000000000000000000000000001000000000000000000000000010dc5440ad49f9ec0dd325b622d9fd225944ee40000000000000000000000000000000000000000000000000000000000000a65'
"""

delegate_changed_2_expected_event = {
                                    "block_number": "15941742",
                                    "transaction_index": 0,
                                    "log_index": 2,
                                    "delegator": "0xA622279f76ddbed4f2CC986c09244262Dba8f4Ba",
                                    "old_delegatees": [('0x1b686ee8e31c5959d9f5bbd8122a58682788eead', 2635)],
                                    "new_delegatees": [('0x010dc5440ad49f9ec0dd325b622d9fd225944ee4', 2661)],
                                    "signature": "DelegateChanged(address,(address,uint96)[],(address,uint96)[])",
                                    "sighash": "327464c976c7451e477f8f5e678ddde081fa6ec7db71881b63f8d989951b8a9b"
                                    }

test_cases = [(delegate_changed_2_ws_payload, DELEGATE_CHANGED_2, delegate_changed_2_expected_event)]

@pytest.mark.parametrize(
    "ws_payload, signature, expected_event",
    test_cases,
)
def test_web_socket_response_serialization_2(scroll_token_abi, ws_payload, signature, expected_event):
    
    event = scroll_token_abi.get_by_signature(signature)
    
    topic = event.topic 
    
    caster = JsonRpcRtWsClientCaster(scroll_token_abi) 
    
    out = caster.lookup(signature)(ws_payload)

    # These are added by the client...not the caster.  Meh.
    out['signature'] = signature
    out['sighash'] = topic.replace("0x", "")

    assert out == expected_event


load_dotenv()
DAO_NODE_ARCHIVE_NODE_HTTP = "https://opt-mainnet.g.alchemy.com/v2/"
ALCHEMY_API_KEY = os.getenv('ALCHEMY_API_KEY')
ARCHIVE_NODE_HTTP_URL = DAO_NODE_ARCHIVE_NODE_HTTP + ALCHEMY_API_KEY

mock_logs = [{x: f"Test Log {x}" for x in range(100)}]

optimism_package = {
    "gov_contract_address": "0xcDF27F107725988f2261Ce2256bDfCdE8B382B10",
    "vote_cast_abi": {
        "type": "event",
        "name": "VoteCast",
        "inputs": [
            {
                "name": "voter",
                "type": "address",
                "indexed": True,
                "internalType": "address"
            },
            {
                "name": "proposalId",
                "type": "uint256",
                "indexed": False,
                "internalType": "uint256"
            },
            {
                "name": "support",
                "type": "uint8",
                "indexed": False,
                "internalType": "uint8"
            },
            {
                "name": "weight",
                "type": "uint256",
                "indexed": False,
                "internalType": "uint256"
            },
            {
                "name": "reason",
                "type": "string",
                "indexed": False,
                "internalType": "string"
            }
        ],
        "anonymous": False
    },
    "event_signature": "VoteCast(address,uint256,uint8,uint256,string)",
}

@pytest.mark.skipif(
    not ALCHEMY_API_KEY,
    reason="Skipping because ALCHEMY_API_KEY is not set"
)
@pytest.mark.parametrize("test_package", [optimism_package])
def test_get_paginated_logs(test_package):
    print("\n")

    jrhhc = JsonRpcHistHttpClient(ARCHIVE_NODE_HTTP_URL)

    # Use the correct Transfer event signature
    hash_of_event_sig = '0x' + keccak(test_package['event_signature'].encode()).hex()

    # Define block range for Optimism token events
    start_block = 135262515
    end_block = 135262530


    # Query transfer logs from Optimism token contract
    logs = jrhhc.get_paginated_logs(
        w3 = jrhhc.connect(),
        contract_address = test_package['gov_contract_address'],
        topics = [hash_of_event_sig],
        start_block=start_block,
        end_block=end_block,
        step=1,
    )
    # Just grab block numbers
    # Could look at token contract and do token transfer events

    print(f"Found {len(logs)} CastVote events")
    assert len(logs) == 2
    # Check first and second block numbers for logs are as expected
    assert logs[0]['blockNumber']== 135262518
    assert logs[1]['blockNumber'] == 135262521

@pytest.mark.skipif(
    not ALCHEMY_API_KEY,
    reason="Skipping because ALCHEMY_API_KEY is not set"
)
@pytest.mark.parametrize("test_package", [optimism_package])
def test_get_paginated_logs_block_range_over_2000(test_package):
    print("\n")
    jrhhc = JsonRpcHistHttpClient(ARCHIVE_NODE_HTTP_URL)

    # Use correct Transfer event signature
    hash_of_event_sig = '0x' + keccak(test_package['event_signature'].encode()).hex()

    # Define block range for Optimism token events
    start_block = 135252530
    end_block = 135262530


    # Query transfer logs from Optimism token contract
    logs = jrhhc.get_paginated_logs(
        w3=jrhhc.connect(),
        contract_address=test_package['gov_contract_address'],
        topics=[hash_of_event_sig],
        start_block=start_block,
        end_block=end_block,
        step=1,
    )

    print(f"Found {len(logs)} CastVote events")
    assert len(logs) == 87

@pytest.mark.skipif(
    not ALCHEMY_API_KEY,
    reason="Skipping because ALCHEMY_API_KEY is not set"
)
@pytest.mark.parametrize("test_package", [optimism_package])
def test_get_paginated_logs_are_in_chronological_order(test_package):
    print("\n")
    jrhhc = JsonRpcHistHttpClient(ARCHIVE_NODE_HTTP_URL)

    # Use correct Transfer event signature
    hash_of_event_sig = '0x' + keccak(test_package['event_signature'].encode()).hex()

    # Define block range for Optimism token events
    start_block = 135252530
    end_block = 135262530


    # Query transfer logs from Optimism token contract
    logs = jrhhc.get_paginated_logs(
        w3=jrhhc.connect(),
        contract_address=test_package['gov_contract_address'],
        topics=[hash_of_event_sig],
        start_block=start_block,
        end_block=end_block,
        step=1,
    )

    curr_high_bn = start_block
    curr_high_tran_idx = 0
    curr_high_log_idx = 0
    for log in logs:
        current_block_number = log['blockNumber']
        # blockNumber should always be ascending
        assert current_block_number >= curr_high_bn
        if current_block_number == curr_high_bn:
            current_tran_idx = log['transactionIndex']
            # If the same blockNumber, transactionIndex should be ascending
            assert current_tran_idx >= curr_high_tran_idx
            if current_tran_idx == curr_high_tran_idx:
                current_log_idx = log['logIndex']
                # If the same transactionIndex, logIndex should be ascending
                assert current_log_idx > curr_high_log_idx
                curr_high_log_idx = current_log_idx

        # Set the new highs
        # Reset to 0 if assert and if statements pass.
        # -1 Allows the index to be 0
            else:
                curr_high_tran_idx = current_tran_idx
                curr_high_log_idx = -1
        else:
            curr_high_bn = current_block_number
            curr_high_tran_idx = -1

