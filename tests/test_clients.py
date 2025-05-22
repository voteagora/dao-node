import os
import json

import pytest
from _pytest.outcomes import Exit

from app.signatures import DELEGATE_VOTES_CHANGE, DELEGATE_CHANGED_1, DELEGATE_CHANGED_2
from app.clients_wsjson import JsonRpcRtWsClientCaster
from pprint import pprint
from app.utils import camel_to_snake
from app.signatures import DELEGATE_VOTES_CHANGE, DELEGATE_CHANGED_1
from eth_abi.abi import decode as decode_abi
from app.clients import JsonRpcRTWsClient, JsonRpcHistHttpClient, resolve_block_count_span
from dotenv import load_dotenv

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

def test_get_paginated_logs():
    load_dotenv()
    DAO_NODE_ARCHIVE_NODE_HTTP = "https://opbnb-mainnet.g.alchemy.com/v2/"
    ARCHIVE_NODE_HTTP_URL = DAO_NODE_ARCHIVE_NODE_HTTP + os.getenv('ALCHEMY_API_KEY', '')
    jrhhc = JsonRpcHistHttpClient(ARCHIVE_NODE_HTTP_URL)

    block_count_span = resolve_block_count_span(10)

    with open('./abis/op-gov.json', 'r') as f:
        abi = json.load(f)

    logs = jrhhc.get_paginated_logs(
        jrhhc.connect(),
        "0x27b0031c64f4231f0aff28e668553d73f48125f3",
        "0x3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f",
        1000000000000000000000000000000000000000000000000000000000000000,
        1000000000000000000000000000000000000000000000000000000000000000,
        block_count_span,
        abi
    )

    print(logs)

def test_get_paginated_logs_block_range_over_2000():
    pass

mock_logs = [{x: f"Test Log {x}" for x in range(100)}]

def mock_get_logs(event_filter):
    start = event_filter.get('fromBlock')
    end = event_filter.get('toBlock')

    if end - start > 20:
        raise Exception(
            "web3.exceptions.Web3RPCError: {'code': -32602, 'message': 'Log response size exceeded. You can make eth_getLogs requests with up to a 2K block range and no limit on the response size, or you can request any block range with a cap of 10K logs in the response. Based on your parameters and the response size limit, this block range should work: [0x81c5fbc, 0x81c6902]'}")

    return [log for x, log in enumerate(mock_logs) if start <= x <= end]

def poc(from_block, to_block, contract_address, event_signature_hash, current_recursion_depth, max_recursion_depth=10):
    if current_recursion_depth > max_recursion_depth:
        raise Exception(f"Maximum recursion depth {max_recursion_depth} exceeded. This can be adjusted in the function parameter.")

    event_filter = {
        "fromBlock": from_block,
        "toBlock": to_block,
        "address": contract_address,
        "topics": [event_signature_hash]
    }
    try:
        logs = mock_get_logs(event_filter)
        return logs
    except Exception as e:
        error_msg = str(e)
        if "Log response size exceeded" in error_msg:
            print("Recursively calling poc with a smaller block range.")

            # add one to recursion depth
            current_recursion_depth += 1

            # split block range in half
            mid = (from_block + to_block) // 2

            print(f"poc1: from {from_block} - {mid-1}\npoc2: from {mid} - {to_block}")
            # Get results from both recursive calls
            first_half = poc(
                from_block=from_block,
                to_block=mid - 1,
                contract_address=contract_address,
                event_signature_hash=event_signature_hash,
                current_recursion_depth=current_recursion_depth,
                max_recursion_depth=max_recursion_depth
            )

            second_half = poc(
                from_block=mid,
                to_block=to_block,
                contract_address=contract_address,
                event_signature_hash=event_signature_hash,
                current_recursion_depth=current_recursion_depth,
                max_recursion_depth=max_recursion_depth
            )

            # Combine results, handling potential None values
            result = []
            if first_half is not None:
                result.extend(first_half)
            if second_half is not None:
                result.extend(second_half)
            return result

        else:
                raise e


def test_poc():
    logs = poc(
        from_block=0,
        to_block=100,
        contract_address="0x27b0031c64f4231f0aff28e668553d73f48125f3",
        event_signature_hash="0x3134e8a2e6d97e929a7e54011ea5485d7d196dd5f0ba4d4ef95803e8e3fc257f",
        current_recursion_depth=0,
        max_recursion_depth=10
    )
    print(logs)