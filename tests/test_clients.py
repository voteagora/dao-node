import pytest
from app.utils import camel_to_snake
from app.signatures import DELEGATE_VOTES_CHANGE, DELEGATE_CHANGED_1
from eth_abi.abi import decode as decode_abi
from app.clients_wsjson import JsonRpcRtWsClientCaster

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
def test_web_socket_response_serialization(pguild_token_abi, ws_payload, signature, expected_event):

    event = pguild_token_abi.get_by_signature(signature)
    
    abi = event.literal

    inputs = abi['inputs']

    topic = event.topic 
    
    caster = JsonRpcRtWsClientCaster(pguild_token_abi) 
    
    out = caster.lookup(signature)(ws_payload)

    # These are added by the client...not the caster.  Meh.
    out['signature'] = signature
    out['sighash'] = topic.replace("0x", "")

    assert out == expected_event