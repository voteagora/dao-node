import pytest
from unittest.mock import patch, MagicMock
from abifsm import ABI, ABISet

from app.clients_db import DbHistClient, DbRtClient, DbClientCaster, CHAIN_ID_TO_BLOCK_TABLE
from app.signatures import TRANSFER, DELEGATE_CHANGED_2, VOTE_CAST_1


@pytest.fixture
def db_abis():
    """ABISet using the pguild token ABI for testing."""
    import os
    abi_path = os.path.join('tests', 'abis', 'pguild-token.json')
    abi = ABI.from_file('token', abi_path)
    abis = ABISet('test_prefix', [abi])
    return abis


def test_db_hist_client_is_valid_no_url():
    client = DbHistClient(url=None)
    assert client.is_valid() is False


def test_db_hist_client_is_valid_empty_url():
    client = DbHistClient(url='')
    assert client.is_valid() is False


def test_db_hist_client_is_valid_ignored_url():
    client = DbHistClient(url='ignored')
    assert client.is_valid() is False


def test_db_hist_client_plan_event(db_abis):
    client = DbHistClient(url='postgresql://fake', db_table_prefix='test_prefix', db_schema='goldsky')
    client.set_abis(db_abis)

    with patch.object(client, 'check_table', return_value=True):
        client.plan_event(chain_id=10, address='0xabc', signature=TRANSFER)

    assert len(client.subscription_meta) == 1
    event_or_block, meta = client.subscription_meta[0]
    assert event_or_block == 'event'
    table_name, chain_id, address, signature, sighash, caster_fn = meta
    assert chain_id == 10
    assert address == '0xabc'
    assert signature == TRANSFER
    assert 'transfer' in table_name
    assert caster_fn is not None


def test_db_hist_client_plan_block():
    client = DbHistClient(url='postgresql://fake', db_table_prefix='test_prefix')

    with patch.object(client, 'check_table', return_value=True):
        client.plan_block(chain_id=10)

    assert len(client.subscription_meta) == 1
    event_or_block, meta = client.subscription_meta[0]
    assert event_or_block == 'block'
    table_name, chain_id = meta
    assert chain_id == 10
    assert table_name == 'blocks_optimism'


def test_db_hist_client_plan_block_unknown_chain():
    client = DbHistClient(url='postgresql://fake', db_table_prefix='test_prefix')

    with pytest.raises(Exception, match="No chain mapping"):
        client.plan_block(chain_id=999999)


def test_db_rt_client_inherits_plan_event(db_abis):
    client = DbRtClient(url='postgresql://fake', db_table_prefix='test_prefix', db_schema='goldsky')
    client.set_abis(db_abis)

    with patch.object(client, 'check_table', return_value=True):
        client.plan_event(chain_id=10, address='0xabc', signature=TRANSFER)

    assert len(client.subscription_meta) == 1


def test_db_client_caster_default(db_abis):
    caster = DbClientCaster(db_abis)
    fn = caster.lookup(TRANSFER)

    row = {'from': '0xABC', 'to': '0xDEF', 'value': 100}
    result = fn(row)
    assert result['from'] == '0xABC'


def test_db_client_caster_vote_cast(db_abis):
    caster = DbClientCaster(db_abis)
    fn = caster.lookup(VOTE_CAST_1)

    row = {'voter': '0xABCdef', 'proposal_id': 1, 'support': 1, 'weight': 100, 'reason': 'yes'}
    result = fn(row)
    assert result['voter'] == '0xabcdef'


def test_db_client_caster_bytes_coercion(db_abis):
    caster = DbClientCaster(db_abis)
    fn = caster.lookup(TRANSFER)

    row = {'from': b'\xab\xcd', 'to': '0xDEF', 'value': 1}
    result = fn(row)
    assert result['from'] == 'abcd'


def test_db_abis_prefix_matches_tables(db_abis):
    """Verify that the DB-specific ABISet produces table names with the right prefix."""
    client = DbHistClient(url='postgresql://fake', db_table_prefix='multi_op')
    client.set_abis(db_abis)

    with patch.object(client, 'check_table', return_value=True):
        client.plan_event(chain_id=10, address='0xabc', signature=TRANSFER)

    _, meta = client.subscription_meta[0]
    table_name = meta[0]
    assert table_name.startswith('multi_op_')


def test_chain_id_to_block_table_coverage():
    """Sanity check that common chain IDs are mapped."""
    assert CHAIN_ID_TO_BLOCK_TABLE[1] == 'ethereum'
    assert CHAIN_ID_TO_BLOCK_TABLE[10] == 'optimism'
    assert CHAIN_ID_TO_BLOCK_TABLE[534352] == 'scroll'
    assert CHAIN_ID_TO_BLOCK_TABLE[480] == 'worldchain'
