import pytest
from unittest.mock import patch, MagicMock
from abifsm import ABI, ABISet

from app.clients_db import DbPollingClient, DbClientCaster, CHAIN_ID_TO_BLOCKS_TABLE
from app.signatures import TRANSFER, DELEGATE_VOTES_CHANGE, DELEGATE_CHANGED_1, VOTE_CAST_1


@pytest.fixture
def db_abis():
    """ABISet using the pguild token ABI for testing."""
    import os
    abi_path = os.path.join('tests', 'abis', 'pguild-token.json')
    abi = ABI.from_file('token', abi_path)
    abis = ABISet('test_prefix', [abi])
    return abis


def test_db_polling_client_is_valid_no_url():
    client = DbPollingClient(db_url=None, db_table_prefix='test')
    assert client.is_valid() is False


def test_db_polling_client_is_valid_empty_url():
    client = DbPollingClient(db_url='', db_table_prefix='test')
    assert client.is_valid() is False


def test_db_polling_client_plan_event(db_abis):
    client = DbPollingClient(db_url='postgresql://fake', db_table_prefix='test_prefix')
    client.set_abis(db_abis)

    client.plan_event(chain_id=10, address='0xabc', signature=TRANSFER)

    assert len(client._event_plans) == 1
    plan = client._event_plans[0]
    assert plan['chain_id'] == 10
    assert plan['address'] == '0xabc'
    assert plan['signature'] == TRANSFER
    assert 'transfer' in plan['table_name']
    assert plan['caster_fn'] is not None


def test_db_polling_client_plan_block():
    client = DbPollingClient(db_url='postgresql://fake', db_table_prefix='test_prefix')

    # Use a mixin-compatible init (no set_abis needed for plan_block)
    client.plan_block(chain_id=10)

    assert len(client._block_plans) == 1
    assert client._block_plans[0]['chain_id'] == 10
    assert client._block_plans[0]['table_name'] == 'blocks_optimism'


def test_db_polling_client_plan_block_unknown_chain():
    client = DbPollingClient(db_url='postgresql://fake', db_table_prefix='test_prefix')

    client.plan_block(chain_id=999999)

    assert len(client._block_plans) == 0


def test_db_polling_client_set_last_seen_block():
    client = DbPollingClient(db_url='postgresql://fake', db_table_prefix='test_prefix')
    assert client.last_seen_block == 0

    client.set_last_seen_block(12345)
    assert client.last_seen_block == 12345


def test_db_client_caster_default(db_abis):
    caster = DbClientCaster(db_abis)
    fn = caster.lookup(DELEGATE_VOTES_CHANGE)

    row = {'delegate': '0xABC', 'previous_votes': 100, 'new_votes': 200}
    result = fn(row)
    assert result['delegate'] == '0xABC'
    assert result['previous_votes'] == 100


def test_db_client_caster_vote_cast(db_abis):
    caster = DbClientCaster(db_abis)
    fn = caster.lookup(VOTE_CAST_1)

    row = {'voter': '0xABCdef', 'proposal_id': 1, 'support': 1, 'weight': 100, 'reason': 'yes'}
    result = fn(row)
    assert result['voter'] == '0xabcdef'


def test_db_client_caster_bytes_coercion(db_abis):
    caster = DbClientCaster(db_abis)
    fn = caster.lookup(DELEGATE_VOTES_CHANGE)

    row = {'delegate': b'\xab\xcd', 'previous_votes': 1, 'new_votes': 2}
    result = fn(row)
    assert result['delegate'] == 'abcd'


def test_db_abis_prefix_matches_tables(db_abis):
    """Verify that the DB-specific ABISet produces table names with the right prefix."""
    client = DbPollingClient(db_url='postgresql://fake', db_table_prefix='multi_pguild')
    client.set_abis(db_abis)

    client.plan_event(chain_id=10, address='0xabc', signature=TRANSFER)

    plan = client._event_plans[0]
    assert plan['table_name'].startswith('multi_pguild_')


def test_db_polling_client_set_max_block():
    client = DbPollingClient(db_url='postgresql://fake', db_table_prefix='test_prefix')
    assert client.max_block is None

    client.set_max_block(99999)
    assert client.max_block == 99999


def test_chain_id_to_blocks_table_coverage():
    """Sanity check that common chain IDs are mapped."""
    assert CHAIN_ID_TO_BLOCKS_TABLE[1] == 'ethereum'
    assert CHAIN_ID_TO_BLOCKS_TABLE[10] == 'optimism'
    assert CHAIN_ID_TO_BLOCKS_TABLE[534352] == 'scroll'
    assert CHAIN_ID_TO_BLOCKS_TABLE[480] == 'worldchain'
