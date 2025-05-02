import pytest
from unittest.mock import Mock, AsyncMock, patch
from sanic import Sanic
from sanic.response import json
from app.server import proposals_handler, proposal_types_handler, delegates_handler, ParticipationModel
from app.data_products import Proposals, Votes, ProposalTypes, Delegations
from app.clients import CSVClient
from app.signatures import *
import json

@pytest.fixture
def app():
    app = Sanic("test_app")
    
    @app.route('/v1/proposals')
    async def proposals(request):
        return await proposals_handler(app, request)

    @app.route('/v1/proposal_types')
    async def proposal_types(request):
        return await proposal_types_handler(app, request)

    @app.route('/v1/delegates')
    async def delegates(request):
        return await delegates_handler(app, request)

    return app

@pytest.fixture
def test_client(app):
    return app.asgi_client


@pytest.mark.asyncio
async def test_proposals_endpoint(app, test_client, compound_governor_abis):

    proposals = Proposals(governor_spec={'name': 'compound'})
    csvc = CSVClient('tests/data/1000-all-uniswap-to-PID83')
    chain_id = 1
    for row in csvc.read(chain_id, '0x408ed6354d4973f66138c91495f2f2fcbd8724c3', 'ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string)', compound_governor_abis):
            proposals.handle(row)

    votes = Votes(governor_spec={'name': 'compound'})
    csvc = CSVClient('tests/data/2000-uniswap-PID83-only')
    chain_id = 1
    for row in csvc.read(chain_id, '0x408ed6354d4973f66138c91495f2f2fcbd8724c3', 'VoteCast(address,uint256,uint8,uint256,string)', compound_governor_abis):
        votes.handle(row)

    # Attach mock proposals to app context
    app.ctx.proposals = proposals
    app.ctx.votes = votes
    
    # Make request to proposals endpoint
    req, resp = await test_client.get('/v1/proposals')
    
    # Verify response
    assert resp.status == 200
    assert len(resp.json) == 1

    prop83 = [proposal for proposal in resp.json['proposals'] if proposal['id'] == '83'][0]

    assert prop83['totals']['no-param']['0'] == '4405600689481310079197606'
    assert prop83['totals']['no-param']['1'] == '60410651581027066697650760'
    assert prop83['totals']['no-param']['2'] == '5795658915470619580362791'

@pytest.mark.asyncio
async def test_proposals_types_endpoint(app, test_client, pguild_ptc_abi):

    pt = ProposalTypes()

    csvc = CSVClient('tests/data/4000-pguild-ptc-w-scopes')
    chain_id = 1115511

    for row in csvc.read(chain_id, '0xb7687e62d6b2cafb3ed3c3c81b0b6cf0a3884602', PROP_TYPE_SET_4, pguild_ptc_abi):
        pt.handle(row)
    for row in csvc.read(chain_id, '0xb7687e62d6b2cafb3ed3c3c81b0b6cf0a3884602', SCOPE_CREATED, pguild_ptc_abi):
        pt.handle(row)
    
    app.ctx.proposal_types = pt

    req, resp = await test_client.get('/v1/proposal_types')
    assert resp.status == 200
    assert len(resp.json) == 1

    expected_array_element = {'quorum': 0, 'approval_threshold': 0, 'name': 'Signal Votes', 'module': '0x4414d030cffec5edc011a27c653ce21704b12d85', 'scopes': []}   
    proposal_type_id = '0' 
    assert expected_array_element == resp.json['proposal_types'][proposal_type_id]

    expected_array_element = {'quorum': 0, 'approval_threshold': 5100, 'name': 'Distribute Splits', 'module': '0x0000000000000000000000000000000000000000', 'scopes': [{'scope_key': '02b27a65975a62cd8de7d22620bc9cd98e79f9042d3f5537', 'block_number': 8118843, 'transaction_index': 66, 'log_index': 113, 'selector': '2d3f5537', 'description': 'Distribute splits contract', 'disabled_event': {}, 'deleted_event': {}, 'status': 'created'}]}
    proposal_type_id = '1'
    assert expected_array_element == resp.json['proposal_types'][proposal_type_id]

@pytest.mark.asyncio
async def test_delegates_endpoint_with_lvb_sorting(app, test_client):
    class MockDelegations:
        def __init__(self):
            # Sample delegate data
            self.delegatee_vp = {
                '0x1111': 1000,
                '0x2222': 2000,
                '0x3333': 3000,
                '0x4444': 4000,
                '0x5555': 5000,
            }

            self.delegatee_cnt = {
                '0x1111': 5,
                '0x2222': 10,
                '0x3333': 15,
                '0x4444': 20,
                '0x5555': 25,
            }

    class MockProposals:
        def completed(self, head=10):
            return []

    class MockVotes:
        def __init__(self):
            self.voter_history = {
                '0x1111': [{'block_number': 100}],
                '0x2222': [{'block_number': 200}],
                '0x4444': [{'block_number': 400}],
                '0x5555': [{'block_number': 500}],
                # 0x3333 has no voting history
            }

        def get_last_vote_block(self, addr):
            votes = self.voter_history.get(addr, [])
            if not votes:
                return 0
            return max(vote['block_number'] for vote in votes)

    app.ctx.delegations = MockDelegations()
    app.ctx.proposals = MockProposals()
    app.ctx.votes = MockVotes()

    req, resp = await test_client.get('/v1/delegates?sort_by=LVB&include=VP,DC')

    assert resp.status == 200
    delegates = resp.json['delegates']

    assert delegates[0]['addr'] == '0x5555'
    assert delegates[0]['last_vote_block'] == 500
    assert delegates[1]['addr'] == '0x4444'
    assert delegates[1]['last_vote_block'] == 400
    assert delegates[2]['addr'] == '0x2222'
    assert delegates[2]['last_vote_block'] == 200
    assert delegates[3]['addr'] == '0x1111'
    assert delegates[3]['last_vote_block'] == 100
    assert delegates[4]['addr'] == '0x3333'
    assert delegates[4]['last_vote_block'] == 0

    assert 'voting_power' in delegates[0]
    assert 'from_cnt' in delegates[0]

    req, resp = await test_client.get('/v1/delegates?sort_by=LVB&reverse=false&include=VP,DC')

    assert resp.status == 200
    delegates = resp.json['delegates']

    assert delegates[0]['addr'] == '0x3333'
    assert delegates[0]['last_vote_block'] == 0
    assert delegates[1]['addr'] == '0x1111'
    assert delegates[1]['last_vote_block'] == 100
    assert delegates[2]['addr'] == '0x2222'
    assert delegates[2]['last_vote_block'] == 200
    assert delegates[3]['addr'] == '0x4444'
    assert delegates[3]['last_vote_block'] == 400
    assert delegates[4]['addr'] == '0x5555'
    assert delegates[4]['last_vote_block'] == 500

    req, resp = await test_client.get('/v1/delegates?sort_by=LVB&page_size=2&include=VP,DC')

    assert resp.status == 200
    delegates = resp.json['delegates']

    assert len(delegates) == 2
    assert delegates[0]['addr'] == '0x5555'
    assert delegates[1]['addr'] == '0x4444'

    req, resp = await test_client.get('/v1/delegates?sort_by=LVB&page_size=2&offset=2&include=VP,DC')

    assert resp.status == 200
    delegates = resp.json['delegates']

    assert len(delegates) == 2
    assert delegates[0]['addr'] == '0x2222'
    assert delegates[1]['addr'] == '0x1111'

@pytest.mark.asyncio
async def test_delegates_endpoint_with_vp_change(app, test_client):
    delegations = Delegations(client=Mock(), chain_id=1)

    delegatee1 = "0x1111111111111111111111111111111111111111"
    delegatee2 = "0x2222222222222222222222222222222222222222"

    delegations.delegatee_list[delegatee1] = ["0xdelegator1", "0xdelegator2"]
    delegations.delegatee_list[delegatee2] = ["0xdelegator3"]
    delegations.delegatee_cnt[delegatee1] = 2
    delegations.delegatee_cnt[delegatee2] = 1

    delegations.delegatee_vp[delegatee1] = 1000000
    delegations.delegatee_vp[delegatee2] = 500000

    current_block = 15000000
    seven_days_ago_block = 14900000  # Approximate blocks for 7 days

    delegations.delegatee_vp_history[delegatee1] = [
        (seven_days_ago_block, 800000),
        (current_block, 1000000)
    ]

    delegations.delegatee_vp_history[delegatee2] = [
        (seven_days_ago_block, 600000),
        (current_block, 500000)
    ]

    delegations.vp_change_7d = {
        delegatee1: 200000,
        delegatee2: -100000
    }

    delegations._get_latest_block = Mock(return_value=current_block)

    app.ctx.delegations = delegations

    app.ctx.proposals = Proposals(governor_spec={'name': 'compound'})
    app.ctx.votes = Votes(governor_spec={'name': 'compound'})

    req, resp = await test_client.get('/v1/delegates?include=VP,VPC')

    assert resp.status == 200
    delegates = resp.json['delegates']

    assert len(delegates) == 2

    delegatee1_data = next((d for d in delegates if d['addr'] == delegatee1), None)
    delegatee2_data = next((d for d in delegates if d['addr'] == delegatee2), None)

    assert delegatee1_data is not None
    assert delegatee2_data is not None

    assert 'vp_change_7d' in delegatee1_data
    assert 'vp_change_7d' in delegatee2_data
    assert delegatee1_data['vp_change_7d'] == '200000'
    assert delegatee2_data['vp_change_7d'] == '-100000'


async def test_delegates_endpoint_sort_by_oldest(app):
    
    request = Mock()
    request.args = {
        "sort_by": "OLD",
        "offset": "0",
        "page_size": "10",
        "reverse": "true",
        "include": "VP,DC"
    }
    
    delegations = Delegations(client=Mock(), chain_id=1)
    
    delegations.delegatee_oldest_event = {
        "0x1111111111111111111111111111111111111111": {"block_number": 100, "delegator": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "from_delegate": "0x0000000000000000000000000000000000000000"},
        "0x2222222222222222222222222222222222222222": {"block_number": 200, "delegator": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "from_delegate": "0x0000000000000000000000000000000000000000"},
        "0x3333333333333333333333333333333333333333": {"block_number": 50, "delegator": "0xcccccccccccccccccccccccccccccccccccccccc", "from_delegate": "0x0000000000000000000000000000000000000000"}
    }
    
    delegations.delegatee_vp = {"0x1111111111111111111111111111111111111111": 1000, "0x2222222222222222222222222222222222222222": 2000, "0x3333333333333333333333333333333333333333": 3000}
    delegations.delegatee_cnt = {"0x1111111111111111111111111111111111111111": 5, "0x2222222222222222222222222222222222222222": 10, "0x3333333333333333333333333333333333333333": 15}
    
    # Configure mock app context
    app.ctx = Mock()
    app.ctx.delegations = delegations
    app.ctx.proposals = Mock(spec=Proposals)
    app.ctx.votes = Mock(spec=Votes)
    
    response = await delegates_handler(app, request)
    
    result = json.loads(response.body)
    delegates = result["delegates"]
    
    assert len(delegates) == 3
    assert delegates[0]["addr"] == "0x2222222222222222222222222222222222222222"  # Block 200 (highest)
    assert delegates[0]["oldest_block"] == 200
    assert delegates[1]["addr"] == "0x1111111111111111111111111111111111111111"  # Block 100
    assert delegates[1]["oldest_block"] == 100
    assert delegates[2]["addr"] == "0x3333333333333333333333333333333333333333"  # Block 50 (lowest)
    assert delegates[2]["oldest_block"] == 50
    
    assert "voting_power" in delegates[0]
    assert "from_cnt" in delegates[0]
    assert delegates[0]["voting_power"] == "2000"
    assert delegates[0]["from_cnt"] == 10

@pytest.mark.asyncio
async def test_delegates_endpoint_sort_by_latest(app):
    
    request = Mock()
    request.args = {
        "sort_by": "MRD",
        "offset": "0",
        "page_size": "10",
        "reverse": "false",
        "include": "VP,DC,OL"
    }
    
    delegations = Delegations(client=Mock(), chain_id=1)
    
    delegations.delegatee_latest_event = {
        "0x1111111111111111111111111111111111111111": {"block_number": 1000, "delegator": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "from_delegate": "0x0000000000000000000000000000000000000000"},
        "0x2222222222222222222222222222222222222222": {"block_number": 2000, "delegator": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "from_delegate": "0x0000000000000000000000000000000000000000"},
        "0x3333333333333333333333333333333333333333": {"block_number": 500, "delegator": "0xcccccccccccccccccccccccccccccccccccccccc", "from_delegate": "0x0000000000000000000000000000000000000000"}
    }
    
    delegations.delegatee_oldest_event = {
        "0x1111111111111111111111111111111111111111": {"block_number": 100, "delegator": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "from_delegate": "0x0000000000000000000000000000000000000000"},
        "0x2222222222222222222222222222222222222222": {"block_number": 200, "delegator": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", "from_delegate": "0x0000000000000000000000000000000000000000"},
        "0x3333333333333333333333333333333333333333": {"block_number": 50, "delegator": "0xcccccccccccccccccccccccccccccccccccccccc", "from_delegate": "0x0000000000000000000000000000000000000000"}
    }
    
    delegations.delegatee_vp = {"0x1111111111111111111111111111111111111111": 1000, "0x2222222222222222222222222222222222222222": 2000, "0x3333333333333333333333333333333333333333": 3000}
    delegations.delegatee_cnt = {"0x1111111111111111111111111111111111111111": 5, "0x2222222222222222222222222222222222222222": 10, "0x3333333333333333333333333333333333333333": 15}
    
    # Configure mock app context
    app.ctx = Mock()
    app.ctx.delegations = delegations
    app.ctx.proposals = Mock(spec=Proposals)
    app.ctx.votes = Mock(spec=Votes)
    
    response = await delegates_handler(app, request)
    
    result = json.loads(response.body)
    delegates = result["delegates"]
    
    assert len(delegates) == 3
    assert delegates[0]["addr"] == "0x3333333333333333333333333333333333333333"  # Block 500 (lowest)
    assert delegates[0]["most_recent_block"] == 500
    assert delegates[1]["addr"] == "0x1111111111111111111111111111111111111111"  # Block 1000
    assert delegates[1]["most_recent_block"] == 1000
    assert delegates[2]["addr"] == "0x2222222222222222222222222222222222222222"  # Block 2000 (highest)
    assert delegates[2]["most_recent_block"] == 2000
