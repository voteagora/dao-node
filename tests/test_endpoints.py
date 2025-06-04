import pytest
import os
os.environ['AGORA_CONFIG_FILE'] = 'tests/test_config.yaml'

from unittest.mock import Mock
from sanic import Sanic
from sanic.response import json
from app.server import proposals_handler, proposal_types_handler, delegates_handler, delegate_handler, ParticipationModel
from app.data_products import Proposals, Votes, Delegations, ProposalTypes, Balances
from app.clients_csv import CSVClient
from app.signatures import *
import json

@pytest.fixture
def app():
    app = Sanic("test_app")
    
    @app.route('/v1/proposals')
    async def proposals(request):
        return await proposals_handler(app, request)
    
    @app.route('/v1/delegates')
    async def delegates(request):
        return await delegates_handler(app, request)

    @app.route('/v1/proposal_types')
    async def proposal_types(request):
        return await proposal_types_handler(app, request)

    @app.route('/v1/delegate/<addr>')
    async def delegate(request, addr):
        return await delegate_handler(app, request, addr)

    return app

@pytest.fixture
def test_client(app):
    return app.asgi_client


@pytest.mark.asyncio
async def test_proposals_endpoint(app, test_client, compound_governor_abis):

    proposals = Proposals(governor_spec={'name': 'compound'})
    votes = Votes(governor_spec={'name': 'compound'})

    chain_id = 1
    address = '0x408ed6354d4973f66138c91495f2f2fcbd8724c3'

    csvc = CSVClient('tests/data/1000-all-uniswap-to-PID83')
    csvc.set_abis(compound_governor_abis)
    csvc.plan_event(chain_id, address, 'ProposalCreated(uint256,address,address[],uint256[],string[],bytes[],uint256,uint256,string)')
    for event, _, _ in csvc.read(after=0):
        proposals.handle(event)

    csvc = CSVClient('tests/data/2000-uniswap-PID83-only')
    csvc.set_abis(compound_governor_abis)
    csvc.plan_event(chain_id, address, 'VoteCast(address,uint256,uint8,uint256,string)')
    for event, _, _ in csvc.read(after=0):
        votes.handle(event)

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
async def test_delegates_endpoint_sort_by_oldest(app):
    
    request = Mock()
    request.args = {
        "sort_by": "OLD",
        "offset": "0",
        "page_size": "10",
        "reverse": "true",
        "include": "VP,DC"
    }
    
    delegations = Delegations()
    
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
    assert delegates[0]["OLD"] == 200
    assert delegates[1]["addr"] == "0x1111111111111111111111111111111111111111"  # Block 100
    assert delegates[1]["OLD"] == 100
    assert delegates[2]["addr"] == "0x3333333333333333333333333333333333333333"  # Block 50 (lowest)
    assert delegates[2]["OLD"] == 50
    
    assert "VP" in delegates[0]
    assert "DC" in delegates[0]
    assert delegates[0]["VP"] == "2000"
    assert delegates[0]["DC"] == 10

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
    
    delegations = Delegations()
    
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
    assert delegates[0]["MRD"] == 500
    assert delegates[1]["addr"] == "0x1111111111111111111111111111111111111111"  # Block 1000
    assert delegates[1]["MRD"] == 1000
    assert delegates[2]["addr"] == "0x2222222222222222222222222222222222222222"  # Block 2000 (highest)
    assert delegates[2]["MRD"] == 2000

@pytest.mark.asyncio
async def test_proposals_types_endpoint(app, test_client, pguild_ptc_abi):

    pt = ProposalTypes()

    csvc = CSVClient('tests/data/4000-pguild-ptc-w-scopes')
    chain_id = 1115511

    csvc.set_abis(pguild_ptc_abi)

    csvc.plan_event(chain_id, '0xb7687e62d6b2cafb3ed3c3c81b0b6cf0a3884602', PROP_TYPE_SET_4)
    csvc.plan_event(chain_id, '0xb7687e62d6b2cafb3ed3c3c81b0b6cf0a3884602', SCOPE_CREATED)

    for event, _, _ in csvc.read(after=0):
        pt.handle(event)
    
    app.ctx.proposal_types = pt

    req, resp = await test_client.get('/v1/proposal_types')
    assert resp.status == 200
    assert len(resp.json) == 1

    expected_array_element = {'quorum': 0, 'approval_threshold': 0, 'name': 'Signal Votes', 'module': '0x4414d030cffec5edc011a27c653ce21704b12d85', 'scopes': []}   
    proposal_type_id = '0' 
    assert expected_array_element == resp.json['proposal_types'][proposal_type_id]

    expected_array_element = {'quorum': 0, 'approval_threshold': 5100, 'name': 'Distribute Splits', 'module': '0x0000000000000000000000000000000000000000', 'scopes': [{'scope_key': '02b27a65975a62cd8de7d22620bc9cd98e79f9042d3f5537', 'block_number': '8118843', 'transaction_index': 66, 'log_index': 113, 'selector': '2d3f5537', 'description': 'Distribute splits contract', 'disabled_event': {}, 'deleted_event': {}, 'status': 'created'}]}
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
            self.latest_vote_block = {
                '0x1111': 100,
                '0x2222': 200,
                '0x4444': 400,
                '0x5555': 500,
            }
    
    app.ctx.delegations = MockDelegations()
    app.ctx.proposals = MockProposals()
    app.ctx.votes = MockVotes()
    
    req, resp = await test_client.get('/v1/delegates?sort_by=LVB&include=VP,DC')
    
    assert resp.status == 200
    delegates = resp.json['delegates']
    
    print(delegates)
    assert len(delegates) == 4, "Expecting 4, not 5, because 0x3333 has never voted"
    assert delegates[0]['addr'] == '0x5555'
    assert delegates[0]['LVB'] == 500
    assert delegates[1]['addr'] == '0x4444'
    assert delegates[1]['LVB'] == 400
    assert delegates[2]['addr'] == '0x2222'
    assert delegates[2]['LVB'] == 200
    assert delegates[3]['addr'] == '0x1111'
    assert delegates[3]['LVB'] == 100
    
    req, resp = await test_client.get('/v1/delegates?sort_by=LVB&reverse=false&include=VP,DC')
    
    assert resp.status == 200
    delegates = resp.json['delegates']
    
    assert delegates[0]['addr'] == '0x1111'
    assert delegates[1]['addr'] == '0x2222'
    assert delegates[2]['addr'] == '0x4444'
    assert delegates[3]['addr'] == '0x5555'
    
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
async def test_delegate_endpoint(app, test_client, scroll_token_abi):
    delegations = Delegations()
    balances = Balances(token_spec={'name': 'erc20', 'version': '?'})
    
    csvc = CSVClient('tests/data/6000-delegations')
    chain_id = 1
    csvc.set_abis(scroll_token_abi)
    csvc.plan_event(chain_id, '0x1234567890123456789012345678901234567890', DELEGATE_CHANGED_2)
    for event, _, _ in csvc.read(after=0):
        delegations.handle(event)
    
    balances.handle({
        'block_number': 123456,
        'from': '0x0000000000000000000000000000000000000000',
        'to': '0x1234567890123456789012345678901234567890',
        'value': 1000000000000000000,
        'signature': 'Transfer(address,address,uint256)',
        'sighash': 'test'
    })
    
    app.ctx.delegations = delegations
    app.ctx.balances = balances

    class MockProposals:
        def counted(self, head=10):
            return []

        def completed(self, head=10):
            return []
    
    class MockVotes:
        def __init__(self):
            self.voter_history = {
                '0xabcdef1234567890123456789012345678901234': [{'block_number': 100, 'proposal_id': '0x1234'}],
                '0x2222': [{'block_number': 200, 'proposal_id': '0x1234'}],
                '0x4444': [{'block_number': 400, 'proposal_id': '0x1234'}],
                '0x5555': [{'block_number': 500, 'proposal_id': '0x1234'}],
                # 0x3333 has no voting history
            }
            self.latest_vote_block = {
                '0xabcdef1234567890123456789012345678901234': 100,
                '0x2222': 200,
                '0x4444': 400,
                '0x5555': 500,
            }

    app.ctx.proposals = MockProposals()
    app.ctx.votes = MockVotes()
    
    req, resp = await test_client.get('/v1/delegate/0xabcdef1234567890123456789012345678901234')
    assert resp.status == 200
    
    data = resp.json
    assert data['delegate']['addr'] == '0xabcdef1234567890123456789012345678901234'
    assert len(data['delegate']['from_list']) == 1
    assert data['delegate']['from_list'][0]['delegator'] == '0x1234567890123456789012345678901234567890'
    assert data['delegate']['from_list'][0]['balance'] == '1000000000000000000'
    assert data['delegate']['from_list'][0]['percentage'] == 10000

    
