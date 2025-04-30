import pytest
from unittest.mock import Mock, AsyncMock
from sanic import Sanic
from sanic.response import json
from app.server import proposals_handler, proposal_types_handler, delegate_handler
from app.data_products import Proposals, Votes, ProposalTypes, Delegations, Balances
from app.clients import CSVClient
from app.signatures import *

@pytest.fixture
def app():
    app = Sanic("test_app")
    
    @app.route('/v1/proposals')
    async def proposals(request):
        return await proposals_handler(app, request)

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
async def test_delegate_endpoint(app, test_client, scroll_token_abi):
    delegations = Delegations()
    balances = Balances(token_spec={'name': 'erc20', 'version': '?'})
    
    csvc = CSVClient('tests/data/6000-delegations')
    chain_id = 1
    for row in csvc.read(chain_id, '0x1234567890123456789012345678901234567890', DELEGATE_CHANGED_2, scroll_token_abi):
        delegations.handle(row)
    
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
    
    req, resp = await test_client.get('/v1/delegate/0xabcdef1234567890123456789012345678901234')
    assert resp.status == 200
    
    data = resp.json
    assert data['delegate']['addr'] == '0xabcdef1234567890123456789012345678901234'
    assert len(data['delegate']['from_list']) == 1
    assert data['delegate']['from_list'][0]['address'] == '0x1234567890123456789012345678901234567890'
    assert data['delegate']['from_list'][0]['balance'] == '1000000000000000000'
    assert data['delegate']['from_list'][0]['percentage'] == 10000

    