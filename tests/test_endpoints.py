import pytest
from unittest.mock import Mock, AsyncMock
from sanic import Sanic
from sanic.response import json
from app.server import proposals_handler
from app.data_products import Proposals

@pytest.fixture
def app():
    app = Sanic("test_app")
    
    @app.route('/v1/proposals')
    async def proposals(request):
        return await proposals_handler(app, request)
    
    return app

@pytest.fixture
def test_client(app):
    return app.asgi_client

@pytest.fixture
def mock_proposals():
    mock = Mock(spec=Proposals)
    mock.get_proposals = AsyncMock(return_value=[{
        'id': '1',
        'title': 'Test Proposal',
        'description': 'Test Description',
        'status': 'active'
    }])
    return mock

@pytest.mark.asyncio
async def test_proposals_endpoint(app, test_client, mock_proposals):
    # Attach mock proposals to app context
    app.ctx.dps = {'proposals': [mock_proposals]}
    
    # Make request to proposals endpoint
    req, resp = await test_client.get('/v1/proposals')
    
    # Verify response
    assert resp.status == 200
    assert len(resp.json) == 1
    assert resp.json[0]['id'] == '1'
    assert resp.json[0]['title'] == 'Test Proposal'
    
    # Verify mock was called
    mock_proposals.get_proposals.assert_called_once()
