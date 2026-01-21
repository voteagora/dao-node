# DAO Node - Architecture & Developer Guide

A comprehensive guide to understanding and running the DAO Node codebase.

---

## Table of Contents

1. [Overview](#overview)
2. [Project Structure](#project-structure)
3. [Architecture](#architecture)
4. [Key Components](#key-components)
5. [Environment Configuration](#environment-configuration)
6. [Running the Project](#running-the-project)
7. [API Endpoints](#api-endpoints)
8. [Testing](#testing)
9. [Docker Deployment](#docker-deployment)

---

## Overview

**DAO Node** is a high-performance service for DAO-centric applications that serves blazing-fast chain data backed by an **in-RAM data model** maintained at tip (latest block). It is designed for:

- **Post-boot performance** - Fast data serving after initial sync
- **Flexibility** - Quick time-to-market for data-model changes
- **Testability** - Comprehensive test coverage
- **Cost optimization** - Low hosting costs for multi-region deployments

### Core Dependencies

- **YAML Config File** - Agora specification for contract deployments
- **Publicly Hosted ABIs** - One file per contract
- **JSON-RPC Endpoint** - For blockchain data
- **Archive Data Sources** (optional) - Accelerates boot times for large DAOs

---

## Project Structure

```
dao-node/
├── app/                      # Main application code
│   ├── server.py             # Sanic web server & API endpoints
│   ├── cli.py                # CLI commands (sync-from-gcs)
│   ├── data_products.py      # Data product classes (Balances, Proposals, Votes, etc.)
│   ├── data_models.py        # Data models (ParticipationRateModel)
│   ├── clients_csv.py        # CSV archive client
│   ├── clients_httpjson.py   # HTTP JSON-RPC client
│   ├── clients_wsjson.py     # WebSocket JSON-RPC client
│   ├── clients_wsvpsnapper.py# VP Snapper WebSocket client
│   ├── signatures.py         # Event signatures (Transfer, VoteCast, etc.)
│   ├── middleware.py         # Request timing middleware
│   ├── profiling.py          # Performance profiling
│   ├── logsetup.py           # Logging configuration
│   ├── dev_modes.py          # Development mode flags
│   └── utils.py              # Utility functions
├── tests/                    # Test suite
│   ├── test_data_products.py # Data product unit tests
│   ├── test_endpoints.py     # API endpoint tests
│   ├── test_clients.py       # Client tests
│   ├── conftest.py           # Pytest fixtures
│   └── abis/                 # Test ABI files
├── static/                   # Static HTML/CSS/JS files
│   └── html/                 # UI pages (index, proposals, delegates)
├── data/                     # Local data storage
├── docs/                     # Additional documentation
├── Dockerfile                # Docker build configuration
├── requirements.txt          # Python dependencies
├── requirements_dev.txt      # Development dependencies
└── pyproject.toml            # Python project configuration (Black formatter)
```

---

## Architecture

### Data Flow

```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────┐
│  Archive Data   │────▶│                  │────▶│   Data Products │
│  (CSV/GCS)      │     │      Feed        │     │  (In-RAM State) │
└─────────────────┘     │                  │     └─────────────────┘
                        │   ClientSequencer│              │
┌─────────────────┐     │                  │              ▼
│  Real-time WS   │────▶│                  │     ┌─────────────────┐
│  (JSON-RPC)     │     └──────────────────┘     │   API Endpoints │
└─────────────────┘                              │   (Sanic)       │
                                                 └─────────────────┘
```

### Core Classes

| Class | Description |
|-------|-------------|
| `DataProductContext` | Main context holding all data products and the event feed |
| `Feed` | Manages reading from archive and real-time data sources |
| `ClientSequencer` | Sequences multiple data clients (archive, realtime, polling) |
| `Sanic('DaoNode')` | The main web application instance |

### Data Products

Data products are in-RAM data structures that process blockchain events and maintain state:

| Data Product | Purpose |
|--------------|---------|
| `Balances` | Token balances from Transfer events |
| `Delegations` | Delegation state from DelegateChanged events |
| `Proposals` | Proposal state from ProposalCreated/Canceled/Executed events |
| `Votes` | Vote records from VoteCast events |
| `ProposalTypes` | Proposal type configurations |
| `NonIVotesVP` | Non-IVotes voting power tracking |

### Event Signatures Handled

The system tracks these Solidity event signatures (defined in `app/signatures.py`):

- **Token Events**: `Transfer(address,address,uint256)`
- **Delegation Events**: `DelegateChanged`, `DelegateVotesChanged`
- **Proposal Events**: `ProposalCreated`, `ProposalCanceled`, `ProposalQueued`, `ProposalExecuted`
- **Vote Events**: `VoteCast`, `VoteCastWithParams`
- **Scope Events**: `ScopeCreated`, `ScopeDeleted`, `ScopeDisabled`
- **Proposal Type Events**: `ProposalTypeSet`

---

## Key Components

### 1. Server (`app/server.py`)

The main Sanic web server that:
- Initializes data products
- Registers API routes
- Manages boot sequence (archive sync → realtime listening)
- Provides OpenAPI documentation at `/docs`

### 2. CLI (`app/cli.py`)

Command-line interface for data operations:

```bash
# Sync archive data from Google Cloud Storage
python -m app.cli sync-from-gcs <directory>
```

### 3. Clients

| Client | Type | Purpose |
|--------|------|---------|
| `CSVClient` | Archive | Reads historical data from CSV files |
| `JsonRpcHistHttpClient` | Archive | Fetches historical data via HTTP JSON-RPC |
| `JsonRpcRtHttpClient` | Polling | Polls for new data via HTTP |
| `JsonRpcRtWsClient` | Realtime | Subscribes to real-time events via WebSocket |
| `VPSnappercWsClient` | Realtime | Specialized VP snapper WebSocket client |

---

## Environment Configuration

Create a `.env` file with these variables:

```bash
# Required
AGORA_CONFIG_FILE="/path/to/your/config.yaml"    # YAML config file path
CONTRACT_DEPLOYMENT="main"                        # Deployment key from config

# Blockchain Node Configuration
DAO_NODE_ARCHIVE_NODE_HTTP="https://your-archive-node"  # Archive node HTTP URL
DAO_NODE_REALTIME_NODE_WS="wss://your-realtime-node"    # Realtime node WebSocket URL

# API Keys (appended to URLs if provider detected)
ALCHEMY_API_KEY="your-alchemy-key"
QUICKNODE_API_KEY="your-quicknode-key"

# Optional
DAO_NODE_DATA_PATH="./data"                       # Local data directory
DAO_NODE_GCLOUD_BUCKET="bucket-name"              # GCS bucket for archive data
DAO_NODE_VPSNAPPER_WS="wss://vpsnapper-url"       # VP Snapper WebSocket URL
GIT_COMMIT_SHA="abc123"                           # Git commit SHA for tracking
```

### YAML Config File Example

```yaml
friendly_short_name: MyDAO

token_spec: 
  name: erc20
  version: '?'  # U -> Uniswap, E -> ENS, G -> latest github, '?' -> unknown

governor_spec:
  name: agora
  version: 1.0

deployments:
  main:
    chain_id: 10
    token: 
      address: '0x4200000000000000000000000000000000000042'
    gov: 
      address: '0xcDF27F107725988f2261Ce2256bDfCdE8B382B10'
    ptc:
      address: '0x67ecA7B65Baf0342CE7fBf0AA15921524414C09f'
```

---

## Running the Project

### Prerequisites

- Python >= 3.11
- Docker (optional)

### 1. Local Development Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# For development
pip install -r requirements_dev.txt
```

### 2. Run the Server

#### Via Python (Development)

```bash
# Direct run with default settings
python -m app.server

# Or using Sanic CLI
sanic app.server --host=0.0.0.0 --port=8000
```

#### Via Sanic with Hot Reload

```bash
sanic app.server --host=0.0.0.0 --port=8004 --dev --debug
```

### 3. Sync Archive Data from GCS

```bash
# Set environment variables
export AGORA_CONFIG_FILE="/path/to/your/config.yaml"
export DAO_NODE_GCLOUD_BUCKET="your-gcs-bucket"
export CONTRACT_DEPLOYMENT="main"

# Sync data to local directory
python -m app.cli sync-from-gcs data

# With multi-processing for faster sync
python -m app.cli sync-from-gcs data --multi-processing

# Strict mode (fail on errors)
python -m app.cli sync-from-gcs data --strict
```

---

## API Endpoints

### Health & Status

| Endpoint | Description |
|----------|-------------|
| `GET /health` | Server health check, returns files, IP, config, version |
| `GET /config` | Server configuration |
| `GET /deployment` | Smart contract deployment info |

### Token State

| Endpoint | Description |
|----------|-------------|
| `GET /v1/balance/<addr>` | Token balance for address (if enabled) |

### Proposal State

| Endpoint | Description |
|----------|-------------|
| `GET /v1/proposals` | All proposals with outcome state |
| `GET /v1/proposal/<id>` | Single proposal details with vote totals |
| `GET /v1/proposal_types` | All proposal type configurations |
| `GET /v1/vote_record/<proposal_id>` | Paginated voting record for a proposal |
| `GET /v1/vote?proposal_id=X&voter=Y` | Specific vote by voter on proposal |
| `GET /v1/voter_history/<voter>` | Voting history for a delegate |

### Delegation State

| Endpoint | Description |
|----------|-------------|
| `GET /v1/delegates` | Sorted, paginated list of delegates |
| `GET /v1/delegate/<addr>` | Single delegate details |
| `GET /v1/delegations?delegatee=X` | Delegations to a specific delegatee |
| `GET /v1/voting_power/<addr>` | Voting power for an address |
| `GET /v1/voting_power/<addr>/<block>` | Historical voting power at block |

### Query Parameters

**Delegates endpoint (`/v1/delegates`):**
- `page_size` - Number of records (default: 200)
- `offset` - Records to skip (default: 0)
- `sort_by` - Sort key: `VP` (voting power), `DC` (delegator count), `MRD` (most recent delegation), `OLD` (oldest delegation), `LVB` (last vote block), `VPC` (7-day VP change)
- `reverse` - Sort descending (default: true)
- `include` - Additional fields: `VP`, `DC`, `PR` (participation rate), `VPC`
- `delegator` - Filter by delegator address

**Vote record endpoint (`/v1/vote_record/<id>`):**
- `sort_by` - Sort by `BN` (block number) or `VP` (voting power)
- `page_size`, `offset`, `reverse`, `full`

### UI Pages

| Endpoint | Description |
|----------|-------------|
| `GET /` | Index page |
| `GET /ui/proposals` | Proposals UI |
| `GET /ui/delegates` | Delegates UI |
| `GET /ui/proposal` | Single proposal UI |

### OpenAPI Documentation

Access interactive API docs at: `http://localhost:8000/docs`

---

## Testing

### Run All Tests

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_data_products.py
pytest tests/test_endpoints.py
pytest tests/test_clients.py

# Run specific test
pytest tests/test_data_products.py::test_balances_handle
```

### Test Structure

- **`tests/test_data_products.py`** - Unit tests for data product classes
- **`tests/test_endpoints.py`** - API endpoint tests with mocked data products
- **`tests/test_clients.py`** - Client tests for data fetching
- **`tests/conftest.py`** - Shared pytest fixtures (ABI sets)
- **`tests/abis/`** - Test ABI files for various governors/tokens

---

## Docker Deployment

### Build Image

```bash
docker build -t daonode .
```

### Run Container

```bash
docker run -p 8000:8000 \
  -e AGORA_CONFIG_FILE="/app/config.yaml" \
  -e ABI_URL="http://your-abi-host.com/abis/" \
  -e CONTRACT_DEPLOYMENT="main" \
  -e DAO_NODE_ARCHIVE_NODE_HTTP="http://your-archive-node" \
  -e DAO_NODE_REALTIME_NODE_WS="ws://your-realtime-node" \
  -v /path/to/config.yaml:/app/config.yaml \
  daonode
```

### Docker Image Details

- **Base**: `python:3.11-slim`
- **Port**: 8000
- **Command**: `sanic app.server --host=0.0.0.0 --port=8000`
- **Includes**: `gsutil` for GCS sync, build tools for C extensions

---

## Code Formatting

The project uses **Black** for code formatting:

```bash
# Format code
black app/ tests/

# Check formatting
black --check app/ tests/
```

Configuration in `pyproject.toml`:
- Line length: 79
- Target versions: py37, py38

---

## Error Codes

DAO Node uses error codes in the format: `E{line_number}{YYMMDD}{suffix}`

This allows easy error creation without collision risk. Errors are logged rather than raising exceptions to keep the server running even with partial data issues.

---

## Performance Notes

- **Server Timing Header**: All responses include `server-timing` header showing processing time in milliseconds
- **O(1) Lookups**: Most endpoints use O(1) data structure lookups
- **In-RAM State**: All data is maintained in memory for fastest access
- **Boot Time**: Initial boot requires syncing archive data, then real-time follows

---

## Summary of Commands

| Command | Description |
|---------|-------------|
| `pip install -r requirements.txt` | Install dependencies |
| `python -m app.server` | Run server directly |
| `sanic app.server --host=0.0.0.0 --port=8000` | Run with Sanic CLI |
| `sanic app.server --dev --debug` | Run in development mode |
| `python -m app.cli sync-from-gcs data` | Sync archive data from GCS |
| `pytest` | Run all tests |
| `pytest -v tests/test_endpoints.py` | Run specific tests |
| `docker build -t daonode .` | Build Docker image |
| `docker run -p 8000:8000 daonode` | Run Docker container |
| `black app/ tests/` | Format code |
