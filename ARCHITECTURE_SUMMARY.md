# DAO Node - Architecture Summary

A concise overview of the DAO Node codebase: a high-performance, in-RAM blockchain data service for DAO governance applications.

---

## What It Does

DAO Node ingests on-chain governance events (transfers, delegations, proposals, votes) from archive and real-time sources, maintains their state entirely in memory, and exposes that state via a fast REST API built on Sanic.

---

## High-Level Architecture

```
                        ┌────────────────────────────────────────────┐
                        │              YAML Config File              │
                        │   (contracts, chain_id, governor spec)     │
                        └──────────────────┬─────────────────────────┘
                                           │
                        ┌──────────────────▼─────────────────────────┐
                        │              Boot Sequence                 │
                        │  1. Load ABIs from internet (abifsm)       │
                        │  2. Instantiate Data Products              │
                        │  3. Register event → data-product mappings │
                        │  4. Read archive clients (sync)            │
                        │  5. Start realtime + polling clients       │
                        └──────────────────┬─────────────────────────┘
                                           │
        ┌──────────────────────────────────▼──────────────────────────────────┐
        │                        ClientSequencer                              │
        │   Sequences multiple data clients in a defined order                │
        │                                                                     │
        │  ┌──────────────┐  ┌──────────────────┐  ┌───────────────────────┐  │
        │  │  CSVClient   │  │ JsonRpcHistHttp   │  │ JsonRpcRtWsClient(s)  │  │
        │  │  (archive)   │  │ Client (archive)  │  │ (realtime WebSocket)  │  │
        │  └──────┬───────┘  └────────┬──────────┘  └───────────┬───────────┘  │
        │         │                   │                         │              │
        │  ┌──────┴───────────────────┴─────────────────────────┴───────────┐  │
        │  │                         Feed                                   │  │
        │  │  • Reads archive data synchronously at boot                    │  │
        │  │  • Reads realtime data asynchronously post-boot                │  │
        │  │  • Deduplicates events across competing WebSocket connections  │  │
        │  │  • Tracks signal counts (archive vs realtime)                  │  │
        │  └───────────────────────────┬────────────────────────────────────┘  │
        └──────────────────────────────┼──────────────────────────────────────┘
                                       │
                                       ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │                     DataProductContext                               │
        │                                                                      │
        │  Routes each event (by chain_id.contract.signature) to the           │
        │  registered Data Product(s) that handle it.                          │
        │                                                                      │
        │  ┌────────────┐ ┌─────────────┐ ┌───────────┐ ┌───────┐             │
        │  │  Balances   │ │ Delegations │ │ Proposals │ │ Votes │  ...        │
        │  └──────┬─────┘ └──────┬──────┘ └─────┬─────┘ └───┬───┘             │
        │         └──────────────┴───────────────┴───────────┘                 │
        │                            In-RAM State                              │
        └─────────────────────────────────┬────────────────────────────────────┘
                                          │
                                          ▼
        ┌──────────────────────────────────────────────────────────────────────┐
        │                     Sanic Web Server (API)                           │
        │                                                                      │
        │  /v1/balance/<addr>          /v1/proposals       /v1/delegates       │
        │  /v1/proposal/<id>           /v1/vote_record     /v1/delegate/<addr> │
        │  /v1/voting_power            /v1/voter_history   /health  /docs      │
        └──────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

| Path | Purpose |
|------|---------|
| `app/server.py` | Main entry point: Sanic app, API endpoints, boot sequence, client orchestration |
| `app/data_products.py` | Core in-RAM data structures: `Balances`, `Delegations`, `Proposals`, `Votes`, `ProposalTypes`, `NonIVotesVP` |
| `app/data_models.py` | Derived models: `ParticipationRateModel` (computed from data products) |
| `app/abcs.py` | Abstract base classes: `DataProduct`, `DataModel` |
| `app/signatures.py` | Solidity event signature constants |
| `app/clients_csv.py` | `CSVClient` - reads historical archive data from local CSV files |
| `app/clients_httpjson.py` | `JsonRpcHistHttpClient` (archive via HTTP), `JsonRpcRtHttpClient` (polling) |
| `app/clients_wsjson.py` | `JsonRpcRtWsClient` - real-time event subscription via WebSocket |
| `app/clients_wsvpsnapper.py` | `VPSnappercWsClient` - specialized WebSocket client for non-IVotes VP snapshots |
| `app/middleware.py` | Request timing middleware (`Server-Timing` header) |
| `app/profiling.py` | `Profiler` class for performance measurement during boot |
| `app/cli.py` | CLI tool for syncing archive data from Google Cloud Storage |
| `app/dev_modes.py` | Feature flags for development (capture outputs, profiling, enable/disable features) |
| `app/logsetup.py` | Logging configuration |
| `app/utils.py` | Utility: `camel_to_snake` converter |
| `tests/` | Test suite: data product unit tests, endpoint tests, client tests |
| `static/` | Static HTML/CSS for UI pages (index, proposals, delegates) |

---

## Core Concepts

### 1. Data Products

Data Products are in-RAM singletons that consume blockchain events and maintain materialized state. Each inherits from `DataProduct` (ABC) and implements a `handle(event)` method.

| Data Product | Consumes | Maintains |
|---|---|---|
| **`Balances`** | `Transfer` events | Token balances per address (`defaultdict(int)`) |
| **`Delegations`** | `DelegateChanged`, `DelegateVotesChanged`, block headers | Delegatee lists, VP history, 7-day VP change, delegator mappings |
| **`Proposals`** | `ProposalCreated/Canceled/Queued/Executed` | Proposal objects with lifecycle state, participation rate tracking |
| **`Votes`** | `VoteCast`, `VoteCastWithParams` | Per-proposal aggregations, voter history, vote records |
| **`ProposalTypes`** | `ProposalTypeSet`, `ScopeCreated/Deleted/Disabled` | Proposal type configs with scope state machines |
| **`NonIVotesVP`** | Off-chain VP snapshots (via WebSocket) | Historical VP snapshots indexed by block and timestamp |

### 2. Data Models

Data Models are derived computations that read from multiple Data Products.

- **`ParticipationRateModel`** - Computes delegate participation rates from `Proposals`, `Votes`, and `Delegations`. Refreshes lazily when proposal state changes.

### 3. Client System

Clients are data sources, sequenced by `ClientSequencer`:

| Client | Timeliness | Transport | Role |
|---|---|---|---|
| `CSVClient` | archive | Local filesystem | Fastest boot from pre-synced GCS data |
| `JsonRpcHistHttpClient` | archive | HTTP JSON-RPC | Catches up from CSV's last block to chain tip |
| `JsonRpcRtWsClient` | realtime | WebSocket | Live event subscription (multiple instances for redundancy) |
| `JsonRpcRtHttpClient` | polling | HTTP JSON-RPC | Fallback polling to catch missed WebSocket events |
| `VPSnappercWsClient` | realtime | WebSocket | Off-chain VP snapshot feed with exponential backoff reconnect |

### 4. Event Routing

Events are routed using a signal key of the form `{chain_id}.{contract_address}.{event_signature}`. The `DataProductContext` maintains a mapping from these keys to lists of Data Products. During archive reads, events are dispatched synchronously; during real-time, they are dispatched asynchronously.

### 5. Feed

The `Feed` class orchestrates data ingestion:

- **Archive phase** (boot): iterates clients in order, yielding events synchronously
- **Realtime phase** (post-boot): async generators from WebSocket and polling clients
- **Deduplication**: tracks `(transaction_index, log_index)` per block to avoid processing duplicate events from competing WebSocket connections
- **Signal counting**: tracks archive vs realtime event counts per signal for diagnostics

---

## Boot Sequence

1. **Config** - Load YAML config, resolve deployment contracts and chain ID
2. **ABIs** - Fetch ABIs from hosted URL via `abifsm` library
3. **Clients** - Instantiate CSV, HTTP, WebSocket, and polling clients
4. **Data Products** - Create and register data products for their respective event signatures
5. **Archive Sync** (`before_server_start`) - Read archive clients in sequence, dispatch events to data products
6. **Index** (`after_server_start`) - Compute participation rates
7. **Realtime** (`after_server_start`) - Start WebSocket subscribers and polling clients as async tasks

---

## API Surface

### Health & Config
- `GET /health` - Server status, version, environment info
- `GET /config` - Governor and token spec
- `GET /deployment` - Smart contract addresses

### Token State
- `GET /v1/balance/<addr>` - ERC20 balance (if enabled)

### Proposals
- `GET /v1/proposals` - All proposals with vote totals (filterable: `?set=relevant`)
- `GET /v1/proposal/<id>` - Single proposal detail with aggregates
- `GET /v1/proposal_types` - Proposal type configurations with scope state
- `GET /v1/vote_record/<id>` - Paginated voting record (sortable by block or VP)
- `GET /v1/vote?proposal_id=X&voter=Y` - Specific vote lookup
- `GET /v1/voter_history/<voter>` - Delegate's voting history

### Delegation
- `GET /v1/delegates` - Sorted, paginated delegate list (sort by VP, DC, PR, MRD, OLD, LVB, VPC)
- `GET /v1/delegate/<addr>` - Delegate detail with delegator list and participation rate
- `GET /v1/delegate_vp/<addr>/<block>` - Historical voting power via bisect search
- `GET /v1/voting_power` - Total DAO voting power

### Non-IVotes VP (conditional)
- `GET /v1/nonivotes/total` - Total non-IVotes VP
- `GET /v1/nonivotes/total/at-block/<block>` - Historical total
- `GET /v1/nonivotes/user/<addr>/at-block/<block>` - User VP at block
- `GET /v1/nonivotes/all/at-block/<block>` - All user VP at block

### Diagnostics
- `GET /v1/progress` - Current block, signal counts, boot time
- `GET /v1/integrity` - Data integrity checks (address casing, type consistency)
- `GET /v1/ram/<worker_id>` - Memory usage per data product (in KB)
- `GET /v1/diagnostics/<safe_mode>` - Event history dump

### UI
- `GET /` - Index page
- `GET /ui/proposals` - Proposals page
- `GET /ui/delegates` - Delegates page
- `GET /ui/proposal` - Single proposal page

---

## Performance Design

- **O(1) lookups** for most endpoints (balances, proposals, individual delegates)
- **O(log n) bisect** for historical voting power queries
- **O(n log n)** sort at request time for the delegates list (planned improvement: pre-sorted indexes)
- **`Server-Timing` header** on every response with `data` (business logic) and `total` (full request) durations
- **In-RAM state** eliminates database round-trips entirely
- **Event deduplication** across redundant WebSocket connections

---

## Governor Support

The system adapts to multiple governor implementations via the YAML config's `governor_spec`:

| Governor | Proposal Events | Notes |
|---|---|---|
| **Compound / ENSGovernor** | `ProposalCreated` (v1) | Standard proposals only, no `VoteCastWithParams` |
| **Agora 0.1** | All 4 `ProposalCreated` variants | Reverse-engineers module from calldata |
| **Agora 1.x** | `ProposalCreated` v2 & v4 | PTC scopes v1 |
| **Agora 2.x** | `ProposalCreated` v1 + Module | PTC scopes v2, proposal type from description |

---

## Key Dependencies

| Package | Role |
|---|---|
| `sanic` + `sanic-ext` | Async web framework + OpenAPI |
| `web3` | Ethereum JSON-RPC and ABI processing |
| `websockets` | Real-time WebSocket subscriptions |
| `abifsm` | ABI fetching and event signature resolution (Agora library) |
| `sortedcontainers` | `SortedDict` for efficient VP history and bisect queries |
| `google-cloud-storage` | GCS archive sync (via `gsutil` CLI) |
| `pympler` | Runtime memory profiling |
| `ujson` | Fast JSON serialization |
| `uvloop` | High-performance async event loop |

---

## Testing

- **`tests/test_data_products.py`** - Unit tests for all data product `handle()` methods with static event data
- **`tests/test_endpoints.py`** - API endpoint tests using Sanic's test client with mocked data products
- **`tests/test_clients.py`** - Client tests for CSV and JSON-RPC data fetching/parsing
- **`tests/conftest.py`** - Shared fixtures (ABI sets from `tests/abis/`)

Run: `pytest -v`

---

## Deployment

- **Docker**: `python:3.11-slim` base, exposes port 8000, runs via `sanic app.server --host=0.0.0.0 --port=8000`
- **CI/CD**: GitHub Actions workflows for tests (`python-tests.yml`) and Docker builds (`publish-docker.yml`, `manual-docker-build.yml`)
- **Archive data**: Pre-synced via `python -m app.cli sync-from-gcs data` from GCS buckets

---

## Error Handling Philosophy

DAO Node uses a **warn-and-continue** approach. Known failure modes produce error codes (format: `E{line}{YYMMDD}{suffix}`) and are logged rather than raising exceptions. The rationale: one corrupted event shouldn't prevent the server from serving data for the rest of the DAO.
