# Plan: Remove WebSocket, Replace with DB Polling

## Context

- **dao-node** currently uses WebSocket (`JsonRpcRtWsClient`) and HTTP polling (`JsonRpcRtHttpClient`) for real-time event sync after GCS archive boot.
- **etl** repo's `daonode_checkpoints.py` already queries the Postgres DB (`goldsky.*` schema) for all events and exports to GCS CSV.
- **abifsm** library provides `ABISet.pgtable(event)` and `FQPGSqlGen` to map event signatures/topics → Postgres table names.

## Current Data Flow

```
[Goldsky/Center Indexer] → [Postgres DB (goldsky schema)]
                                    ↓
                        [ETL daonode_checkpoints.py]
                                    ↓
                            [GCS CSV files]
                                    ↓
            dao-node boot: CSVClient + JsonRpcHistHttpClient
                                    ↓
            dao-node realtime: JsonRpcRtWsClient (WS) + JsonRpcRtHttpClient (poll)
```

## New Data Flow

```
[Goldsky/Center Indexer] → [Postgres DB (goldsky schema)]
                                    ↓
            dao-node boot: CSVClient (GCS archive, unchanged)
                                    ↓
            dao-node ongoing: DbPollingClient (queries DB directly)
```

## Implementation Steps

### Step 1: Create `app/clients_db.py`
- New `DbPollingClient` implementing `SubscriptionPlannerMixin`
- Uses `abifsm.FQPGSqlGen` to resolve event signature → `goldsky.<table_name>`
- `plan_event(chain_id, address, signature)` → stores table + field mapping
- `plan_block(chain_id)` → stores blocks table reference
- `timeliness = 'polling'`
- Async `read()` method: queries `WHERE block_number > last_seen AND address = X ORDER BY block_number, tx_index, log_index`
- Reuses `CSVClientCaster` for type casting (DB returns similar types to CSV)

### Step 2: Wire into `server.py`
- Add `DbPollingClient` to client list in `bootstrap_data_feeds`
- New env var: `DAO_NODE_DB_URL` for Postgres connection string
- Remove `JsonRpcRtWsClient` instantiation
- Remove `JsonRpcRtHttpClient` instantiation (or keep as optional fallback)
- Simplify `subscribe_feeds` — remove `read_realtime`, keep `read_polling` with DB client

### Step 3: Clean up WS code
- Remove/deprecate `clients_wsjson.py`
- Remove WS-related env vars from boot sequence
- Keep `clients_wsvpsnapper.py` if `non_ivotes_vp` feature still needs it

### Step 4: Update dependencies
- Add `asyncpg` (or `psycopg2` + `sqlalchemy`) to `requirements.txt`
- Mark `websocket-client`, `websockets` as optional (still needed for VPSnapper)

## Key Interface Contract

Each client must implement:
- `SubscriptionPlannerMixin` (provides `init()`, `set_abis()`, `plan()`)
- `plan_event(chain_id, address, signature)` 
- `plan_block(chain_id)`
- `is_valid() -> bool`
- `get_fallback_block() -> int` (archive only)
- `read(after=block)` → yields `(event_dict, signal_str, new_signal_bool)` for archive
- `async read()` → async yields `event_dict` with `signal` key for realtime/polling

Event dict format:
```python
{
    'block_number': str,       # string for archive, int sometimes for realtime
    'transaction_index': int,
    'log_index': int,
    'signature': str,          # e.g. 'Transfer(address,address,uint256)'
    'sighash': str,            # topic hex without 0x prefix
    'signal': str,             # e.g. '10.0x4200...0042.Transfer(address,address,uint256)' (realtime only)
    # ... event-specific fields in snake_case
}
```

## DB Table Schema (goldsky.*)

Tables follow `abifsm.ABISet.pgtable()` naming: `multi_<tenant>_<contract>_<event_slug>`

Columns: `block_number`, `transaction_index`, `log_index`, `address`, `sighash`, + event-specific fields in snake_case.

## Risks & Mitigations
- **DB availability**: If DB is down, dao-node can't sync. Mitigation: keep CSV archive for cold start.
- **Latency**: DB indexing has ~seconds delay vs WS real-time. Acceptable for most use cases.
- **Schema differences**: DB field names may differ slightly from WS/HTTP caster output. Need careful mapping via `abifsm`.
