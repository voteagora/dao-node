import os
import json
import asyncio
import psycopg2
import psycopg2.extras

from collections import defaultdict
from sanic.log import logger as logr

from abifsm import ABISet

from .clients_csv import SubscriptionPlannerMixin
from .utils import camel_to_snake
from .signatures import DELEGATE_CHANGED_2, VOTE_CAST_1, VOTE_CAST_WITH_PARAMS_1, \
    PROPOSAL_CREATED_1, PROPOSAL_CREATED_2, PROPOSAL_CREATED_3, PROPOSAL_CREATED_4, \
    PROPOSAL_CREATED_MODULE


# Mapping from chain_id to the blocks table suffix in the goldsky schema.
# Mirrors CHAIN_ID_TO_BLOCKS_TABLE from the etl repo's common/constants.py.
CHAIN_ID_TO_BLOCKS_TABLE = {
    1:        'ethereum',
    11155111: 'ethereum_sepolia',
    10:       'optimism',
    11155420: 'optimism_sepolia',
    8453:     'base',
    7560:     'cyber',
    534352:   'scroll',
    957:      'derive',
    901:      'derive_testnet',
    42161:    'arbitrum_one',
    421614:   'arbitrum_sepolia',
    480:      'worldchain',
    11011:    'shape_sepolia',
    360:      'shape',
}


class DbClientCaster:
    """Casts DB row values to match the format expected by dao-node data products.
    
    DB returns native Python types (int, str, bytes, etc.) rather than CSV strings,
    so most casting is lighter than CSVClientCaster.  Special-case signatures still
    need explicit handling to match what the data products expect.
    """
    
    def __init__(self, abis):
        self.abis = abis

    @staticmethod
    def _coerce_raw_values(row):
        """Convert memoryview / bytes DB values to hex strings, and numeric types to int."""
        from decimal import Decimal
        for key in list(row.keys()):
            val = row[key]
            if isinstance(val, memoryview):
                row[key] = val.tobytes().hex()
            elif isinstance(val, bytes):
                row[key] = val.hex()
            elif isinstance(val, Decimal):
                row[key] = int(val)
            elif isinstance(val, str) and key in ('value', 'amount', 'weight', 'previous_balance', 'new_balance', 'old_balance', 'start_block', 'end_block', 'proposal_id', 'eta', 'proposal_type', 'support'):
                # Convert numeric string fields to int
                try:
                    row[key] = int(val)
                except (ValueError, TypeError):
                    pass
        return row

    def lookup(self, signature):

        if signature == DELEGATE_CHANGED_2:

            def caster_fn(row):
                row = self._coerce_raw_values(row)
                for field in ('old_delegatees', 'new_delegatees'):
                    val = row.get(field)
                    if isinstance(val, str):
                        try:
                            val = json.loads(val)
                        except json.JSONDecodeError:
                            val = []
                    if isinstance(val, list):
                        parsed = []
                        for x in val:
                            if isinstance(x, dict):
                                parsed.append((x.get('_delegatee', x.get('delegatee', '')).lower(),
                                               int(x.get('_numerator', x.get('numerator', 0)))))
                            elif isinstance(x, (list, tuple)) and len(x) >= 2:
                                parsed.append((str(x[0]).lower(), int(x[1])))
                            else:
                                parsed.append(x)
                        row[field] = parsed
                return row

        elif signature == VOTE_CAST_1:

            def caster_fn(row):
                row = self._coerce_raw_values(row)
                if 'voter' in row:
                    row['voter'] = str(row['voter']).lower()
                return row

        elif signature == VOTE_CAST_WITH_PARAMS_1:

            def caster_fn(row):
                row = self._coerce_raw_values(row)
                if 'voter' in row:
                    row['voter'] = str(row['voter']).lower()
                if 'params' in row:
                    val = row['params']
                    if isinstance(val, (bytes, memoryview)):
                        row['params'] = bytes(val).hex()
                return row

        elif signature in (PROPOSAL_CREATED_1, PROPOSAL_CREATED_2,
                           PROPOSAL_CREATED_3, PROPOSAL_CREATED_4):

            def caster_fn(row):
                row = self._coerce_raw_values(row)
                for field in ('targets', 'calldatas', 'signatures', 'values'):
                    val = row.get(field)
                    if isinstance(val, str) and val.startswith('['):
                        try:
                            row[field] = json.loads(val)
                        except json.JSONDecodeError:
                            pass
                return row

        elif signature == PROPOSAL_CREATED_MODULE:

            def caster_fn(row):
                row = self._coerce_raw_values(row)
                for field in ('settings', 'options'):
                    val = row.get(field)
                    if isinstance(val, str):
                        try:
                            row[field] = json.loads(val)
                        except json.JSONDecodeError:
                            pass
                return row

        else:

            def caster_fn(row):
                return self._coerce_raw_values(row)

        return caster_fn


class DbPollingClient(SubscriptionPlannerMixin):
    """Polls a Postgres DB (goldsky schema) for new events / blocks.

    Replaces the WebSocket realtime client (JsonRpcRtWsClient) and the
    HTTP polling client (JsonRpcRtHttpClient).  The DB is populated by the
    Goldsky / Center indexer and is the same source that the ETL
    daonode_checkpoints pipeline reads from.

    Constructor args:
        db_url            – Postgres connection string (env: DAO_NODE_DB_URL)
        db_table_prefix   – ABISet name prefix matching the DB tables
                            (env: DAO_NODE_DB_TABLE_PREFIX, e.g. "multi_scroll")
        db_schema         – Postgres schema (default "goldsky")
        name              – human-friendly label for logging
    """

    timeliness = 'polling'

    def __init__(self, db_url, db_table_prefix, db_schema='goldsky', name='DBPOLL', batch_size=50000):
        self.db_url = db_url
        self.db_table_prefix = db_table_prefix
        self.db_schema = db_schema
        self.name = name
        self.last_seen_block = 0
        self.max_block = None
        self.batch_size = batch_size  # Process blocks in batches to avoid memory issues

        self.init()
        self.casterCls = DbClientCaster

        self._event_plans = []
        self._block_plans = []

    # -- SubscriptionPlannerMixin override ------------------------------------

    def set_abis(self, abi_set: ABISet):
        assert not self.abis_set

        self.abis_set = True
        self.abis = abi_set
        self.caster = self.casterCls(self.abis)

        # Create a DB-specific ABISet whose pgtable() output matches the
        # actual table names stored in the database.
        self.db_abis = ABISet(self.db_table_prefix, abi_set.abis)

    # -- Planning -------------------------------------------------------------

    def plan_event(self, chain_id, address, signature):

        abi_frag = self.abis.get_by_signature(signature)
        if abi_frag is None:
            logr.warning(f"{self.name}: No ABI fragment for {signature}, skipping DB plan")
            return

        caster_fn = self.caster.lookup(signature)

        table_name = self.db_abis.pgtable(abi_frag)
        fields = [camel_to_snake(f) for f in abi_frag.fields]

        self._event_plans.append({
            'chain_id':   chain_id,
            'address':    address,
            'signature':  signature,
            'table_name': table_name,
            'fields':     fields,
            'caster_fn':  caster_fn,
            'abi_frag':   abi_frag,
        })

        logr.info(f"{self.name}: planned event {signature} → {self.db_schema}.{table_name}")

    def plan_block(self, chain_id):

        blocks_suffix = CHAIN_ID_TO_BLOCKS_TABLE.get(chain_id)
        if blocks_suffix is None:
            logr.warning(f"{self.name}: No blocks table mapping for chain_id={chain_id}")
            return

        self._block_plans.append({
            'chain_id':   chain_id,
            'table_name': f'blocks_{blocks_suffix}',
        })

        logr.info(f"{self.name}: planned blocks → {self.db_schema}.blocks_{blocks_suffix}")

    # -- Validity / connectivity ---------------------------------------------

    def is_valid(self):
        if not self.db_url:
            logr.info(f"{self.name}: No DB URL configured, client not valid")
            return False
        try:
            conn = psycopg2.connect(self.db_url)
            conn.close()
            logr.info(f"{self.name}: DB connection is valid")
            return True
        except Exception as e:
            logr.info(f"{self.name}: DB connection failed: {e}")
            return False

    def _connect(self):
        return psycopg2.connect(self.db_url)

    # -- External helpers -----------------------------------------------------

    def set_last_seen_block(self, block):
        """Sync with the archive's latest block before polling begins."""
        self.last_seen_block = block
        logr.info(f"{self.name}: last_seen_block set to {block}")

    def set_max_block(self, block):
        """Cap DB queries at this block number (inclusive)."""
        self.max_block = block
        logr.info(f"{self.name}: max_block set to {block}")

    # -- Async read (polling interface) ---------------------------------------

    async def read(self):
        """Async generator that polls the DB for new events since last_seen_block.

        Output format matches JsonRpcRtHttpClient / JsonRpcRtWsClient:
        each yielded dict contains ``signal``, ``signature``, ``sighash``,
        ``block_number``, ``transaction_index``, ``log_index``, plus
        event-specific fields.
        """

        logr.info(f"{self.name}: read() called, about to fetch events")
        loop = asyncio.get_event_loop()

        all_events = await loop.run_in_executor(None, self._fetch_new_events)
        logr.info(f"{self.name}: _fetch_new_events returned {len(all_events)} events")

        all_events.sort(key=lambda x: (
            int(x['block_number']),
            x.get('transaction_index', -1),
            x.get('log_index', -1),
        ))

        max_block = self.last_seen_block

        for event in all_events:
            block_num = int(event['block_number'])
            if block_num > max_block:
                max_block = block_num
            yield event

        if max_block > self.last_seen_block:
            # Don't advance beyond max_block if set (respects DAO_NODE_MAX_BLOCK)
            if self.max_block:
                max_block = min(max_block, self.max_block)
            logr.info(f"{self.name}: advanced last_seen_block {self.last_seen_block} → {max_block}")
            self.last_seen_block = max_block

    # -- Blocking DB fetch (runs in executor) ---------------------------------

    def _fetch_new_events(self):
        """Query the DB for all blocks + events newer than last_seen_block."""

        all_events = []

        logr.info(f"{self.name}: _fetch_new_events called with last_seen_block={self.last_seen_block}, max_block={self.max_block}")
        logr.info(f"{self.name}: DB config: schema={self.db_schema}, table_prefix={self.db_table_prefix}, batch_size={self.batch_size}")
        logr.info(f"{self.name}: {len(self._block_plans)} block plans, {len(self._event_plans)} event plans")
        
        # Log all plans for verification
        for i, plan in enumerate(self._block_plans):
            logr.info(f"{self.name}: Block plan {i}: chain_id={plan['chain_id']}, table={self.db_schema}.{plan['table_name']}")
        for i, plan in enumerate(self._event_plans):
            logr.info(f"{self.name}: Event plan {i}: {plan['signature']}, address={plan['address']}, table={self.db_schema}.{plan['table_name']}")

        # Calculate block range and split into batches
        start_block = self.last_seen_block
        end_block = self.max_block if self.max_block else start_block + self.batch_size
        total_blocks = end_block - start_block
        
        # Create batch ranges
        batch_ranges = []
        current = start_block
        while current < end_block:
            batch_end = min(current + self.batch_size, end_block)
            batch_ranges.append((current, batch_end))
            current = batch_end
        
        if len(batch_ranges) > 1:
            logr.info(f"{self.name}: Splitting {total_blocks} blocks into {len(batch_ranges)} batches of ~{self.batch_size} blocks")
        
        try:
            conn = self._connect()
            
            # Process each batch
            for batch_idx, (batch_start, batch_end) in enumerate(batch_ranges):
                logr.info(f"{self.name}: Processing batch {batch_idx + 1}/{len(batch_ranges)}: blocks {batch_start} to {batch_end}")
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # ---- blocks ----
                for plan in self._block_plans:
                    try:
                        import time
                        table_full = f"{self.db_schema}.{plan['table_name']}"
                        query = f"""
                            SELECT block_number, timestamp
                            FROM {self.db_schema}.{plan['table_name']}
                            WHERE block_number > %s AND block_number <= %s
                            ORDER BY block_number DESC
                        """
                        start_time = time.time()
                        cur.execute(query, (batch_start, batch_end))
                        
                        row_count = 0
                        for row in cur:
                            row_count += 1
                            event = dict(row)
                            event['block_number'] = int(event['block_number'])
                            event['timestamp'] = int(event['timestamp'])
                            event['signal'] = f"{plan['chain_id']}.blocks"
                            all_events.append(event)
                        
                        elapsed = time.time() - start_time
                        if row_count > 0:
                            logr.info(f"{self.name}: batch {batch_idx + 1}: {row_count} blocks from {table_full} ({elapsed:.2f}s)")

                    except Exception as e:
                        logr.error(f"{self.name}: blocks query error ({plan['table_name']}): {e}")

                # ---- events ----
                for plan in self._event_plans:
                    try:
                        import time
                        fields_sql = ''
                        if plan['fields']:
                            fields_sql = ', ' + ', '.join(f'"{f}"' for f in plan['fields'])

                        table_full = f"{self.db_schema}.{plan['table_name']}"
                        query = f"""
                            SELECT *
                            FROM {self.db_schema}.{plan['table_name']}
                            WHERE address = %s AND block_number > %s AND block_number <= %s
                            ORDER BY block_number DESC, transaction_index DESC, log_index DESC
                        """
                        start_time = time.time()
                        cur.execute(query, (plan['address'], batch_start, batch_end))

                        row_count = 0
                        for row in cur:
                            row_count += 1
                            event = dict(row)
                            
                            # Filter out DB metadata columns and unnecessary fields
                            db_metadata_fields = {'id', 'event_name', 'block_hash', 'transaction_hash', 'chain_id', 'address'}
                            for field in db_metadata_fields:
                                event.pop(field, None)
                            
                            event['block_number']      = str(event['block_number'])
                            event['transaction_index']  = int(event['transaction_index'])
                            event['log_index']          = int(event['log_index'])
                            event['signature']          = plan['signature']
                            event['sighash']            = plan['abi_frag'].topic
                            event['signal']             = f"{plan['chain_id']}.{plan['address']}.{plan['signature']}"

                            event = plan['caster_fn'](event)
                            all_events.append(event)
                        
                        elapsed = time.time() - start_time
                        if row_count > 0:
                            logr.info(f"{self.name}: batch {batch_idx + 1}: {row_count} {plan['signature']} from {table_full} ({elapsed:.2f}s)")

                    except Exception as e:
                        logr.error(f"{self.name}: event query error ({plan['table_name']}): {e}")
                
                cur.close()
                logr.info(f"{self.name}: Batch {batch_idx + 1}/{len(batch_ranges)} complete, {len(all_events)} total events so far")

            conn.close()

        except Exception as e:
            logr.error(f"{self.name}: DB connection error during fetch: {e}")

        logr.info(f"{self.name}: fetched {len(all_events)} new events/blocks (after block {self.last_seen_block})")

        return all_events
