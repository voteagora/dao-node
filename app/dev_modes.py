

import os

from dotenv import load_dotenv

load_dotenv()

CAPTURE_CLIENT_OUTPUTS_TO_DISK = bool(os.getenv('CAPTURE_CLIENT_OUTPUTS_TO_DISK', False))
CAPTURE_WS_CLIENT_OUTPUTS = bool(os.getenv('CAPTURE_WS_CLIENT_OUTPUTS', True))
PROFILE_ARCHIVE_CLIENT = bool(os.getenv('PROFILE_ARCHIVE_CLIENT', False))

ENABLE_BALANCES = bool(os.getenv('ENABLE_BALANCES', True))
ENABLE_DELEGATION = bool(os.getenv('ENABLE_DELEGATION', True))

# Set to a block number to stop ingesting events beyond that block.
# Used for snapshot-based testing: index to block X, snapshot endpoints,
# then compare against a DB-fed run that also caps at block X.
# Set to 0 or leave unset to disable (no cap).
DAO_NODE_MAX_BLOCK = int(os.getenv('DAO_NODE_MAX_BLOCK', 0))

# When set, archive clients (CSV / HTTP) stop at this block and the
# DbPollingClient takes over from here, syncing up to DAO_NODE_MAX_BLOCK.
# Set to 0 or leave unset to disable (archive reads everything available).
DAO_NODE_DB_SYNC_FROM_BLOCK = int(os.getenv('DAO_NODE_DB_SYNC_FROM_BLOCK', 0))