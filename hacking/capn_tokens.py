import capnp
import tempfile
import random
import os
import subprocess
import csv
import time

def generate_valid_schema_id():
    result = subprocess.run(["capnp", "id"], capture_output=True)
    if result.returncode != 0:
        raise RuntimeError("Failed to generate schema ID: " + result.stderr.decode("utf-8"))
    return result.stdout.decode("utf-8").strip()

# Define the schema as a string, with a placeholder for the ID
SCHEMA_TEMPLATE = """__SCHEMA_ID__;

struct Payloads {
  blockNumber @0 :UInt32;
  transactionIndex @1 :UInt16;
  logIndex @2 :UInt16;
  fromAddress @3 :Text;
  toAddress @4 :Text;
  value @5 :Text;

}
"""

def load_schema_at_runtime():
    schema_id = generate_valid_schema_id()
    print(schema_id)
    full_schema = SCHEMA_TEMPLATE.replace("__SCHEMA_ID__", schema_id)

    with tempfile.NamedTemporaryFile(suffix=".capnp", mode="w+", delete=False) as tmp:
        tmp.write(full_schema)
        tmp.flush()
        schema_path = tmp.name

    try:
        loaded = capnp.load(schema_path)
    finally:
        os.unlink(schema_path)  # Clean up temporary file

    return loaded

# Example usage
payloads_capnp = load_schema_at_runtime()

# Define your event CSV files and the handler for each one
EVENT_FILES = {
    "Transfer(address,address,uint256).csv": "transfer",
    # "DelegateChanged(address,address,address).csv": "delegateChanged",
    # "DelegateVotesChanged(address,uint256,uint256).csv": "delegateVotesChanged",
}

CSV_DIR = '/Users/jm/code/dao_node/data/10/0x4200000000000000000000000000000000000042/'

def safe_get(d, k):
    return d[k] if k in d and d[k] is not None else ""

def parse_event(row, event_type, msg):
    if event_type == "transfer":
        msg.transfer = msg.init("transfer")
        msg.transfer.fromAddress = safe_get(row, "from")
        msg.transfer.toAddress = safe_get(row, "to")
        msg.transfer.value = safe_get(row, "value")

    elif event_type == "delegateChanged":
        msg.delegateChanged = msg.init("delegateChanged")
        msg.delegateChanged.delegator = safe_get(row, "delegator")
        msg.delegateChanged.fromDelegate = safe_get(row, "from_delegate")
        msg.delegateChanged.toDelegate = safe_get(row, "to_delegate")

    elif event_type == "delegateVotesChanged":
        msg.delegateVotesChanged = msg.init("delegateVotesChanged")
        msg.delegateVotesChanged.delegate = safe_get(row, "delegate")
        msg.delegateVotesChanged.previousBalance = safe_get(row, "previous_balance")
        msg.delegateVotesChanged.newBalance = safe_get(row, "new_balance")

def safe_int(val):
    try:
        return int(val)
    except:
        return 0

# Collect all events
all_events = []

start = time.perf_counter()

DO_WRITE = True 

for filename, event_type in EVENT_FILES.items():
    path = os.path.join(CSV_DIR, filename)
    with open(path, newline="") as csvfile:
        cnt = 0
        reader = csv.DictReader(csvfile)
        print(path)

        for row in reader:

            if cnt == 2000000:
                break
            
            cnt += 1

            row['block_number'] = int(row['block_number'])
            row['transaction_index'] = int(row['transaction_index'])
            row['log_index'] = int(row['log_index'])

            if int(row.get("block_number")) % 1000000 == 0:
                print(row)



            if DO_WRITE:

                # Make a sample message
                msg = payloads_capnp.Payloads.new_message()

                msg.blockNumber = safe_int(row.get("block_number"))
                msg.transactionIndex = safe_int(row.get("transaction_index"))
                msg.logIndex = safe_int(row.get("log_index"))
                msg.fromAddress = safe_get(row, "from")
                msg.toAddress = safe_get(row, "to")
                msg.value = safe_get(row, "value")

                # parse_event(row, event_type, msg)

                all_events.append(msg)

end = time.perf_counter()

print(f"Took {end - start} seconds to process {cnt} events.")


if DO_WRITE:
    # Sort events by timestamp
    all_events.sort(key=lambda e: e.blockNumber)

    # Write all events to file
    with open("events_tokens.capnp", "w+b") as f:
        for msg in all_events:
            msg.write_packed(f)

start = time.perf_counter()

cnt = 0
with open("events_tokens.capnp", "r+b") as f:
    for msg in payloads_capnp.Payloads.read_multiple_packed(f):
        cnt += 1
        
        if msg.blockNumber % 1000000 ==  0:
            print(msg)
            
end = time.perf_counter()

print(f"Took {end - start} seconds to read {cnt} events.")