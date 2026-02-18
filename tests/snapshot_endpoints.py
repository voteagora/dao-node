"""
Snapshot Endpoints Script
=========================

Captures responses from all 21 DAO Node API endpoints and uploads them
as individual JSON files to a GCS bucket for later comparison.

Endpoints tested (all 21):
    Static:
     1. GET /health
     2. GET /config
     3. GET /deployment
     4. GET /v1/progress
     5. GET /v1/voting_power
     6. GET /v1/integrity
     7. GET /v1/proposals
     8. GET /v1/proposal_types

    Per-proposal (auto-discovered from /v1/proposals):
     9. GET /v1/proposal/<proposal_id>
    10. GET /v1/vote_record/<proposal_id>?full=true
    11. GET /v1/vote?proposal_id=<id>&voter=<addr>

    Per-delegate (auto-discovered from /v1/delegates):
    12. GET /v1/balance/<addr>
    13. GET /v1/delegates?sort_by=<VP|DC|MRD|OLD|LVB|VPC>
    14. GET /v1/delegate/<addr>
    15. GET /v1/delegate/<addr>/voting_history
    16. GET /v1/delegate_vp/<addr>/<block_number>
    17. GET /v1/voter_history/<voter>

    NonIVotes (conditional, may 404):
    18. GET /v1/nonivotes/total
    19. GET /v1/nonivotes/total/at-block/<block_number>
    20. GET /v1/nonivotes/user/<addr>/at-block/<block_number>
    21. GET /v1/nonivotes/all/at-block/<block_number>

GCS Structure:
    gs://<DAONODE_DATA_COMPARE>/<block_number>-<chain_slug>/<label>/<section>.json

Usage:
    python tests/snapshot_endpoints.py --label old
    python tests/snapshot_endpoints.py --label new
    python tests/snapshot_endpoints.py --label old --no-gcs
    python tests/snapshot_endpoints.py --label old --base-url http://localhost:8004
    python tests/snapshot_endpoints.py --label old --chain-slug optimism --block 135000000
"""

import argparse
import json
import os
import time
from datetime import datetime

import requests


# ─── Helpers ──────────────────────────────────────────────────────────────────

def fetch(session, base_url, path, label, errors, quiet=False):
    """GET base_url+path, return parsed JSON or None."""
    url = f"{base_url}{path}"
    try:
        resp = session.get(url, timeout=60)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        errors.append({"endpoint": label, "url": url, "error": str(e)})
        if not quiet:
            print(f"  x {label}: {e}")
        return None


def strip_volatile(data, label):
    """Remove fields that change between runs (timestamps, worker IDs, etc)."""
    if data is None:
        return None
    if label == "health":
        data.pop("ip_address", None)
        data.pop("env", None)
    if label == "progress":
        data.pop("boot_time", None)
        data.pop("worker_id", None)
        data.pop("git_commit_sha", None)
        data.pop("realtime_counts", None)
    return data


# ─── Auto-discovery ───────────────────────────────────────────────────────────

def discover(session, base_url, page_size, errors):
    """Auto-discover proposal IDs, delegate addresses, block number, and a voter sample."""

    # Block number from progress
    progress = fetch(session, base_url, "/v1/progress", "discover_progress", errors)
    block = str(progress.get("block", 0)) if progress else "0"

    # Proposal IDs
    proposals_resp = fetch(session, base_url, "/v1/proposals", "discover_proposals", errors)
    proposal_ids = []
    if proposals_resp and "proposals" in proposals_resp:
        proposal_ids = [p["id"] for p in proposals_resp["proposals"]]

    # Delegate addresses (limit discovery to 50 to reduce data size)
    delegates_resp = fetch(session, base_url,
                           f"/v1/delegates?page_size=50&sort_by=VP&include=VP,DC",
                           "discover_delegates", errors)
    delegate_addrs = []
    if delegates_resp and "delegates" in delegates_resp:
        delegate_addrs = [d["addr"] for d in delegates_resp["delegates"]]

    # Find a (proposal_id, voter) pair for endpoint #11
    # Try silently — don't pollute errors list with discovery attempts
    vote_sample = None
    discovery_errors = []
    for pid in proposal_ids[:5]:  # only try first 5
        vr = fetch(session, base_url,
                   f"/v1/vote_record/{pid}?page_size=1",
                   "discover_vote_sample", discovery_errors)
        if vr and vr.get("vote_record"):
            voter = vr["vote_record"][0].get("voter") or vr["vote_record"][0].get("address")
            if voter:
                vote_sample = (pid, voter)
                break

    print(f"  Discovered: {len(proposal_ids)} proposals, {len(delegate_addrs)} delegates, block={block}")
    if vote_sample:
        print(f"  Vote sample: proposal={vote_sample[0]}, voter={vote_sample[1]}")

    return {
        "block": block,
        "proposal_ids": proposal_ids,
        "delegate_addrs": delegate_addrs,
        "vote_sample": vote_sample,
    }


CHAIN_ID_TO_SLUG = {
    1: "ethereum",
    10: "optimism",
    42161: "arbitrum",
    8453: "base",
    137: "polygon",
    43114: "avalanche",
    56: "bsc",
    11155111: "sepolia",
    11155420: "op-sepolia",
}


def detect_chain_slug(session, base_url, errors):
    """Auto-detect chain slug from /deployment endpoint's chain_id."""
    deployment = fetch(session, base_url, "/deployment", "detect_chain_slug", errors)
    if deployment and "deployment" in deployment:
        chain_id = deployment["deployment"].get("chain_id")
        if chain_id:
            return CHAIN_ID_TO_SLUG.get(int(chain_id), f"chain-{chain_id}")
    return "unknown-chain"


# ─── Snapshot ─────────────────────────────────────────────────────────────────

def snapshot_all(base_url, page_size, max_proposals, max_delegates):
    """Fetch all 21 endpoints and return a dict of {section_name: response_data}."""
    session = requests.Session()
    errors = []
    snapshot = {}
    start = time.time()

    print(f"\nSnapshotting {base_url} ...")

    # ── Auto-discover params ──────────────────────────────────────────────
    ctx = discover(session, base_url, page_size, errors)
    block = ctx["block"]
    proposal_ids = ctx["proposal_ids"][:max_proposals] if max_proposals else ctx["proposal_ids"]
    delegate_addrs = ctx["delegate_addrs"][:max_delegates] if max_delegates else ctx["delegate_addrs"]
    vote_sample = ctx["vote_sample"]

    # ── 1. GET /health ────────────────────────────────────────────────────
    data = fetch(session, base_url, "/health", "health", errors)
    snapshot["health"] = strip_volatile(data, "health")
    print("  1. /health")

    # ── 2. GET /config ────────────────────────────────────────────────────
    snapshot["config"] = fetch(session, base_url, "/config", "config", errors)
    print("  2. /config")

    # ── 3. GET /deployment ────────────────────────────────────────────────
    snapshot["deployment"] = fetch(session, base_url, "/deployment", "deployment", errors)
    print("  3. /deployment")

    # ── 4. GET /v1/progress ───────────────────────────────────────────────
    data = fetch(session, base_url, "/v1/progress", "progress", errors)
    snapshot["progress"] = strip_volatile(data, "progress")
    print("  4. /v1/progress")

    # ── 5. GET /v1/voting_power ───────────────────────────────────────────
    snapshot["voting_power"] = fetch(session, base_url, "/v1/voting_power", "voting_power", errors)
    print("  5. /v1/voting_power")

    # ── 6. GET /v1/integrity ──────────────────────────────────────────────
    snapshot["integrity"] = fetch(session, base_url, "/v1/integrity", "integrity", errors)
    print("  6. /v1/integrity")

    # ── 7. GET /v1/proposals ──────────────────────────────────────────────
    snapshot["proposals"] = fetch(session, base_url, "/v1/proposals", "proposals", errors)
    print(f"  7. /v1/proposals ({len(proposal_ids)} found)")

    # ── 8. GET /v1/proposal_types ─────────────────────────────────────────
    snapshot["proposal_types"] = fetch(session, base_url, "/v1/proposal_types", "proposal_types", errors)
    print("  8. /v1/proposal_types")

    # ── 9. GET /v1/proposal/<id> ──────────────────────────────────────────
    snapshot["proposal_details"] = {}
    # Sort proposals by ID and limit to first 50 to reduce data size
    sorted_proposals = sorted(proposal_ids)[:50]
    for pid in sorted_proposals:
        data = fetch(session, base_url, f"/v1/proposal/{pid}", f"proposal/{pid}", errors)
        if data is not None:
            snapshot["proposal_details"][pid] = data
    print(f" 9. /v1/proposal/<id> (sampled {len(sorted_proposals)} proposals, sorted by ID)")

    # ── 10. GET /v1/vote_record/<id>?full=true ────────────────────────────
    snapshot["vote_records"] = {}
    # Sort proposals by ID and limit to first 50 to reduce data size
    sorted_proposals = sorted(proposal_ids)[:50]
    for pid in sorted_proposals:
        # Limit vote records to 100 per proposal to reduce data size
        data = fetch(session, base_url, f"/v1/vote_record/{pid}?page_size=100", f"vote_record/{pid}", errors)
        if data is not None:
            snapshot["vote_records"][pid] = data
    print(f" 10. /v1/vote_record/<id> (sampled {len(sorted_proposals)} proposals with max 100 votes each, sorted by ID)")

    # ── 11. GET /v1/vote?proposal_id=X&voter=Y ────────────────────────────
    if vote_sample:
        pid, voter = vote_sample
        snapshot["vote_lookup"] = fetch(session, base_url,
                                        f"/v1/vote?proposal_id={pid}&voter={voter}",
                                        "vote_lookup", errors)
        print(f" 11. /v1/vote?proposal_id={pid}&voter={voter}")
    else:
        snapshot["vote_lookup"] = None
        print(" 11. /v1/vote (skipped, no vote sample found)")

    # ── 12. GET /v1/balance/<addr> ────────────────────────────────────────
    snapshot["balances"] = {}
    # Sort delegate addresses and limit to first 50 to reduce data size
    sorted_delegates = sorted(delegate_addrs)[:50]
    # Use the same limited delegate sample for consistency
    for addr in sorted_delegates:
        data = fetch(session, base_url, f"/v1/balance/{addr}", f"balance/{addr}", errors)
        if data is not None:
            snapshot["balances"][addr] = data
    print(f" 12. /v1/balance/<addr> (sampled {len(sorted_delegates)} delegates)")

    # ── 13. GET /v1/delegates?sort_by=... ─────────────────────────────────
    sort_options = ["VP", "DC", "MRD", "OLD", "LVB", "VPC"]
    snapshot["delegates_by_sort"] = {}
    for sort_by in sort_options:
        data = fetch(session, base_url,
                     f"/v1/delegates?sort_by={sort_by}&page_size={page_size}&include=VP,DC,VPC",
                     f"delegates_sort_{sort_by}", errors)
        if data is not None:
            snapshot["delegates_by_sort"][sort_by] = data
    print(f" 13. /v1/delegates (sorted by {', '.join(sort_options)})")

    # ── 14. GET /v1/delegate/<addr> ───────────────────────────────────────
    snapshot["delegate_details"] = {}
    # Use the same limited delegate sample for consistency
    for addr in sorted_delegates:
        data = fetch(session, base_url, f"/v1/delegate/{addr}", f"delegate/{addr}", errors)
        if data is not None:
            snapshot["delegate_details"][addr] = data
    print(f" 14. /v1/delegate/<addr> (sampled {len(sorted_delegates)} delegates, sorted by address)")

    # ── 15. GET /v1/delegate/<addr>/voting_history ────────────────────────
    snapshot["delegate_voting_histories"] = {}
    # Use the same limited delegate sample for consistency
    for addr in sorted_delegates:
        data = fetch(session, base_url, f"/v1/delegate/{addr}/voting_history",
                     f"delegate_voting_history/{addr}", errors)
        if data is not None:
            snapshot["delegate_voting_histories"][addr] = data
    print(f" 15. /v1/delegate/<addr>/voting_history (sampled {len(sorted_delegates)} delegates)")

    # ── 16. GET /v1/delegate_vp/<addr>/<block> ────────────────────────────
    snapshot["delegate_vp_at_block"] = {}
    # Use the same limited delegate sample for consistency
    for addr in sorted_delegates:
        data = fetch(session, base_url, f"/v1/delegate_vp/{addr}/{block}",
                     f"delegate_vp/{addr}/{block}", errors)
        if data is not None:
            snapshot["delegate_vp_at_block"][addr] = data
    print(f" 16. /v1/delegate_vp/<addr>/{block} (sampled {len(sorted_delegates)} delegates)")

    # ── 17. GET /v1/voter_history/<voter> ──────────────────────────────────
    snapshot["voter_histories"] = {}
    # Use the same limited delegate sample for consistency
    for addr in sorted_delegates:
        data = fetch(session, base_url, f"/v1/voter_history/{addr}",
                     f"voter_history/{addr}", errors)
        if data is not None:
            snapshot["voter_histories"][addr] = data
    print(f" 17. /v1/voter_history/<voter> (sampled {len(sorted_delegates)} delegates)")

    # ── 18. GET /v1/nonivotes/total ───────────────────────────────────────
    snapshot["nonivotes_total"] = fetch(session, base_url,
                                        "/v1/nonivotes/total",
                                        "nonivotes_total", errors, quiet=True)
    print(" 18. /v1/nonivotes/total")

    # ── 19. GET /v1/nonivotes/total/at-block/<block> ──────────────────────
    snapshot["nonivotes_total_at_block"] = fetch(session, base_url,
                                                  f"/v1/nonivotes/total/at-block/{block}",
                                                  "nonivotes_total_at_block", errors, quiet=True)
    print(f" 19. /v1/nonivotes/total/at-block/{block}")

    # ── 20. GET /v1/nonivotes/user/<addr>/at-block/<block> ────────────────
    snapshot["nonivotes_user_at_block"] = {}
    # Use the same limited delegate sample for consistency
    sample_size = min(10, len(sorted_delegates))
    for addr in sorted_delegates[:sample_size]:
        data = fetch(session, base_url,
                     f"/v1/nonivotes/user/{addr}/at-block/{block}",
                     f"nonivotes_user/{addr}", errors, quiet=True)
        if data is not None:
            snapshot["nonivotes_user_at_block"][addr] = data
    print(f" 20. /v1/nonivotes/user/<addr>/at-block/{block} (sampled {sample_size} delegates)")

    # ── 21. GET /v1/nonivotes/all/at-block/<block> ────────────────────────
    snapshot["nonivotes_all_at_block"] = fetch(session, base_url,
                                                f"/v1/nonivotes/all/at-block/{block}",
                                                "nonivotes_all_at_block", errors, quiet=True)
    print(f" 21. /v1/nonivotes/all/at-block/{block}")

    # ── Metadata ──────────────────────────────────────────────────────────
    elapsed = time.time() - start
    snapshot["_meta"] = {
        "base_url": base_url,
        "timestamp": datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "block": block,
        "num_proposals": len(proposal_ids),
        "num_delegates": len(delegate_addrs),
        "sampled_proposals": len(sorted_proposals),
        "sampled_delegates": len(sorted_delegates),
        "vote_sample": vote_sample,
        "page_size": page_size,
        "errors": errors,
    }

    print(f"\nDone in {elapsed:.1f}s. {len(errors)} errors.")
    return snapshot, block


# ─── Storage ──────────────────────────────────────────────────────────────────

def upload_to_gcs(snapshot, bucket_name, folder_prefix):
    """Upload each section as a separate JSON file to GCS."""
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    total = len(snapshot)
    for i, (section_name, section_data) in enumerate(snapshot.items(), 1):
        blob_path = f"{folder_prefix}/{section_name}.json"
        blob = bucket.blob(blob_path)
        payload = json.dumps(section_data, indent=2, default=str)
        size_mb = len(payload) / (1024 * 1024)
        print(f"  [{i}/{total}] uploading {section_name}.json ({size_mb:.1f} MB) ...", end=" ", flush=True)
        if size_mb > 5:
            blob.upload_from_string(payload, content_type="application/json", timeout=300, num_retries=2)
        else:
            blob.upload_from_string(payload, content_type="application/json", timeout=120)
        print("done")


def save_locally(snapshot, output_dir, folder_prefix):
    """Save each section as a separate JSON file locally."""
    local_dir = os.path.join(output_dir, folder_prefix)
    os.makedirs(local_dir, exist_ok=True)

    for section_name, section_data in snapshot.items():
        fpath = os.path.join(local_dir, f"{section_name}.json")
        with open(fpath, "w") as f:
            json.dump(section_data, f, indent=2, default=str)

    return local_dir


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Snapshot all 21 DAO Node API endpoints")
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--label", required=True, help="'old' or 'new'")
    parser.add_argument("--chain-slug", default=None, help="Override (auto-detected from /health)")
    parser.add_argument("--block", default=None, help="Override (auto-detected from /v1/progress)")
    parser.add_argument("--bucket", default=None, help="GCS bucket (default: DAONODE_DATA_COMPARE env)")
    parser.add_argument("--no-gcs", action="store_true", help="Local only, skip GCS")
    parser.add_argument("--page-size", type=int, default=200)
    parser.add_argument("--max-proposals", type=int, default=50, help="Max proposals for per-proposal endpoints (default 50)")
    parser.add_argument("--max-delegates", type=int, default=10, help="Max delegates for per-delegate endpoints (default 10)")
    args = parser.parse_args()

    session = requests.Session()
    errors = []

    chain_slug = args.chain_slug or detect_chain_slug(session, args.base_url, errors)

    snapshot, detected_block = snapshot_all(
        base_url=args.base_url,
        page_size=args.page_size,
        max_proposals=args.max_proposals,
        max_delegates=args.max_delegates,
    )

    block = args.block or detected_block
    folder_name = f"{block}-{chain_slug}"
    folder_prefix = f"{folder_name}/{args.label}"

    # Always save locally
    output_dir = os.path.join(os.path.dirname(__file__), "snapshots")
    local_dir = save_locally(snapshot, output_dir, folder_prefix)
    print(f"\nSaved locally: {local_dir}")

    # Upload to GCS
    if not args.no_gcs:
        bucket_name = args.bucket or os.getenv("DAONODE_DATA_COMPARE", "daonode-data-compare")
        print(f"Uploading to gs://{bucket_name}/{folder_prefix}/ ...")
        upload_to_gcs(snapshot, bucket_name, folder_prefix)

    print(f"\nFolder for comparison: {folder_name}")


if __name__ == "__main__":
    main()
