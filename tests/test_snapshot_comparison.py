"""
Snapshot Comparison Test
========================

Compares "old" vs "new" endpoint snapshots stored as per-section JSON files,
either locally or in a GCS bucket.

GCS Structure:
    gs://<DAONODE_DATA_COMPARE>/<block_number>-<chain_slug>/old/<section>.json
    gs://<DAONODE_DATA_COMPARE>/<block_number>-<chain_slug>/new/<section>.json

Usage:
    # Compare old vs new from GCS (same parent folder)
    python tests/test_snapshot_comparison.py --folder 135000000-optimism

    # Compare old vs new from different folders (different block heights)
    python tests/test_snapshot_comparison.py --old-folder 135000000-optimism/old --new-folder 136000000-optimism/new

    # Compare old vs new from local snapshots dir
    python tests/test_snapshot_comparison.py --folder 135000000-optimism --no-gcs

    # Custom bucket
    python tests/test_snapshot_comparison.py --folder 135000000-optimism --bucket my-bucket

    # Or run via pytest
    SNAPSHOT_FOLDER=135000000-optimism pytest tests/test_snapshot_comparison.py -v
    SNAPSHOT_FOLDER=135000000-optimism SNAPSHOT_NO_GCS=1 pytest tests/test_snapshot_comparison.py -v

    # pytest with separate old/new folders
    SNAPSHOT_OLD_FOLDER=135000000-optimism/old SNAPSHOT_NEW_FOLDER=136000000-optimism/new pytest tests/test_snapshot_comparison.py -v
"""

import argparse
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(__file__))
from snapshot_endpoints import normalize


def load_snapshot_from_local(folder_path):
    """Load all section JSON files from a local directory into a single dict."""
    snapshot = {}
    if not os.path.isdir(folder_path):
        raise FileNotFoundError(f"Local snapshot folder not found: {folder_path}")
    for fname in os.listdir(folder_path):
        if fname.endswith(".json"):
            section = fname.replace(".json", "")
            with open(os.path.join(folder_path, fname), "r") as f:
                snapshot[section] = json.load(f)
    return snapshot


def load_snapshot_from_gcs(bucket_name, prefix):
    """Load all section JSON files from a GCS prefix into a single dict."""
    from google.cloud import storage

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    snapshot = {}

    blobs = list(bucket.list_blobs(prefix=prefix + "/"))
    if not blobs:
        raise FileNotFoundError(f"No blobs found at gs://{bucket_name}/{prefix}/")

    for blob in blobs:
        if blob.name.endswith(".json"):
            section = blob.name.split("/")[-1].replace(".json", "")
            data = blob.download_as_text()
            snapshot[section] = json.loads(data)
            print(f"  ↓ gs://{bucket_name}/{blob.name}")

    return snapshot


def load_snapshots(folder=None, bucket_name=None, no_gcs=False, old_folder=None, new_folder=None):
    """Load old and new snapshots from GCS or local disk.

    Pass either ``folder`` (expects ``<folder>/old`` and ``<folder>/new`` sub-paths)
    or explicit ``old_folder`` / ``new_folder`` paths when the two snapshots live
    at different locations (e.g. different block heights).
    """
    if old_folder is None:
        if folder is None:
            raise ValueError("Provide either 'folder' or both 'old_folder' and 'new_folder'")
        old_folder = f"{folder}/old"
        new_folder = f"{folder}/new"
    elif new_folder is None:
        raise ValueError("'new_folder' must be provided when 'old_folder' is set")

    if no_gcs:
        snapshots_base = os.path.join(os.path.dirname(__file__), "snapshots")

        def _local(path):
            full = path if os.path.isabs(path) else os.path.join(snapshots_base, path)
            print(f"Loading from local: {full}")
            return load_snapshot_from_local(full)

        snap_old = _local(old_folder)
        snap_new = _local(new_folder)
    else:
        bucket_name = bucket_name or os.getenv("DAONODE_DATA_COMPARE", "daonode-data-compare")
        print(f"Loading old from gs://{bucket_name}/{old_folder}/")
        print(f"Loading new from gs://{bucket_name}/{new_folder}/")
        snap_old = load_snapshot_from_gcs(bucket_name, old_folder)
        snap_new = load_snapshot_from_gcs(bucket_name, new_folder)
    return normalize(snap_old), normalize(snap_new)


def diff_values(a, b, path=""):
    """Recursively compare two JSON-like structures. Returns list of diff descriptions."""
    diffs = []

    if type(a) != type(b):
        diffs.append(f"{path}: type mismatch: {type(a).__name__} vs {type(b).__name__}")
        return diffs

    if isinstance(a, dict):
        all_keys = set(a.keys()) | set(b.keys())
        for key in sorted(all_keys):
            child_path = f"{path}.{key}" if path else key
            if key not in a:
                diffs.append(f"{child_path}: missing in snapshot A")
            elif key not in b:
                diffs.append(f"{child_path}: missing in snapshot B")
            else:
                diffs.extend(diff_values(a[key], b[key], child_path))

    elif isinstance(a, list):
        if len(a) != len(b):
            diffs.append(f"{path}: list length mismatch: {len(a)} vs {len(b)}")
        for i in range(min(len(a), len(b))):
            diffs.extend(diff_values(a[i], b[i], f"{path}[{i}]"))

    else:
        if a != b:
            a_str = str(a)[:100]
            b_str = str(b)[:100]
            diffs.append(f"{path}: {a_str} != {b_str}")

    return diffs


def compare_section(snap_a, snap_b, section, max_diffs=20):
    """Compare a top-level section between two snapshots."""
    if section not in snap_a and section not in snap_b:
        return []  # Both missing, that's fine

    if section not in snap_a:
        return [f"{section}: missing entirely in snapshot A"]
    if section not in snap_b:
        return [f"{section}: missing entirely in snapshot B"]

    diffs = diff_values(snap_a[section], snap_b[section], section)
    if len(diffs) > max_diffs:
        truncated = len(diffs) - max_diffs
        diffs = diffs[:max_diffs]
        diffs.append(f"... and {truncated} more diffs in {section}")
    return diffs


# Sections to compare — matches snapshot_endpoints.py section names (all 21 endpoints)
SECTIONS = [
    "health",                       #  1. GET /health
    "config",                       #  2. GET /config
    "deployment",                   #  3. GET /deployment
    "progress",                     #  4. GET /v1/progress
    "voting_power",                 #  5. GET /v1/voting_power
    "integrity",                    #  6. GET /v1/integrity
    "proposals",                    #  7. GET /v1/proposals
    "proposal_types",               #  8. GET /v1/proposal_types
    "proposal_details",             #  9. GET /v1/proposal/<id>
    "vote_records",                 # 10. GET /v1/vote_record/<id>
    "vote_lookup",                  # 11. GET /v1/vote?proposal_id=X&voter=Y
    "balances",                     # 12. GET /v1/balance/<addr>
    "delegates_by_sort",            # 13. GET /v1/delegates?sort_by=...
    "delegate_details",             # 14. GET /v1/delegate/<addr>
    "delegate_voting_histories",    # 15. GET /v1/delegate/<addr>/voting_history
    "delegate_vp_at_block",         # 16. GET /v1/delegate_vp/<addr>/<block>
    "voter_histories",              # 17. GET /v1/voter_history/<voter>
    "nonivotes_total",              # 18. GET /v1/nonivotes/total
    "nonivotes_total_at_block",     # 19. GET /v1/nonivotes/total/at-block/<block>
    "nonivotes_user_at_block",      # 20. GET /v1/nonivotes/user/<addr>/at-block/<block>
    "nonivotes_all_at_block",       # 21. GET /v1/nonivotes/all/at-block/<block>
]


def full_comparison(snap_a, snap_b):
    """Run full comparison and return {section: diffs} dict."""
    results = {}
    for section in SECTIONS:
        diffs = compare_section(snap_a, snap_b, section)
        results[section] = diffs
    return results


def print_report(results, path_a, path_b):
    """Print a human-readable comparison report."""
    total_diffs = sum(len(d) for d in results.values())
    passed = [s for s, d in results.items() if len(d) == 0]
    failed = [s for s, d in results.items() if len(d) > 0]

    print(f"\n{'='*60}")
    print(f"Snapshot Comparison Report")
    print(f"{'='*60}")
    print(f"  A: {path_a}")
    print(f"  B: {path_b}")
    print(f"  Total diffs: {total_diffs}")
    print(f"  Sections passed: {len(passed)}/{len(passed)+len(failed)}")
    print()

    if passed:
        print("PASSED:")
        for s in passed:
            print(f"  ✓ {s}")

    if failed:
        print("\nFAILED:")
        for s in failed:
            print(f"\n  ✗ {s} ({len(results[s])} diffs):")
            for diff in results[s]:
                print(f"    - {diff}")

    print(f"\n{'='*60}")
    return total_diffs == 0


# ─── pytest-based tests ───────────────────────────────────────────────────────

SNAPSHOT_FOLDER = os.getenv("SNAPSHOT_FOLDER")
SNAPSHOT_OLD_FOLDER = os.getenv("SNAPSHOT_OLD_FOLDER")
SNAPSHOT_NEW_FOLDER = os.getenv("SNAPSHOT_NEW_FOLDER")
SNAPSHOT_NO_GCS = bool(os.getenv("SNAPSHOT_NO_GCS"))
SNAPSHOT_BUCKET = os.getenv("DAONODE_DATA_COMPARE", "daonode-data-compare")

_has_snapshots = bool(SNAPSHOT_FOLDER or (SNAPSHOT_OLD_FOLDER and SNAPSHOT_NEW_FOLDER))
skip_reason = (
    "Set SNAPSHOT_FOLDER (e.g. '135000000-optimism') or both "
    "SNAPSHOT_OLD_FOLDER and SNAPSHOT_NEW_FOLDER env vars"
)
skip_if_no_snapshots = pytest.mark.skipif(
    not _has_snapshots,
    reason=skip_reason,
)


@pytest.fixture(scope="module")
def snapshots():
    if not _has_snapshots:
        pytest.skip(skip_reason)
    return load_snapshots(
        folder=SNAPSHOT_FOLDER,
        bucket_name=SNAPSHOT_BUCKET,
        no_gcs=SNAPSHOT_NO_GCS,
        old_folder=SNAPSHOT_OLD_FOLDER,
        new_folder=SNAPSHOT_NEW_FOLDER,
    )


@skip_if_no_snapshots
@pytest.mark.parametrize("section", SECTIONS)
def test_snapshot_section(snapshots, section):
    snap_old, snap_new = snapshots
    diffs = compare_section(snap_old, snap_new, section)
    if diffs:
        msg = f"\n{section} has {len(diffs)} diffs:\n"
        msg += "\n".join(f"  - {d}" for d in diffs[:20])
        pytest.fail(msg)


@skip_if_no_snapshots
def test_snapshot_block_numbers_match(snapshots):
    """Both snapshots must be at the same block height."""
    snap_old, snap_new = snapshots
    block_old = snap_old.get("progress", {}).get("block")
    block_new = snap_new.get("progress", {}).get("block")
    assert block_old == block_new, f"Block mismatch: old={block_old} vs new={block_new}"


@skip_if_no_snapshots
def test_snapshot_archive_counts_match(snapshots):
    """Archive signal counts should match (same events processed)."""
    snap_old, snap_new = snapshots
    counts_old = snap_old.get("progress", {}).get("archive_counts", {})
    counts_new = snap_new.get("progress", {}).get("archive_counts", {})
    diffs = diff_values(counts_old, counts_new, "archive_counts")
    if diffs:
        msg = "\nArchive signal count diffs:\n"
        msg += "\n".join(f"  - {d}" for d in diffs)
        pytest.fail(msg)


# ─── CLI entry point ──────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Compare old vs new endpoint snapshots")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--folder", help="Parent folder; expects <folder>/old and <folder>/new sub-paths")
    group.add_argument("--old-folder", help="Explicit path for the 'old' snapshot (use with --new-folder)")
    parser.add_argument("--new-folder", help="Explicit path for the 'new' snapshot (required with --old-folder)")
    parser.add_argument("--bucket", default=None, help="GCS bucket (defaults to DAONODE_DATA_COMPARE env var)")
    parser.add_argument("--no-gcs", action="store_true", help="Load from local tests/snapshots/ instead of GCS")
    args = parser.parse_args()

    if args.old_folder and not args.new_folder:
        parser.error("--new-folder is required when --old-folder is specified")

    bucket_name = args.bucket or os.getenv("DAONODE_DATA_COMPARE", "daonode-data-compare")
    snap_old, snap_new = load_snapshots(
        folder=args.folder,
        bucket_name=bucket_name,
        no_gcs=args.no_gcs,
        old_folder=args.old_folder,
        new_folder=args.new_folder,
    )

    path_a = args.old_folder or f"{args.folder}/old"
    path_b = args.new_folder or f"{args.folder}/new"
    results = full_comparison(snap_old, snap_new)
    ok = print_report(results, path_a, path_b)

    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
