#!/usr/bin/env python3
"""
Hit /v1/delegates on two dao-node servers and compare the two lists by
membership (presence in A vs B, keyed by `addr`) and per-delegate field
differences. Unlike compare_servers.py, order is ignored.

Usage:
    python compare_delegate_list.py http://host-a:8000 http://host-b:8000
    python compare_delegate_list.py http://host-a:8000 http://host-b:8000 \
        --query "sort_by=DC&page_size=1000"
"""
import argparse
import json
import sys
import requests

TIMEOUT = 120
MAX_VAL_LEN = 120


def get_json(url):
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def short(v):
    s = v if isinstance(v, str) else json.dumps(v, default=str)
    return s if len(s) <= MAX_VAL_LEN else s[: MAX_VAL_LEN - 3] + "..."


def index_by_addr(delegates):
    out = {}
    for d in delegates:
        addr = d.get("addr")
        if addr is None:
            continue
        out[addr.lower()] = d
    return out


def diff_delegate(a, b):
    diffs = []
    for k in sorted(set(a.keys()) | set(b.keys())):
        if k == "addr":
            continue
        if k not in a:
            diffs.append(f"    + {k} = {short(b[k])}")
        elif k not in b:
            diffs.append(f"    - {k} = {short(a[k])}")
        elif a[k] != b[k]:
            if isinstance(a[k], int) and isinstance(b[k], int):
                diffs.append(f"    ~ {k}: {short(a[k])} != {short(b[k])} ({a[k] - b[k]})")
            else:
                diffs.append(f"    ~ {k}: {short(a[k])} != {short(b[k])}")
    return diffs


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("url_a")
    ap.add_argument("url_b")
    ap.add_argument(
        "--query",
        # default="include=VP,MRD,OLD&sort_by=DC&page_size=200000",
        default="include=VP,MRD,OLD&sort_by=VP&page_size=200000",
        help="Query string without leading '?'. Default: sort_by=DC&page_size=1000",
    )
    args = ap.parse_args()

    a_base = args.url_a.rstrip("/")
    b_base = args.url_b.rstrip("/")
    path = "/v1/delegates" + (f"?{args.query}" if args.query else "")

    a_url = a_base + path
    b_url = b_base + path

    print(f"# A = {a_url}")
    print(f"# B = {b_url}")

    try:
        a_list = get_json(a_url).get("delegates", [])
    except Exception as e:
        print(f"A ERROR: {e}")
        sys.exit(1)
    try:
        b_list = get_json(b_url).get("delegates", [])
    except Exception as e:
        print(f"B ERROR: {e}")
        sys.exit(1)

    a_idx = index_by_addr(a_list)
    b_idx = index_by_addr(b_list)

    a_set = set(a_idx)
    b_set = set(b_idx)
    only_a = sorted(a_set - b_set)
    only_b = sorted(b_set - a_set)
    common = sorted(a_set & b_set)

    print(f"# A count: {len(a_list)}")
    print(f"# B count: {len(b_list)}")
    print(f"# Only in A: {len(only_a)}")
    print(f"# Only in B: {len(only_b)}")
    print(f"# Common:    {len(common)}")

    if only_a:
        print(f"\n== Only in A ({len(only_a)}) ==")
        for addr in only_a:
            print(f"  - {addr}  {short(a_idx[addr])}")

    if only_b:
        print(f"\n== Only in B ({len(only_b)}) ==")
        for addr in only_b:
            print(f"  + {addr}  {short(b_idx[addr])}")

    mismatches = []
    for addr in common:
        diffs = diff_delegate(a_idx[addr], b_idx[addr])
        if diffs:
            mismatches.append((addr, diffs))

    print(f"\n== Field differences in {len(mismatches)} of {len(common)} common delegates ==")
    for addr, diffs in mismatches:
        print(f"  ~ {addr}")
        for line in diffs:
            print(line)

    if not only_a and not only_b and not mismatches:
        print("\nOK — lists match by membership and per-delegate fields.")


if __name__ == "__main__":
    main()
