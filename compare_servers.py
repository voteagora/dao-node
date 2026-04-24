#!/usr/bin/env python3
"""
Hit every JSON endpoint declared in app/server.py against two dao-node servers
and print a compact, line-per-difference diff to stdout.

Usage:
    python compare_servers.py http://host-a:8000 http://host-b:8000
"""
import argparse
import json
import random
import sys
import requests

TIMEOUT = 60
MAX_VAL_LEN = 80
RANDOM_SEED = 42 # This is hardcoded, so we get the same set of proposals on a per tenant basis.

DIAGNOSTIC_ENDPOINTS = \
                    [("/health", []),
                    ("/config", []),
                    ("/deployment", []),
                    ("/v1/progress", []),
                    ("/v1/integrity", [])]

DELEGATE_SORT_ENDPOINTS = \
        [("/v1/delegates?sort_by=DC&page_size=100", []),
        ("/v1/delegates?sort_by=MRD&page_size=100", []),
        ("/v1/delegates?sort_by=LVB&page_size=100", []),
        ("/v1/delegates?sort_by=VPC&page_size=100", [])]

DIRECT_ENDPOINTS = \
        [("/v1/direct/votes/proposal_vote_record", []),
        ("/v1/direct/delegations/delegatee_vp", []),
        ("/v1/direct/votes/voter_history", [])]

APPLICATION_ENDPOINTS = [
        ("/v1/voting_power", []),
        ("/v1/proposals", []),
        ("/v1/proposals?set=relevant", []),
        ("/v1/proposals?sort=start_block", []),
        ("/v1/proposal_types", []),
        # ("/v1/diagnostics/true", []),
        ("/v1/proposal/{proposal_id}", ["proposal_id"]),
        # ("/v1/vote_record/{proposal_id}?page_size=25", ["proposal_id"]),
        # ("/v1/vote_record/{proposal_id}?sort_by=VP&page_size=25", ["proposal_id"]),
        ("/v1/voter_history/{voter}", ["voter"]),
        ("/v1/vote?proposal_id={proposal_id}&voter={voter}", ["proposal_voter_pair"]),
        ("/v1/delegate/{delegate}", ["delegate"]),
        ("/v1/delegate/{delegate}/voting_history", ["delegate"]),
        ("/v1/delegate_vp/{delegate}/{block_number}", ["delegate", "block_number"]),
        # ("/v1/balance/{delegate}", ["delegate"]),
    ]

NON_IVOTES_ENDPOINTS = [
        ("/v1/nonivotes/total/at-block/{block_number}", ["block_number"]),
        ("/v1/nonivotes/user/{delegate}/at-block/{block_number}", ["delegate", "block_number"]),
        ("/v1/nonivotes/all/at-block/{block_number}", ["block_number"]),
        ("/v1/nonivotes/total", [])]


dao = 'xai'

if dao == 'uniswap':

    SAMPLE_SIZE = 60
    ENDPOINTS = DIAGNOSTIC_ENDPOINTS + DELEGATE_SORT_ENDPOINTS + DIRECT_ENDPOINTS + APPLICATION_ENDPOINTS

elif dao == 'xai':

    SAMPLE_SIZE = 1
    ENDPOINTS = DIAGNOSTIC_ENDPOINTS + DELEGATE_SORT_ENDPOINTS + DIRECT_ENDPOINTS + APPLICATION_ENDPOINTS




else:

    SAMPLE_SIZE = 5
    ENDPOINTS = DIAGNOSTIC_ENDPOINTS + DELEGATE_SORT_ENDPOINTS + DIRECT_ENDPOINTS + APPLICATION_ENDPOINTS + NON_IVOTES_ENDPOINTS


def get_json(url):
    r = requests.get(url, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()


def flatten(obj, prefix=""):
    if isinstance(obj, dict):
        out = {}
        for k in obj:
            out.update(flatten(obj[k], f"{prefix}.{k}" if prefix else str(k)))
        return out
    if isinstance(obj, list):
        out = {}
        for i, v in enumerate(obj):
            out.update(flatten(v, f"{prefix}[{i}]"))
        if not obj:
            out[prefix] = []
        return out
    return {prefix: obj}


def short(v):
    s = v if isinstance(v, str) else json.dumps(v, default=str)
    return s if len(s) <= MAX_VAL_LEN else s[: MAX_VAL_LEN - 3] + "..."


def diff_json(a, b):
    fa, fb = flatten(a), flatten(b)
    for k in sorted(set(fa) | set(fb)):
        if k not in fa:
            yield f"  + {k} = {short(fb[k])}"
        elif k not in fb:
            yield f"  - {k} = {short(fa[k])}"
        elif fa[k] != fb[k]:

            if isinstance(fa[k], (int, float)) and isinstance(fb[k], (int, float)):
                yield f"  ~ {k}: {short(fa[k])} != {short(fb[k])} ({fa[k] - fb[k]})"
            elif fa[k].isnumeric() and fb[k].isnumeric():
                yield f"  ~ {k}: {short(fa[k])} != {short(fb[k])} ({int(fa[k]) - int(fb[k])})"
            else:
                yield f"  ~ {k}: {short(fa[k])} != {short(fb[k])}"


def compare(a_base, b_base, path):
    a_url, b_url = a_base + path, b_base + path
    try:
        a = get_json(a_url)
    except Exception as e:
        print(f"[{path}] A ERROR: {e}")
        return
    try:
        b = get_json(b_url)
    except Exception as e:
        print(f"[{path}] B ERROR: {e}")
        return
    diffs = list(diff_json(a, b))
    if not diffs:
        print(f"[{path}] OK")
    else:
        print(f"[{path}] {len(diffs)} diff(s)")
        for d in diffs:
            print(d)


def discover(base, rng):
    """Pull SAMPLE_SIZE proposal ids, delegates, voters, and block numbers from A.

    All lists are sorted before sampling so the rng draws from a stable order,
    which keeps a given seed deterministic across runs.
    """
    p = {
        "proposal_ids": [],
        "delegates": [],
        "voters": [],
        "block_numbers": [],
        "proposal_voter_pairs": [],
    }

    try:
        props = get_json(base + "/v1/proposals").get("proposals", [])
        ids = sorted(str(x["id"]) for x in props if "id" in x)
        if ids:
            p["proposal_ids"] = rng.sample(ids, min(SAMPLE_SIZE, len(ids)))
    except Exception as e:
        print(f"# discover proposals failed: {e}", file=sys.stderr)

    try:
        dels = get_json(base + "/v1/delegates?page_size=500").get("delegates", [])
        addrs = sorted(x["addr"] for x in dels if "addr" in x)
        if addrs:
            p["delegates"] = rng.sample(addrs, min(SAMPLE_SIZE, len(addrs)))
    except Exception as e:
        print(f"# discover delegates failed: {e}", file=sys.stderr)

    try:
        current_block = int(get_json(base + "/v1/progress").get("block") or 1)
        upper = max(1, current_block)
        p["block_numbers"] = sorted(
            rng.sample(range(1, upper + 1), min(SAMPLE_SIZE, upper))
        )
    except Exception as e:
        print(f"# discover progress failed: {e}", file=sys.stderr)

    # Voters come from the vote_record of the sampled proposals. We also keep
    # correlated (proposal_id, voter) pairs so /v1/vote can be exercised with
    # voters who actually voted on the paired proposal.
    if p["proposal_ids"]:
        all_voters = set()
        pairs = []
        for pid in p["proposal_ids"]:
            try:
                vr = get_json(
                    base + f"/v1/vote_record/{pid}?page_size=100"
                ).get("vote_record", [])
                voters_in_prop = sorted({v["voter"] for v in vr if "voter" in v})
                all_voters.update(voters_in_prop)
                if voters_in_prop:
                    pairs.append((pid, rng.choice(voters_in_prop)))
            except Exception as e:
                print(f"# discover voter failed for {pid}: {e}", file=sys.stderr)
        voters_list = sorted(all_voters)
        if voters_list:
            p["voters"] = rng.sample(voters_list, min(SAMPLE_SIZE, len(voters_list)))
        p["proposal_voter_pairs"] = pairs

    return p


PARAM_PLURAL = {
    "proposal_id": "proposal_ids",
    "voter": "voters",
    "delegate": "delegates",
    "block_number": "block_numbers",
}

def expand(tmpl, needs, probes):
    """Yield fully formatted paths by iterating parallel probe lists."""
    if not needs:
        yield tmpl
        return

    if "proposal_voter_pair" in needs:
        for pid, voter in probes.get("proposal_voter_pairs", []):
            yield tmpl.format(proposal_id=pid, voter=voter)
        return

    lists = [probes.get(PARAM_PLURAL[n], []) for n in needs]
    if any(not lst for lst in lists):
        return

    n_iter = min(len(lst) for lst in lists)
    for i in range(n_iter):
        yield tmpl.format(**{needs[j]: lists[j][i] for j in range(len(needs))})


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("url_a")
    ap.add_argument("url_b")
    args = ap.parse_args()
    a, b = args.url_a.rstrip("/"), args.url_b.rstrip("/")

    rng = random.Random(RANDOM_SEED)
    probes = discover(a, rng)
    print(f"# seed={RANDOM_SEED} sample_size={SAMPLE_SIZE}")
    print(f"# proposal_ids={probes['proposal_ids']}")
    print(f"# delegates={probes['delegates']}")
    print(f"# voters={probes['voters']}")
    print(f"# block_numbers={probes['block_numbers']}")
    print(f"# proposal_voter_pairs={probes['proposal_voter_pairs']}")
    print(f"# A = {a}")
    print(f"# B = {b}")

    for tmpl, needs in ENDPOINTS:
        paths = list(expand(tmpl, needs, probes))
        if not paths and needs:
            print(f"[{tmpl}] SKIP no probe values available")
            continue
        for path in paths:
            compare(a, b, path)


if __name__ == "__main__":
    main()
