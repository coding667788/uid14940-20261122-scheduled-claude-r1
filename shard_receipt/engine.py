"""Receipt classification, interpretation sweep, release decision, witness.

Pipeline per manifest:

1.  Filter receipts to the manifest's doc id, generation and declared
    source domains (shards crossing docs / generations / domains cannot
    be combined).
2.  Classify: untrusted receipts are recorded only; a source instance
    that signs two different values for the same shard index has *all*
    its shards in this generation quarantined; exact duplicates are
    counted once; trusted shards arriving after the version deadline are
    archived but unusable for release.
3.  Interpretation sweep: at most one source domain may be unreliable.
    Evaluate the linear system given by the remaining trusted shards for
    the no-fault interpretation and for each single-domain exclusion.
4.  Release iff at least one interpretation is consistent and *every*
    consistent interpretation has a unique solution whose canonical
    commitment matches the manifest.  Inconsistent interpretations never
    enter the result set, but their contradiction evidence is kept.
5.  Witness: among the releasable interpretations pick a basis of
    shards, breaking ties by (fewest distinct source domains, fewest
    shards, ascending receipt ids).
"""

import itertools

from .canon import commitment_of
from .gf257 import MOD, solve_system

STATUS_UNIQUE = "unique"
STATUS_UNDER = "underdetermined"
STATUS_INCONSISTENT = "inconsistent"


def _in_scope(manifest, r):
    return (
        r["doc_id"] == manifest["doc_id"]
        and r["generation"] == manifest["generation"]
        and r["source_domain"] in manifest["source_domains"]
    )


def classify(manifest, receipts):
    """Split in-scope receipts into usable / duplicates / quarantined /
    archived_late / untrusted.  ``receipts`` must be in event order."""
    scoped = [r for r in receipts if _in_scope(manifest, r)]
    out_of_scope = [r["receipt_id"] for r in receipts if not _in_scope(manifest, r)]

    # First pass: a source instance signing two different (row, payload)
    # pairs for the same shard index poisons *all* its shards here.
    signed = {}
    poisoned = set()
    for r in scoped:
        key = (r["source_instance"], r["shard_index"])
        val = (tuple(x % MOD for x in r["row"]), r["payload"] % MOD)
        if key in signed:
            if signed[key] != val:
                poisoned.add(r["source_instance"])
        else:
            signed[key] = val

    usable, duplicates, quarantined, archived_late, untrusted = [], [], [], [], []
    seen_content = set()
    for r in scoped:
        entry = {"receipt_id": r["receipt_id"], "source_instance": r["source_instance"],
                 "source_domain": r["source_domain"], "shard_index": r["shard_index"],
                 "time": r["time"]}
        if not r["trusted"]:
            untrusted.append(entry)
            continue
        if r["source_instance"] in poisoned:
            quarantined.append(entry)
            continue
        if r["time"] > manifest["deadline"]:
            archived_late.append(entry)
            continue
        ckey = (r["source_instance"], r["shard_index"],
                tuple(x % MOD for x in r["row"]), r["payload"] % MOD)
        if ckey in seen_content:
            duplicates.append(entry)
            continue
        seen_content.add(ckey)
        usable.append(r)

    return {
        "usable": usable,
        "duplicates": duplicates,
        "quarantined": quarantined,
        "quarantined_sources": sorted(poisoned),
        "archived_late": archived_late,
        "untrusted": untrusted,
        "out_of_scope": out_of_scope,
    }


def _equations(rows):
    return [(r["receipt_id"], r["row"], r["payload"]) for r in rows]


def _interpretation(excluded_domain, rows, n, commitment):
    res = solve_system(_equations(rows), n)
    interp = {
        "excluded_domain": excluded_domain,
        "status": res["status"],
        "rank": res["rank"],
        "shards_used": sorted(r["receipt_id"] for r in rows),
    }
    if res["status"] == STATUS_INCONSISTENT:
        interp["evidence"] = {"type": "conflict", **res["conflict"]}
    elif res["status"] == STATUS_UNDER:
        interp["evidence"] = {"type": "under_rank", "rank": res["rank"],
                              "null_vector": res["null_vector"]}
    else:
        digest = commitment_of(res["solution"])
        interp["solution"] = res["solution"]
        interp["commitment_computed"] = digest
        interp["digest_match"] = digest == commitment
        if not interp["digest_match"]:
            interp["evidence"] = {
                "type": "commitment_mismatch",
                "expected": commitment,
                "computed": digest,
            }
    return interp


def _greedy_basis(pool, n):
    """Greedy rank-increasing basis over ``pool`` (already id-sorted)."""
    basis, rows_so_far = [], []
    rank = 0
    for r in pool:
        trial = rows_so_far + [r]
        res = solve_system(_equations(trial), n)
        if res["rank"] > rank:
            basis.append(r)
            rows_so_far.append(r)
            rank = res["rank"]
            if rank == n:
                return basis
    return None


def select_witness(rows, n):
    """Basis of shards determining the data vector, tie-broken by
    (fewest distinct source domains, fewest shards, ascending ids)."""
    domains = sorted({r["source_domain"] for r in rows})
    for size in range(1, len(domains) + 1):
        for subset in itertools.combinations(domains, size):
            pool = sorted((r for r in rows if r["source_domain"] in set(subset)),
                          key=lambda r: r["receipt_id"])
            basis = _greedy_basis(pool, n)
            if basis is not None:
                return basis
    return None


def _segment_checks(manifest, usable, n):
    """Single-segment sufficiency, checked separately from whole-package
    compatibility: for each declared segment, only shards whose row
    support lies entirely inside the segment are projected onto it."""
    segs = manifest.get("segments") or [{"name": "whole", "start": 0, "length": n}]
    checks = []
    for seg in segs:
        s, length = seg["start"], seg["length"]
        proj = []
        for r in usable:
            row = r["row"]
            if all(row[i] % MOD == 0 for i in range(n) if not (s <= i < s + length)):
                proj.append((r["receipt_id"], row[s:s + length], r["payload"]))
        res = solve_system(proj, length)
        checks.append({
            "segment": seg["name"],
            "start": s,
            "length": length,
            "covering_shards": len(proj),
            "status": res["status"],
            "sufficient": res["status"] == STATUS_UNIQUE,
        })
    return checks


def evaluate(manifest, receipts, n):
    """Full release evaluation of one manifest against a receipt list."""
    cls = classify(manifest, receipts)
    usable = cls["usable"]
    commitment = manifest["commitment"]

    domains = sorted({r["source_domain"] for r in usable})
    interpretations = []
    for excluded in [None] + domains:
        rows = [r for r in usable if r["source_domain"] != excluded]
        interpretations.append(_interpretation(excluded, rows, n, commitment))

    consistent = [i for i in interpretations if i["status"] != STATUS_INCONSISTENT]
    under = [i for i in consistent if i["status"] == STATUS_UNDER]
    mismatched = [i for i in consistent
                  if i["status"] == STATUS_UNIQUE and not i["digest_match"]]
    releasable = [i for i in consistent
                  if i["status"] == STATUS_UNIQUE and i["digest_match"]]

    reasons = []
    if not consistent:
        decision = "hold"
        reasons.append("no_consistent_interpretation")
    elif under:
        decision = "hold"
        for i in under:
            reasons.append("underdetermined:" + (i["excluded_domain"] or "<none>"))
    elif mismatched:
        decision = "hold"
        for i in mismatched:
            reasons.append("commitment_mismatch:" + (i["excluded_domain"] or "<none>"))
    elif releasable:
        decision = "release"
        reasons.append("ok")
    else:  # pragma: no cover - unreachable given the branches above
        decision = "hold"
        reasons.append("no_consistent_interpretation")

    witness = None
    if decision == "release":
        candidates = []
        for i in releasable:
            rows = [r for r in usable
                    if i["excluded_domain"] is None
                    or r["source_domain"] != i["excluded_domain"]]
            basis = select_witness(rows, n)
            if basis:
                candidates.append((i["excluded_domain"], basis))
        if candidates:
            def key(item):
                _excl, basis = item
                ids = sorted(b["receipt_id"] for b in basis)
                doms = sorted({b["source_domain"] for b in basis})
                return (len(doms), len(ids), ids)
            excl, basis = min(candidates, key=key)
            basis_rows = sorted(basis, key=lambda b: b["receipt_id"])
            sol = solve_system(_equations(basis_rows), n)["solution"]
            witness = {
                "interpretation_excluded_domain": excl,
                "receipt_ids": [b["receipt_id"] for b in basis_rows],
                "source_domains": sorted({b["source_domain"] for b in basis_rows}),
                "solution": sol,
                "commitment": commitment_of(sol),
            }

    no_fault = interpretations[0]
    return {
        "manifest_id": manifest["manifest_id"],
        "doc_id": manifest["doc_id"],
        "generation": manifest["generation"],
        "decision": decision,
        "reasons": reasons,
        "counts": {
            "usable": len(usable),
            "duplicates": len(cls["duplicates"]),
            "quarantined": len(cls["quarantined"]),
            "archived_late": len(cls["archived_late"]),
            "untrusted": len(cls["untrusted"]),
            "out_of_scope": len(cls["out_of_scope"]),
        },
        "quarantined_sources": cls["quarantined_sources"],
        "quarantined": cls["quarantined"],
        "archived_late": cls["archived_late"],
        "untrusted": cls["untrusted"],
        "duplicates": cls["duplicates"],
        "out_of_scope": cls["out_of_scope"],
        "cross_source_compatible": no_fault["status"] != STATUS_INCONSISTENT,
        "segment_checks": _segment_checks(manifest, usable, n),
        "interpretations": interpretations,
        "witness": witness,
    }
