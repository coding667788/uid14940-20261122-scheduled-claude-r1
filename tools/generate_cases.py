"""Deterministically generate the runnable cases under cases/.

Run from the repo root:

    python tools/generate_cases.py

The generator uses a fixed seed and the library's own canonical
commitment, so the emitted JSON is byte-stable across runs.
"""

import json
import os
import random
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shard_receipt.canon import commitment_of  # noqa: E402
from shard_receipt.gf257 import MOD  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..", "cases")

N = 12
DOMAINS = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]
SEGMENTS = [("head", 0, 4), ("mid", 4, 4), ("tail", 8, 4)]


def dot(row, x):
    return sum(a * b for a, b in zip(row, x)) % MOD


def segment_row(rng, start, length):
    row = [0] * N
    for i in range(start, start + length):
        row[i] = rng.randrange(MOD)
    return row


def make_shards(rng, x, domain, instance, doc, gen, count_per_segment=2,
                start_index=0):
    """Rows whose support lies inside one segment, two per segment."""
    shards = []
    idx = start_index
    for (_name, start, length) in SEGMENTS:
        for _ in range(count_per_segment):
            row = segment_row(rng, start, length)
            shards.append({
                "link": "link-%s-%d" % (instance, idx),
                "source_instance": instance,
                "source_domain": domain,
                "doc_id": doc,
                "generation": gen,
                "shard_index": idx,
                "trusted": True,
                "row": row,
                "payload": dot(row, x),
            })
            idx += 1
    return shards


def build_explain_case():
    rng = random.Random(20261122)
    x = [rng.randrange(MOD) for _ in range(N)]
    commitment = commitment_of(x)

    # Good shards for every domain; gamma has two instances.
    pool = {}
    for d in DOMAINS:
        inst = "%s-1" % d
        pool[inst] = make_shards(rng, x, d, inst, "DOC-1", 1)
    pool["gamma-2"] = make_shards(rng, x, "gamma", "gamma-2", "DOC-1", 1,
                                  start_index=100)

    # beta-1 signs two wrong payloads (unreliable source domain).
    pool["beta-1"][0]["payload"] = (pool["beta-1"][0]["payload"] + 1) % MOD
    pool["beta-1"][3]["payload"] = (pool["beta-1"][3]["payload"] + 2) % MOD

    # gamma-2 double-signs shard 100 with a different row+payload.
    conflict_shard = dict(pool["gamma-2"][0])
    conflict_shard["row"] = segment_row(rng, 0, 4)
    conflict_shard["payload"] = rng.randrange(MOD)
    conflict_shard["link"] = "link-gamma-2-100b"

    # delta-1 shard 5 arrives after the deadline.
    pool["delta-1"][5]["_late"] = True
    # epsilon-1 shard 4 is flagged untrusted by the receiver.
    pool["epsilon-1"][4]["_untrusted"] = True

    # Interleave events so early prefixes are underdetermined.  All
    # generation-1 events stay before the deadline (100) except the one
    # deliberately late delta shard; generation-2 events use a separate
    # time window within their own deadline (500).
    events = []
    order = ["alpha-1", "beta-1", "gamma-1", "delta-1", "epsilon-1", "zeta-1",
             "gamma-2"]
    t = 0
    rid = 0

    def emit(shard, time):
        nonlocal t, rid
        rid += 1
        ev = {"type": "receipt", "receipt_id": "rcpt-%03d" % rid,
              "time": time}
        for k, v in shard.items():
            if not k.startswith("_"):
                ev[k] = v
        if shard.get("_untrusted"):
            ev["trusted"] = False
        events.append(ev)

    rounds = max(len(pool[i]) for i in order)
    for r in range(rounds):
        for inst in order:
            shards = pool[inst]
            if r < len(shards):
                t += 2
                shard = shards[r]
                emit(shard, 150 if shard.get("_late") else t)
                # Duplicate delivery of alpha-1 shard 0 over a second link.
                if inst == "alpha-1" and r == 0:
                    dup = dict(shard)
                    dup["link"] = "link-alpha-1-0-relay"
                    t += 2
                    emit(dup, t)
                # gamma-2's conflicting re-signature of shard 100.
                if inst == "gamma-2" and r == 0:
                    t += 2
                    emit(conflict_shard, t)

    # A second generation of the same document: isolation check.
    x2 = [rng.randrange(MOD) for _ in range(N)]
    gen2_shards = []
    for d in DOMAINS[:4]:
        gen2_shards += make_shards(rng, x2, d, "%s-1" % d, "DOC-1", 2)
    t = 110
    for shard in gen2_shards:
        t += 2
        emit(shard, t)

    case = {
        "case_id": "case-explain-1",
        "data_symbols": N,
        "manifests": [
            {
                "manifest_id": "M-DOC1-G1",
                "doc_id": "DOC-1",
                "generation": 1,
                "source_domains": DOMAINS,
                "recipient_domain": "inbox-1",
                "deadline": 100,
                "commitment": commitment,
                "data_symbols": N,
                "segments": [{"name": nm, "start": s, "length": l}
                             for (nm, s, l) in SEGMENTS],
            },
            {
                "manifest_id": "M-DOC1-G2",
                "doc_id": "DOC-1",
                "generation": 2,
                "source_domains": DOMAINS,
                "recipient_domain": "inbox-1",
                "deadline": 500,
                "commitment": commitment_of(x2),
                "data_symbols": N,
            },
        ],
        "events": events,
    }
    return case


def build_reuse_case():
    rng = random.Random(20261123)
    y = [rng.randrange(MOD) for _ in range(N)]
    commitment = commitment_of(y)

    manifest = {
        "manifest_id": "M-DOCR-G1",
        "doc_id": "DOC-R",
        "generation": 1,
        "source_domains": DOMAINS,
        "recipient_domain": "inbox-1",
        "deadline": 100,
        "commitment": commitment,
        "data_symbols": N,
        "segments": [{"name": nm, "start": s, "length": l}
                     for (nm, s, l) in SEGMENTS],
    }
    # Second generation, deliberately starved of shards: its release
    # attempt must come back underdetermined.
    y2 = [rng.randrange(MOD) for _ in range(N)]
    manifest2 = dict(manifest)
    manifest2.update({"manifest_id": "M-DOCR-G2", "generation": 2,
                      "commitment": commitment_of(y2)})
    manifest2.pop("segments", None)

    ops = [
        {"op": "register_manifest", "request_id": "reg-g1",
         "manifest": manifest},
        {"op": "register_manifest", "request_id": "reg-g2",
         "manifest": manifest2},
    ]

    t = 0
    rid = 0

    def submit_op(shard, req):
        nonlocal t, rid
        rid += 1
        t += 2
        rec = {"receipt_id": "rr-%03d" % rid, "time": t}
        rec.update(shard)
        return {"op": "submit", "request_id": req, "receipt": rec}

    # Four rows per domain -> any five domains still span GF(257)^12.
    for d in DOMAINS:
        for shard in make_shards(rng, y, d, "%s-1" % d, "DOC-R", 1,
                                 count_per_segment=2):
            ops.append(submit_op(shard, "sub-%s-%d" % (d, shard["shard_index"])))

    # A couple of generation-2 shards only: never enough rank.
    for shard in make_shards(rng, y2, "alpha", "alpha-1", "DOC-R", 2,
                             count_per_segment=1):
        ops.append(submit_op(shard, "sub-g2-%d" % shard["shard_index"]))

    # Process restart, then an exact replay of an earlier request.
    ops.append({"op": "restart"})
    replayed = dict(ops[2])
    ops.append(replayed)

    # Concurrent writers: delta-1 signs shard 0 twice with different
    # payloads; the race must quarantine delta-1 and re-check the
    # unreleased manifests.
    base = None
    for op in ops:
        if op["op"] == "submit" and op["receipt"]["source_instance"] == "delta-1" \
                and op["receipt"]["shard_index"] == 0:
            base = op["receipt"]
            break
    bad_a = dict(base)
    bad_a["receipt_id"] = "rr-conflict-a"
    bad_a["payload"] = (base["payload"] + 7) % MOD
    bad_b = dict(base)
    bad_b["receipt_id"] = "rr-conflict-b"
    bad_b["payload"] = (base["payload"] + 9) % MOD
    ops.append({"op": "concurrent_submit", "requests": [
        {"request_id": "race-a", "receipt": bad_a},
        {"request_id": "race-b", "receipt": bad_b},
    ]})

    ops.append({"op": "status", "doc_id": "DOC-R", "generation": 1})
    ops.append({"op": "release", "request_id": "rel-g1",
                "doc_id": "DOC-R", "generation": 1})
    # Replay of the release request: must read back the original record.
    ops.append({"op": "release", "request_id": "rel-g1",
                "doc_id": "DOC-R", "generation": 1})
    # Starved generation cannot be released.
    ops.append({"op": "release", "request_id": "rel-g2",
                "doc_id": "DOC-R", "generation": 2})
    # Post-release quarantine flag: recorded independently, the stored
    # release receipt is not rewritten.
    ops.append({"op": "flag_quarantine", "request_id": "flag-gamma",
                "doc_id": "DOC-R", "generation": 1,
                "source_instance": "gamma-1",
                "reason": "late_external_evidence"})
    ops.append({"op": "restart"})
    ops.append({"op": "status", "doc_id": "DOC-R", "generation": 1})
    ops.append({"op": "status", "doc_id": "DOC-R", "generation": 2})

    return {"case_id": "case-reuse-1", "data_symbols": N, "ops": ops}


def main():
    os.makedirs(ROOT, exist_ok=True)
    for name, case in [("case_explain_1.json", build_explain_case()),
                       ("case_reuse_1.json", build_reuse_case())]:
        path = os.path.join(ROOT, name)
        with open(path, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(case, fh, ensure_ascii=False, sort_keys=True, indent=2)
            fh.write("\n")
        print("wrote", path)


if __name__ == "__main__":
    main()
