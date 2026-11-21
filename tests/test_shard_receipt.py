"""Unittest suite for shard_receipt.

Covers the acceptance scenarios:

* full shard count but under-rank            -> null-space evidence
* bad shards spread across two domains       -> no consistent interpretation
* single-domain lone evidence                -> cannot be released
* same doc id, different generations         -> no cross pollution
* collaborative shards expired               -> insufficient rank
* reuse: replay / restart / concurrent mis-sign recheck / read-back
"""

import json
import os
import random
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from shard_receipt.canon import canonical_bytes, commitment_of  # noqa: E402
from shard_receipt.engine import evaluate  # noqa: E402
from shard_receipt.gf257 import MOD, mat_vec, solve_system  # noqa: E402

ROOT = os.path.join(os.path.dirname(__file__), "..")
N = 12
DOMAINS = ["alpha", "beta", "gamma", "delta", "epsilon", "zeta"]


def mk_manifest(commitment, doc="DOC-T", gen=1, deadline=1000, domains=None):
    return {
        "manifest_id": "M-%s-%d" % (doc, gen),
        "doc_id": doc,
        "generation": gen,
        "source_domains": domains or list(DOMAINS),
        "recipient_domain": "inbox-1",
        "deadline": deadline,
        "commitment": commitment,
        "data_symbols": N,
    }


class ShardFactory:
    """Deterministic shard builder for a true data vector."""

    def __init__(self, seed, x):
        self.rng = random.Random(seed)
        self.x = x
        self.counter = 0

    def shard(self, domain, doc="DOC-T", gen=1, time=1, trusted=True,
              row=None, payload=None, instance=None):
        self.counter += 1
        if row is None:
            row = [self.rng.randrange(MOD) for _ in range(N)]
        if payload is None:
            payload = sum(a * b for a, b in zip(row, self.x)) % MOD
        return {
            "receipt_id": "t-%04d" % self.counter,
            "time": time,
            "link": "L",
            "source_instance": instance or ("%s-1" % domain),
            "source_domain": domain,
            "doc_id": doc,
            "generation": gen,
            "shard_index": self.counter,
            "trusted": trusted,
            "row": row,
            "payload": payload,
        }


def random_vector(seed):
    return [random.Random(seed).randrange(MOD) for _ in range(N)]


def full_rank_shards(factory, domains, per_domain=3, **kw):
    """Shards whose rows span GF(257)^N; with more than one domain,
    spanning must survive dropping any single domain."""
    require_redundant = len(domains) > 1
    for _attempt in range(1000):
        shards = [factory.shard(d, **kw) for d in domains
                  for _ in range(per_domain)]
        rows = [s["row"] for s in shards]
        if solve_system([("r%d" % i, r, 0) for i, r in enumerate(rows)],
                        N)["rank"] != N:
            continue
        if not require_redundant:
            return shards
        ok = all(
            solve_system(
                [("r%d" % i, s["row"], 0)
                 for i, s in enumerate(shards) if s["source_domain"] != d],
                N)["rank"] == N
            for d in domains)
        if ok:
            return shards
    raise AssertionError("could not build full-rank shards in 1000 attempts")


class TestGf257(unittest.TestCase):
    def test_unique_solution(self):
        x = random_vector(1)
        f = ShardFactory(2, x)
        shards = full_rank_shards(f, DOMAINS, per_domain=3)
        res = solve_system([(s["receipt_id"], s["row"], s["payload"])
                            for s in shards], N)
        self.assertEqual(res["status"], "unique")
        self.assertEqual(res["solution"], x)

    def test_under_rank_null_vector(self):
        rng = random.Random(3)
        rows = [[rng.randrange(MOD) for _ in range(N)] for _ in range(N - 2)]
        res = solve_system([("r%d" % i, r, 0) for i, r in enumerate(rows)], N)
        self.assertEqual(res["status"], "underdetermined")
        nv = res["null_vector"]
        self.assertTrue(any(v != 0 for v in nv))
        self.assertEqual(mat_vec(rows, nv), [0] * len(rows))

    def test_inconsistent_conflict_evidence(self):
        x = random_vector(4)
        f = ShardFactory(5, x)
        shards = full_rank_shards(f, DOMAINS, per_domain=3)
        shards[0]["payload"] = (shards[0]["payload"] + 1) % MOD
        res = solve_system([(s["receipt_id"], s["row"], s["payload"])
                            for s in shards], N)
        self.assertEqual(res["status"], "inconsistent")
        comb = res["conflict"]["combination"]
        by_id = {s["receipt_id"]: s for s in shards}
        lhs = [0] * N
        rhs = 0
        for term in comb:
            s = by_id[term["row_id"]]
            lhs = [(a + term["coeff"] * b) % MOD for a, b in zip(lhs, s["row"])]
            rhs = (rhs + term["coeff"] * s["payload"]) % MOD
        self.assertEqual(lhs, [0] * N)
        self.assertEqual(rhs, res["conflict"]["rhs"])
        self.assertNotEqual(rhs, 0)


class TestCommitment(unittest.TestCase):
    def test_canonical_encoding_fixed_width(self):
        self.assertEqual(canonical_bytes([0, 1, 256]),
                         b"\x00\x00\x01\x00\x00\x01")

    def test_commitment_stable_and_sensitive(self):
        x = random_vector(6)
        c1 = commitment_of(x)
        self.assertTrue(c1.startswith("sha256:"))
        self.assertEqual(len(c1), 7 + 64)
        y = list(x)
        y[0] = (y[0] + 1) % MOD
        self.assertNotEqual(c1, commitment_of(y))


class TestExplainScenarios(unittest.TestCase):
    def test_full_shard_count_but_under_rank(self):
        """A full set of shards whose rows span only a subspace must be
        held back with a non-zero null-space vector as evidence."""
        x = random_vector(7)
        f = ShardFactory(8, x)
        # 12 shards (= symbol count) from 6 domains, but rows live in a
        # rank-10 subspace: last two coordinates always zero.
        shards = []
        for d in DOMAINS:
            for _ in range(2):
                row = [f.rng.randrange(MOD) for _ in range(N - 2)] + [0, 0]
                shards.append(f.shard(d, row=row))
        manifest = mk_manifest(commitment_of(x))
        result = evaluate(manifest, shards, N)
        self.assertEqual(result["decision"], "hold")
        self.assertTrue(any(r.startswith("underdetermined")
                            for r in result["reasons"]))
        no_fault = result["interpretations"][0]
        self.assertEqual(no_fault["status"], "underdetermined")
        nv = no_fault["evidence"]["null_vector"]
        self.assertTrue(any(v != 0 for v in nv))
        rows = [s["row"] for s in shards]
        self.assertEqual(mat_vec(rows, nv), [0] * len(rows))

    def test_bad_shards_spread_across_two_domains(self):
        """One unreliable domain is tolerable; two is not.  No
        interpretation is consistent, and conflict evidence is kept."""
        x = random_vector(9)
        f = ShardFactory(10, x)
        shards = full_rank_shards(f, DOMAINS, per_domain=3)
        for s in shards:
            if s["source_domain"] in ("beta", "epsilon"):
                s["payload"] = (s["payload"] + 1) % MOD
        manifest = mk_manifest(commitment_of(x))
        result = evaluate(manifest, shards, N)
        self.assertEqual(result["decision"], "hold")
        self.assertIn("no_consistent_interpretation", result["reasons"])
        for interp in result["interpretations"]:
            self.assertEqual(interp["status"], "inconsistent")
            self.assertEqual(interp["evidence"]["type"], "conflict")
            self.assertTrue(interp["evidence"]["combination"])
        self.assertIsNone(result["witness"])

    def test_single_domain_lone_evidence_cannot_release(self):
        """Shards from one domain only: removing that domain leaves an
        empty (underdetermined) system, so release is impossible."""
        x = random_vector(11)
        f = ShardFactory(12, x)
        shards = full_rank_shards(f, ["alpha"], per_domain=N + 2)
        manifest = mk_manifest(commitment_of(x))
        result = evaluate(manifest, shards, N)
        self.assertEqual(result["decision"], "hold")
        self.assertIn("underdetermined:alpha", result["reasons"])
        no_fault = result["interpretations"][0]
        self.assertEqual(no_fault["status"], "unique")
        self.assertTrue(no_fault["digest_match"])

    def test_same_doc_different_generations_isolated(self):
        x1 = random_vector(13)
        x2 = random_vector(14)
        f = ShardFactory(15, x1)
        gen1 = full_rank_shards(f, DOMAINS, per_domain=3, gen=1)
        f.x = x2
        gen2 = full_rank_shards(f, DOMAINS, per_domain=3, gen=2)
        m1 = mk_manifest(commitment_of(x1), gen=1)
        m2 = mk_manifest(commitment_of(x2), gen=2)
        r1 = evaluate(m1, gen1 + gen2, N)
        r2 = evaluate(m2, gen1 + gen2, N)
        self.assertEqual(r1["decision"], "release")
        self.assertEqual(r2["decision"], "release")
        self.assertEqual(r1["witness"]["solution"], x1)
        self.assertEqual(r2["witness"]["solution"], x2)
        self.assertEqual(r1["counts"]["out_of_scope"], len(gen2))
        self.assertEqual(r2["counts"]["out_of_scope"], len(gen1))

    def test_expired_collaboration_insufficient_rank(self):
        """Two domains each provide half a basis; one domain's shards
        arrive after the deadline, so the rank collapses."""
        x = random_vector(16)
        f = ShardFactory(17, x)
        shards = []
        # alpha covers the first half of the coordinates, beta the rest.
        for i in range(6):
            row = [0] * N
            row[i] = 1
            shards.append(f.shard("alpha", row=row, time=10))
        for i in range(6, N):
            row = [0] * N
            row[i] = 1
            shards.append(f.shard("beta", row=row, time=2000))  # late
        manifest = mk_manifest(commitment_of(x), deadline=1000)
        result = evaluate(manifest, shards, N)
        self.assertEqual(result["decision"], "hold")
        self.assertEqual(result["counts"]["archived_late"], 6)
        no_fault = result["interpretations"][0]
        self.assertEqual(no_fault["status"], "underdetermined")
        self.assertEqual(no_fault["rank"], 6)

    def test_conflicting_signature_quarantines_source(self):
        x = random_vector(18)
        f = ShardFactory(19, x)
        shards = full_rank_shards(f, DOMAINS, per_domain=3)
        # gamma-1 re-signs the same shard index with a different value.
        victim = dict(shards[3])
        victim["receipt_id"] = "t-evil"
        victim["source_instance"] = "gamma-1"
        victim["shard_index"] = shards[3]["shard_index"]
        victim["payload"] = (victim["payload"] + 5) % MOD
        # Make the original also gamma-1 so the instance double-signs.
        shards[3]["source_instance"] = "gamma-1"
        manifest = mk_manifest(commitment_of(x))
        result = evaluate(manifest, shards + [victim], N)
        self.assertIn("gamma-1", result["quarantined_sources"])
        self.assertGreater(result["counts"]["quarantined"], 0)

    def test_duplicate_delivery_counted_once(self):
        x = random_vector(20)
        f = ShardFactory(21, x)
        shards = full_rank_shards(f, DOMAINS, per_domain=3)
        dup = dict(shards[0])
        dup["receipt_id"] = "t-dup"
        dup["link"] = "other-link"
        manifest = mk_manifest(commitment_of(x))
        result = evaluate(manifest, shards + [dup], N)
        self.assertEqual(result["counts"]["duplicates"], 1)
        self.assertEqual(result["decision"], "release")


class TestCli(unittest.TestCase):
    def run_cli(self, *args, cwd=ROOT):
        return subprocess.run(
            [sys.executable, "-m", "shard_receipt", *args],
            cwd=cwd, capture_output=True)

    def test_explain_case_releases_and_is_deterministic(self):
        case = os.path.join("cases", "case_explain_1.json")
        p1 = self.run_cli("explain", case)
        p2 = self.run_cli("explain", case)
        self.assertEqual(p1.returncode, 0, p1.stderr.decode("utf-8", "replace"))
        self.assertEqual(p1.stdout, p2.stdout)
        out = json.loads(p1.stdout.decode("utf-8"))
        last = out["prefixes"][-1]
        decisions = {m["manifest_id"]: m["decision"] for m in last["manifests"]}
        self.assertEqual(decisions["M-DOC1-G1"], "release")
        self.assertEqual(decisions["M-DOC1-G2"], "release")
        g1 = [m for m in last["manifests"]
              if m["manifest_id"] == "M-DOC1-G1"][0]
        # The witness must recompute: basis rows -> solution -> commitment.
        w = g1["witness"]
        self.assertIsNotNone(w)
        with open(os.path.join(ROOT, case), encoding="utf-8") as fh:
            case_data = json.load(fh)
        by_id = {e["receipt_id"]: e for e in case_data["events"]}
        eqs = [(rid, by_id[rid]["row"], by_id[rid]["payload"])
               for rid in w["receipt_ids"]]
        res = solve_system(eqs, N)
        self.assertEqual(res["status"], "unique")
        self.assertEqual(res["solution"], w["solution"])
        self.assertEqual(commitment_of(res["solution"]), w["commitment"])

    def test_explain_input_error_is_locatable(self):
        with tempfile.TemporaryDirectory() as td:
            bad = os.path.join(td, "bad.json")
            with open(bad, "w", encoding="utf-8") as fh:
                json.dump({"case_id": "x", "manifests": [{"manifest_id": "m"}],
                           "events": []}, fh)
            p = self.run_cli("explain", bad)
            self.assertEqual(p.returncode, 2)
            err = json.loads(p.stderr.decode("utf-8"))
            self.assertIn("manifests[0]", err["error"]["path"])

    def test_scale_32_rows_12_symbols_6_domains(self):
        """Typical scale: solved by elimination, never by enumerating the
        257^12 data vectors."""
        case = os.path.join("cases", "case_explain_1.json")
        p = self.run_cli("explain", case)
        self.assertEqual(p.returncode, 0)
        out = json.loads(p.stdout.decode("utf-8"))
        g1 = out["prefixes"][-1]["manifests"][0]
        self.assertGreaterEqual(g1["counts"]["usable"], 32)


class TestReuse(unittest.TestCase):
    def setUp(self):
        self.td = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.td.name, "reuse.db")
        self.case = os.path.join("cases", "case_reuse_1.json")

    def tearDown(self):
        self.td.cleanup()

    def run_reuse(self):
        return subprocess.run(
            [sys.executable, "-m", "shard_receipt", "reuse", self.case,
             "--db", self.db], cwd=ROOT, capture_output=True)

    def test_reuse_case_end_to_end(self):
        p = self.run_reuse()
        self.assertEqual(p.returncode, 0, p.stderr.decode("utf-8", "replace"))
        out = json.loads(p.stdout.decode("utf-8"))
        results = out["results"]

        submits = [r for r in results if r["op"] == "submit"]
        replayed = [r for r in submits if r["replayed"]]
        self.assertEqual(len(replayed), 1)
        self.assertEqual(replayed[0]["submission"], 1)

        conc = [r for r in results if r["op"] == "concurrent_submit"][0]
        self.assertGreaterEqual(conc["conflicts_detected"], 1)
        rechecks = [r for s in conc["results"]
                    for r in s.get("rechecks", [])]
        self.assertTrue(any(r["generation"] == 1 for r in rechecks))

        releases = [r for r in results if r["op"] == "release"]
        g1 = [r for r in releases if r["request_id"] == "rel-g1"]
        self.assertEqual(g1[0]["decision"], "release")
        self.assertTrue(g1[1]["replayed"])
        self.assertEqual(g1[0]["release_submission"],
                         g1[1]["release_submission"])
        g2 = [r for r in releases if r["request_id"] == "rel-g2"][0]
        self.assertEqual(g2["decision"], "hold")
        self.assertTrue(any(r.startswith("underdetermined")
                            for r in g2["reasons"]))

        final = [r for r in results if r["op"] == "status"
                 and r["generation"] == 1][-1]
        self.assertEqual(len(final["releases"]), 1)
        self.assertEqual(len(final["quarantine_flags"]), 3)

    def test_replay_after_process_restart_reads_original(self):
        """Run the whole case, then rerun it against the same DB: every
        request must replay against its original submission."""
        p1 = self.run_reuse()
        self.assertEqual(p1.returncode, 0)
        p2 = self.run_reuse()
        self.assertEqual(p2.returncode, 0, p2.stderr.decode("utf-8", "replace"))
        out = json.loads(p2.stdout.decode("utf-8"))
        for r in out["results"]:
            if r["op"] in ("submit", "register_manifest", "release"):
                self.assertTrue(r["replayed"],
                                "op %s not replayed: %r" % (r["op"], r))
        g1 = [r for r in out["results"]
              if r["op"] == "release" and r["request_id"] == "rel-g1"][0]
        self.assertEqual(g1["release_submission"], 1)
        self.assertEqual(g1["decision"], "release")

    def test_post_release_quarantine_does_not_rewrite_release(self):
        p1 = self.run_reuse()
        self.assertEqual(p1.returncode, 0)
        out = json.loads(p1.stdout.decode("utf-8"))
        releases = [r for r in out["results"] if r["op"] == "release"
                    and r["request_id"] == "rel-g1"]
        # The flag_quarantine op comes after the release; the stored
        # release receipt must be byte-identical on read-back.
        self.assertEqual(releases[0]["decision"], releases[1]["decision"])
        self.assertEqual(releases[0]["witness"], releases[1]["witness"])
        flags = [r for r in out["results"] if r["op"] == "flag_quarantine"]
        self.assertEqual(len(flags), 1)
        self.assertFalse(flags[0]["replayed"])


if __name__ == "__main__":
    unittest.main()
