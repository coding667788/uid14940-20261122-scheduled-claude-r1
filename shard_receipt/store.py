"""SQLite-backed atomic registration for the ``reuse`` command.

Each mutating operation runs in a single IMMEDIATE transaction, so a
process exit between operations can never leave a half-written receipt.
Idempotency is anchored on caller-supplied request ids: replaying a
request reads back the original submission instead of applying twice.

When a concurrent write reveals that a source instance signed two
different values for the same shard, the instance's shards are
quarantined and every *unreleased* manifest of the affected document is
re-checked inside the same transaction.  Already published release
receipts are never rewritten; later quarantine flags are recorded
independently.
"""

import json
import sqlite3
import threading
from contextlib import contextmanager

from .engine import evaluate

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS manifests(
  doc_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  request_id TEXT NOT NULL UNIQUE,
  manifest_json TEXT NOT NULL,
  PRIMARY KEY(doc_id, generation));
CREATE TABLE IF NOT EXISTS receipts(
  submission INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL UNIQUE,
  receipt_json TEXT NOT NULL,
  doc_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  source_instance TEXT NOT NULL,
  source_domain TEXT NOT NULL,
  shard_index INTEGER NOT NULL,
  status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS releases(
  submission INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT NOT NULL UNIQUE,
  doc_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  decision TEXT NOT NULL,
  result_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS quarantine_flags(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  request_id TEXT UNIQUE,
  doc_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  source_instance TEXT NOT NULL,
  reason TEXT NOT NULL,
  detail_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS rechecks(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  trigger_request TEXT NOT NULL,
  doc_id TEXT NOT NULL,
  generation INTEGER NOT NULL,
  decision TEXT NOT NULL,
  result_json TEXT NOT NULL);
"""


def _dump(obj):
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


class Store:
    def __init__(self, db_path):
        self.db_path = db_path
        self._open()

    def _open(self):
        # Autocommit + explicit BEGIN IMMEDIATE: writers serialize on the
        # database lock from the start of the transaction instead of
        # deadlocking on a deferred lock upgrade.
        self.conn = sqlite3.connect(self.db_path, timeout=30.0,
                                    isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(SCHEMA)

    @contextmanager
    def _tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield
        except Exception:
            self.conn.execute("ROLLBACK")
            raise
        else:
            self.conn.execute("COMMIT")

    def close(self):
        self.conn.close()

    def restart(self):
        """Simulate process exit + restart: close and reopen the DB."""
        self.close()
        self._open()
        return {"op": "restart", "reopened": True}

    # ----------------------------------------------------------- helpers
    def _manifest(self, doc_id, generation):
        row = self.conn.execute(
            "SELECT manifest_json FROM manifests WHERE doc_id=? AND generation=?",
            (doc_id, generation)).fetchone()
        return json.loads(row["manifest_json"]) if row else None

    def _receipts(self, doc_id, generation):
        rows = self.conn.execute(
            "SELECT submission, receipt_json, status FROM receipts"
            " WHERE doc_id=? AND generation=? ORDER BY submission",
            (doc_id, generation)).fetchall()
        out = []
        for r in rows:
            rec = json.loads(r["receipt_json"])
            rec["submission"] = r["submission"]
            rec["db_status"] = r["status"]
            out.append(rec)
        return out

    def _recheck_unlocked(self, trigger_request, doc_id):
        """Re-evaluate every unreleased manifest of ``doc_id`` (caller
        holds the transaction)."""
        results = []
        rows = self.conn.execute(
            "SELECT doc_id, generation, manifest_json FROM manifests"
            " WHERE doc_id=? ORDER BY generation", (doc_id,)).fetchall()
        for row in rows:
            released = self.conn.execute(
                "SELECT 1 FROM releases WHERE doc_id=? AND generation=?"
                " AND decision='release'",
                (doc_id, row["generation"])).fetchone()
            if released:
                continue
            manifest = json.loads(row["manifest_json"])
            receipts = self._receipts(doc_id, row["generation"])
            result = evaluate(manifest, receipts, manifest["data_symbols"])
            self.conn.execute(
                "INSERT INTO rechecks(trigger_request, doc_id, generation,"
                " decision, result_json) VALUES (?,?,?,?,?)",
                (trigger_request, doc_id, row["generation"],
                 result["decision"], _dump(result)))
            results.append({"doc_id": doc_id, "generation": row["generation"],
                            "decision": result["decision"],
                            "reasons": result["reasons"]})
        return results

    def _quarantine_instance_unlocked(self, request_id, doc_id, generation,
                                      source_instance, reason, detail):
        self.conn.execute(
            "UPDATE receipts SET status='quarantined'"
            " WHERE doc_id=? AND generation=? AND source_instance=?",
            (doc_id, generation, source_instance))
        self.conn.execute(
            "INSERT OR IGNORE INTO quarantine_flags(request_id, doc_id,"
            " generation, source_instance, reason, detail_json)"
            " VALUES (?,?,?,?,?,?)",
            (request_id, doc_id, generation, source_instance, reason,
             _dump(detail)))

    # -------------------------------------------------------------- ops
    def register_manifest(self, request_id, manifest):
        with self._tx():
            row = self.conn.execute(
                "SELECT doc_id, generation, manifest_json FROM manifests"
                " WHERE request_id=?", (request_id,)).fetchone()
            if row:
                return {"op": "register_manifest", "request_id": request_id,
                        "replayed": True, "doc_id": row["doc_id"],
                        "generation": row["generation"]}
            self.conn.execute(
                "INSERT INTO manifests(doc_id, generation, request_id,"
                " manifest_json) VALUES (?,?,?,?)",
                (manifest["doc_id"], manifest["generation"], request_id,
                 _dump(manifest)))
            return {"op": "register_manifest", "request_id": request_id,
                    "replayed": False, "doc_id": manifest["doc_id"],
                    "generation": manifest["generation"]}

    def submit(self, request_id, receipt):
        with self._tx():
            row = self.conn.execute(
                "SELECT submission, status FROM receipts WHERE request_id=?",
                (request_id,)).fetchone()
            if row:
                return {"op": "submit", "request_id": request_id,
                        "replayed": True, "submission": row["submission"],
                        "status": row["status"]}

            manifest = self._manifest(receipt["doc_id"], receipt["generation"])
            status = self._classify_unlocked(manifest, receipt)
            cur = self.conn.execute(
                "INSERT INTO receipts(request_id, receipt_json, doc_id,"
                " generation, source_instance, source_domain, shard_index,"
                " status) VALUES (?,?,?,?,?,?,?,?)",
                (request_id, _dump(receipt), receipt["doc_id"],
                 receipt["generation"], receipt["source_instance"],
                 receipt["source_domain"], receipt["shard_index"], status))
            submission = cur.lastrowid

            result = {"op": "submit", "request_id": request_id,
                      "replayed": False, "submission": submission,
                      "status": status}

            conflict = self._detect_conflict_unlocked(receipt)
            if conflict:
                inst = receipt["source_instance"]
                self._quarantine_instance_unlocked(
                    "auto:" + request_id, receipt["doc_id"],
                    receipt["generation"], inst, "conflicting_shard", conflict)
                result["conflict_detected"] = conflict
                result["quarantined_source_instance"] = inst
                result["rechecks"] = self._recheck_unlocked(
                    request_id, receipt["doc_id"])
            return result

    def _classify_unlocked(self, manifest, receipt):
        if manifest is None:
            return "no_manifest"
        if receipt["source_domain"] not in manifest["source_domains"]:
            return "out_of_scope"
        if not receipt["trusted"]:
            return "untrusted"
        flagged = self.conn.execute(
            "SELECT 1 FROM quarantine_flags WHERE doc_id=? AND generation=?"
            " AND source_instance=?",
            (receipt["doc_id"], receipt["generation"],
             receipt["source_instance"])).fetchone()
        if flagged:
            return "quarantined"
        if receipt["time"] > manifest["deadline"]:
            return "archived_late"
        dup = self.conn.execute(
            "SELECT 1 FROM receipts WHERE doc_id=? AND generation=?"
            " AND source_instance=? AND shard_index=? AND receipt_json=?"
            " AND status='usable'",
            (receipt["doc_id"], receipt["generation"],
             receipt["source_instance"], receipt["shard_index"],
             _dump(receipt))).fetchone()
        if dup:
            return "duplicate"
        return "usable"

    def _detect_conflict_unlocked(self, receipt):
        rows = self.conn.execute(
            "SELECT submission, receipt_json FROM receipts WHERE doc_id=?"
            " AND generation=? AND source_instance=? AND shard_index=?",
            (receipt["doc_id"], receipt["generation"],
             receipt["source_instance"], receipt["shard_index"])).fetchall()
        canon = lambda r: (tuple(x % 257 for x in r["row"]), r["payload"] % 257)
        mine = canon(receipt)
        others = []
        for row in rows:
            other = json.loads(row["receipt_json"])
            if canon(other) != mine:
                others.append(other.get("receipt_id"))
        if others:
            return {"source_instance": receipt["source_instance"],
                    "shard_index": receipt["shard_index"],
                    "conflicting_receipt_ids": sorted(
                        others + [receipt.get("receipt_id", "")])}
        return None

    def release(self, request_id, doc_id, generation):
        with self._tx():
            row = self.conn.execute(
                "SELECT submission, decision, result_json FROM releases"
                " WHERE request_id=?", (request_id,)).fetchone()
            if row:
                out = json.loads(row["result_json"])
                out["replayed"] = True
                return out
            manifest = self._manifest(doc_id, generation)
            if manifest is None:
                result = {"op": "release", "request_id": request_id,
                          "replayed": False, "decision": "hold",
                          "reasons": ["unknown_manifest"],
                          "doc_id": doc_id, "generation": generation}
                self.conn.execute(
                    "INSERT INTO releases(request_id, doc_id, generation,"
                    " decision, result_json) VALUES (?,?,?,?,?)",
                    (request_id, doc_id, generation, "hold", _dump(result)))
                return result
            receipts = self._receipts(doc_id, generation)
            evaluation = evaluate(manifest, receipts, manifest["data_symbols"])
            submissions = {r["receipt_id"]: r["submission"] for r in receipts}
            result = {"op": "release", "request_id": request_id,
                      "replayed": False,
                      "decision": evaluation["decision"],
                      "reasons": evaluation["reasons"],
                      "doc_id": doc_id, "generation": generation,
                      "receipt_submissions": submissions,
                      "witness": evaluation["witness"],
                      "evaluation": evaluation}
            cur = self.conn.execute(
                "INSERT INTO releases(request_id, doc_id, generation,"
                " decision, result_json) VALUES (?,?,?,?,?)",
                (request_id, doc_id, generation, evaluation["decision"],
                 _dump(result)))
            result["release_submission"] = cur.lastrowid
            # Persist again with the submission number included so replays
            # read back the exact original record.
            self.conn.execute(
                "UPDATE releases SET result_json=? WHERE request_id=?",
                (_dump(result), request_id))
            return result

    def flag_quarantine(self, request_id, doc_id, generation,
                        source_instance, reason):
        """Record a quarantine flag independently; never rewrites an
        existing release receipt, but re-checks unreleased manifests."""
        with self._tx():
            row = self.conn.execute(
                "SELECT id FROM quarantine_flags WHERE request_id=?",
                (request_id,)).fetchone()
            if row:
                return {"op": "flag_quarantine", "request_id": request_id,
                        "replayed": True, "flag_id": row["id"]}
            self._quarantine_instance_unlocked(
                request_id, doc_id, generation, source_instance, reason,
                {"manual_flag": True})
            flag_id = self.conn.execute(
                "SELECT id FROM quarantine_flags WHERE request_id=?",
                (request_id,)).fetchone()["id"]
            rechecks = self._recheck_unlocked(request_id, doc_id)
            return {"op": "flag_quarantine", "request_id": request_id,
                    "replayed": False, "flag_id": flag_id,
                    "rechecks": rechecks}

    def status(self, doc_id, generation):
        manifest = self._manifest(doc_id, generation)
        receipts = self._receipts(doc_id, generation)
        releases = [dict(r) for r in self.conn.execute(
            "SELECT submission, request_id, decision FROM releases"
            " WHERE doc_id=? AND generation=? ORDER BY submission",
            (doc_id, generation)).fetchall()]
        flags = [dict(r) for r in self.conn.execute(
            "SELECT id, request_id, source_instance, reason FROM"
            " quarantine_flags WHERE doc_id=? AND generation=? ORDER BY id",
            (doc_id, generation)).fetchall()]
        rechecks = [dict(r) for r in self.conn.execute(
            "SELECT id, trigger_request, decision FROM rechecks"
            " WHERE doc_id=? AND generation=? ORDER BY id",
            (doc_id, generation)).fetchall()]
        return {
            "op": "status", "doc_id": doc_id, "generation": generation,
            "manifest_registered": manifest is not None,
            "receipts": [{"submission": r["submission"],
                          "receipt_id": r["receipt_id"],
                          "status": r["db_status"]} for r in receipts],
            "releases": releases,
            "quarantine_flags": flags,
            "rechecks": rechecks,
        }

    # ------------------------------------------------- op dispatch
    def apply_op(self, op, index):
        kind = op.get("op")
        if kind == "register_manifest":
            return self.register_manifest(op["request_id"], op["manifest"])
        if kind == "submit":
            return self.submit(op["request_id"], op["receipt"])
        if kind == "concurrent_submit":
            return self._concurrent_submit(op["requests"])
        if kind == "release":
            return self.release(op["request_id"], op["doc_id"], op["generation"])
        if kind == "flag_quarantine":
            return self.flag_quarantine(op["request_id"], op["doc_id"],
                                        op["generation"],
                                        op["source_instance"],
                                        op.get("reason", "manual"))
        if kind == "restart":
            return self.restart()
        if kind == "status":
            return self.status(op["doc_id"], op["generation"])
        raise ValueError("ops[%d]: unknown op %r" % (index, kind))

    def _concurrent_submit(self, requests):
        """Submit receipts from concurrent writer threads, each with its
        own connection; SQLite serializes the IMMEDIATE transactions."""
        results = [None] * len(requests)
        errors = []

        def worker(i, req):
            try:
                store = Store(self.db_path)
                try:
                    results[i] = store.submit(req["request_id"], req["receipt"])
                finally:
                    store.close()
            except Exception as exc:  # surfaced deterministically below
                errors.append((i, repr(exc)))

        threads = [threading.Thread(target=worker, args=(i, req))
                   for i, req in enumerate(requests)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        if errors:
            raise RuntimeError("concurrent_submit failed: %r" % (errors,))
        conflicts = [r for r in results if r and r.get("conflict_detected")]
        return {"op": "concurrent_submit",
                "results": results,
                "conflicts_detected": len(conflicts)}
