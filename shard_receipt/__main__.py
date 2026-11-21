"""Command line interface.

    python -m shard_receipt explain CASE.json
    python -m shard_receipt reuse   CASE.json --db PATH

Output is deterministic UTF-8 JSON (sorted keys, LF line endings) on
stdout.  Structural input errors are reported as a JSON object on
stderr with a locatable path and exit code 2.
"""

import argparse
import json
import sys

from . import __version__, FIELD_MODULUS
from .engine import evaluate
from .model import CaseError, load_case, validate_manifest, validate_receipt
from .store import Store


def _emit(obj):
    data = json.dumps(obj, ensure_ascii=False, sort_keys=True, indent=2) + "\n"
    sys.stdout.buffer.write(data.encode("utf-8"))


def _fail(exc):
    sys.stderr.buffer.write(
        (json.dumps(exc.to_json(), ensure_ascii=False, sort_keys=True) + "\n")
        .encode("utf-8"))
    return 2


def cmd_explain(case_path):
    case = load_case(case_path)
    events = case["events"]
    prefixes = []
    for k in range(len(events) + 1):
        seen = events[:k]
        manifests_out = []
        for m in case["manifests"]:
            manifests_out.append(evaluate(m, seen, m["data_symbols"]))
        prefixes.append({"prefix_events": k, "manifests": manifests_out})
    return {
        "command": "explain",
        "case_id": case["case_id"],
        "field_modulus": FIELD_MODULUS,
        "tool_version": __version__,
        "prefixes": prefixes,
    }


def _validate_op(op, index, n_default):
    path = "ops[%d]" % index
    if not isinstance(op, dict) or "op" not in op:
        raise CaseError(path, "missing 'op' key")
    kind = op["op"]
    if kind == "register_manifest":
        validate_manifest(op.get("manifest"), path + ".manifest", n_default)
    elif kind in ("submit",):
        validate_receipt(op.get("receipt"), path + ".receipt")
    elif kind == "concurrent_submit":
        reqs = op.get("requests")
        if not isinstance(reqs, list) or not reqs:
            raise CaseError(path + ".requests", "expected non-empty list")
        for j, req in enumerate(reqs):
            if not isinstance(req, dict) or "request_id" not in req:
                raise CaseError("%s.requests[%d]" % (path, j),
                                "missing request_id")
            validate_receipt(req.get("receipt"),
                             "%s.requests[%d].receipt" % (path, j))
    elif kind in ("release", "status"):
        for key in ("doc_id", "generation"):
            if key not in op:
                raise CaseError(path, "missing %r" % key)
    elif kind == "flag_quarantine":
        for key in ("doc_id", "generation", "source_instance"):
            if key not in op:
                raise CaseError(path, "missing %r" % key)
    elif kind == "restart":
        pass
    else:
        raise CaseError(path + ".op", "unknown op %r" % (kind,))


def cmd_reuse(case_path, db_path):
    try:
        with open(case_path, "r", encoding="utf-8") as fh:
            case = json.load(fh)
    except json.JSONDecodeError as exc:
        raise CaseError("$", "invalid JSON: %s" % exc)
    if not isinstance(case, dict) or "ops" not in case:
        raise CaseError("$", "expected object with 'ops' list")
    ops = case["ops"]
    if not isinstance(ops, list):
        raise CaseError("$.ops", "expected list")
    n_default = case.get("data_symbols", 12)
    for i, op in enumerate(ops):
        _validate_op(op, i, n_default)

    store = Store(db_path)
    results = []
    try:
        for i, op in enumerate(ops):
            res = store.apply_op(op, i)
            res_with_index = {"op_index": i}
            res_with_index.update(res)
            results.append(res_with_index)
    finally:
        store.close()
    return {
        "command": "reuse",
        "case_id": case.get("case_id", ""),
        "field_modulus": FIELD_MODULUS,
        "tool_version": __version__,
        "db": db_path,
        "results": results,
    }


def main(argv=None):
    parser = argparse.ArgumentParser(prog="shard_receipt")
    sub = parser.add_subparsers(dest="command", required=True)
    p_ex = sub.add_parser("explain", help="verify event prefixes of a case")
    p_ex.add_argument("case")
    p_re = sub.add_parser("reuse", help="replay ops against a SQLite store")
    p_re.add_argument("case")
    p_re.add_argument("--db", required=True, help="SQLite database path")
    args = parser.parse_args(argv)

    try:
        if args.command == "explain":
            out = cmd_explain(args.case)
        else:
            out = cmd_reuse(args.case, args.db)
    except CaseError as exc:
        return _fail(exc)
    _emit(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
