"""Case file loading and structural validation.

Every structural error is raised as :class:`CaseError` carrying a JSON
path (e.g. ``manifests[1].deadline``) so the offending spot is easy to
locate.
"""

import json

from . import FIELD_MODULUS

MOD = FIELD_MODULUS


class CaseError(Exception):
    def __init__(self, path, message):
        super().__init__("%s: %s" % (path, message))
        self.path = path
        self.message = message

    def to_json(self):
        return {"error": {"path": self.path, "message": self.message}}


def _req(obj, key, path):
    if not isinstance(obj, dict) or key not in obj:
        raise CaseError(path, "missing required key %r" % key)
    return obj[key]


def _check_int(v, path):
    if not isinstance(v, int) or isinstance(v, bool):
        raise CaseError(path, "expected integer, got %r" % (v,))
    return v


def _check_str(v, path):
    if not isinstance(v, str) or not v:
        raise CaseError(path, "expected non-empty string, got %r" % (v,))
    return v


def _check_bool(v, path):
    if not isinstance(v, bool):
        raise CaseError(path, "expected boolean, got %r" % (v,))
    return v


def validate_manifest(m, path, n_default):
    out = {
        "manifest_id": _check_str(_req(m, "manifest_id", path), path + ".manifest_id"),
        "doc_id": _check_str(_req(m, "doc_id", path), path + ".doc_id"),
        "generation": _check_int(_req(m, "generation", path), path + ".generation"),
        "source_domains": _req(m, "source_domains", path + ".source_domains"),
        "recipient_domain": _check_str(_req(m, "recipient_domain", path),
                                       path + ".recipient_domain"),
        "deadline": _check_int(_req(m, "deadline", path), path + ".deadline"),
        "commitment": _check_str(_req(m, "commitment", path), path + ".commitment"),
    }
    if not isinstance(out["source_domains"], list) or not out["source_domains"] \
            or not all(isinstance(d, str) and d for d in out["source_domains"]):
        raise CaseError(path + ".source_domains",
                        "expected non-empty list of domain names")
    if len(set(out["source_domains"])) != len(out["source_domains"]):
        raise CaseError(path + ".source_domains", "duplicate domain names")
    if not out["commitment"].startswith("sha256:") or len(out["commitment"]) != 7 + 64:
        raise CaseError(path + ".commitment",
                        "expected 'sha256:' followed by 64 hex chars")
    n = m.get("data_symbols", n_default)
    out["data_symbols"] = _check_int(n, path + ".data_symbols")
    if out["data_symbols"] <= 0:
        raise CaseError(path + ".data_symbols", "must be positive")
    segs = m.get("segments")
    if segs is not None:
        if not isinstance(segs, list):
            raise CaseError(path + ".segments", "expected list")
        checked = []
        covered = 0
        for i, s in enumerate(segs):
            sp = "%s.segments[%d]" % (path, i)
            name = _check_str(_req(s, "name", sp), sp + ".name")
            start = _check_int(_req(s, "start", sp), sp + ".start")
            length = _check_int(_req(s, "length", sp), sp + ".length")
            if start != covered or length <= 0:
                raise CaseError(sp, "segments must tile [0, data_symbols) in order")
            covered += length
            checked.append({"name": name, "start": start, "length": length})
        if covered != out["data_symbols"]:
            raise CaseError(path + ".segments",
                            "segments must cover exactly data_symbols symbols")
        out["segments"] = checked
    return out


def validate_receipt(r, path):
    out = {
        "receipt_id": _check_str(_req(r, "receipt_id", path), path + ".receipt_id"),
        "time": _check_int(_req(r, "time", path), path + ".time"),
        "link": _check_str(_req(r, "link", path), path + ".link"),
        "source_instance": _check_str(_req(r, "source_instance", path),
                                      path + ".source_instance"),
        "source_domain": _check_str(_req(r, "source_domain", path),
                                    path + ".source_domain"),
        "doc_id": _check_str(_req(r, "doc_id", path), path + ".doc_id"),
        "generation": _check_int(_req(r, "generation", path), path + ".generation"),
        "shard_index": _check_int(_req(r, "shard_index", path), path + ".shard_index"),
        "trusted": _check_bool(_req(r, "trusted", path), path + ".trusted"),
        "row": _req(r, "row", path + ".row"),
        "payload": _check_int(_req(r, "payload", path), path + ".payload"),
    }
    if not isinstance(out["row"], list) or not out["row"] \
            or not all(isinstance(x, int) and not isinstance(x, bool)
                       for x in out["row"]):
        raise CaseError(path + ".row", "expected non-empty list of integers")
    return out


def load_case(path_str):
    """Load and validate a CASE.json file.  Returns the case dict."""
    try:
        with open(path_str, "r", encoding="utf-8") as fh:
            case = json.load(fh)
    except json.JSONDecodeError as exc:
        raise CaseError("$", "invalid JSON: %s" % exc)
    if not isinstance(case, dict):
        raise CaseError("$", "top level must be an object")
    case_id = _check_str(_req(case, "case_id", "$"), "$.case_id")
    n_default = case.get("data_symbols", 12)
    _check_int(n_default, "$.data_symbols")
    manifests = _req(case, "manifests", "$")
    if not isinstance(manifests, list) or not manifests:
        raise CaseError("$.manifests", "expected non-empty list")
    out_manifests = []
    seen_ids = set()
    for i, m in enumerate(manifests):
        vm = validate_manifest(m, "manifests[%d]" % i, n_default)
        if vm["manifest_id"] in seen_ids:
            raise CaseError("manifests[%d].manifest_id" % i, "duplicate manifest_id")
        seen_ids.add(vm["manifest_id"])
        out_manifests.append(vm)
    events = _req(case, "events", "$")
    if not isinstance(events, list):
        raise CaseError("$.events", "expected list")
    out_events = []
    seen_rids = set()
    for i, e in enumerate(events):
        ep = "events[%d]" % i
        etype = _req(e, "type", ep)
        if etype != "receipt":
            raise CaseError(ep + ".type", "unknown event type %r" % (etype,))
        vr = validate_receipt(e, ep)
        if vr["receipt_id"] in seen_rids:
            raise CaseError(ep + ".receipt_id", "duplicate receipt_id")
        seen_rids.add(vr["receipt_id"])
        out_events.append(vr)
    # Row lengths must match the manifest they target.
    n_by_doc = {}
    for m in out_manifests:
        n_by_doc.setdefault((m["doc_id"], m["generation"]), set()).add(m["data_symbols"])
    for i, r in enumerate(out_events):
        key = (r["doc_id"], r["generation"])
        if key in n_by_doc:
            ns = n_by_doc[key]
            if len(ns) == 1 and len(r["row"]) != next(iter(ns)):
                raise CaseError("events[%d].row" % i,
                                "row length %d does not match manifest data_symbols"
                                % len(r["row"]))
    return {"case_id": case_id, "data_symbols": n_default,
            "manifests": out_manifests, "events": out_events,
            "raw": case}
