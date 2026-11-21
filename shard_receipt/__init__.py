"""Offline shard receipt receiver and verifier.

Simulates a read-only cross-site inbox that collects authenticated shards
of a document over independent links and decides, per manifest, whether
the document can be released or must be held/quarantined.

All arithmetic is over GF(257).  No network, no external dependencies.
"""

__version__ = "1.0.0"

FIELD_MODULUS = 257
