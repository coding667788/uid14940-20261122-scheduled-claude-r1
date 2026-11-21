"""Canonical encoding and content commitment.

The content commitment of a data vector ``x = [x0, ..., x(n-1)]`` with
symbols in GF(257) is::

    commitment = "sha256:" + SHA256( enc(x0) || enc(x1) || ... )

where ``enc(xi)`` is the 2-byte little-endian unsigned encoding of the
least residue of ``xi mod 257`` (values 0..256 fit in 9 bits, so one
byte is not enough; two bytes little-endian keep the encoding fixed
width, order independent of locale, and trivially recomputable).

The same canonical byte string is what an independent verifier must
rebuild from a witness before hashing.
"""

import hashlib
import struct

from . import FIELD_MODULUS

MOD = FIELD_MODULUS


def canonical_bytes(symbols):
    """Canonical byte encoding of a data vector over GF(257)."""
    out = bytearray()
    for s in symbols:
        v = s % MOD
        out += struct.pack("<H", v)
    return bytes(out)


def commitment_of(symbols):
    """Content commitment digest of a data vector."""
    return "sha256:" + hashlib.sha256(canonical_bytes(symbols)).hexdigest()
