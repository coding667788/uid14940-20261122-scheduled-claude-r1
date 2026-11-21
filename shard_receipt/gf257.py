"""Linear algebra over GF(257) with reproducible evidence.

The solver never enumerates data vectors: it is plain Gauss-Jordan
elimination, polynomial in the number of rows and symbols.  Besides the
solution it produces machine-checkable evidence:

* inconsistency  -> the exact combination of input row IDs that yields
                    the contradictory equation ``0 = b (mod 257)``;
* under-rank     -> a non-zero null-space vector of the row matrix;
* unique         -> the unique solution.
"""

from . import FIELD_MODULUS

MOD = FIELD_MODULUS


def inv(a):
    """Multiplicative inverse in GF(257) (257 is prime)."""
    a %= MOD
    if a == 0:
        raise ZeroDivisionError("no inverse for 0 in GF(257)")
    return pow(a, MOD - 2, MOD)


def solve_system(equations, n):
    """Solve A x = b over GF(257).

    ``equations`` is a list of ``(row_id, coeffs, rhs)`` where ``coeffs``
    has length ``n``.  Returns a dict with one of these shapes:

    ``{"status": "unique", "rank": n, "solution": [...]}``
    ``{"status": "underdetermined", "rank": r, "null_vector": [...]}``
    ``{"status": "inconsistent", "rank": r,
       "conflict": {"combination": [{"row_id": .., "coeff": ..}, ...],
                    "rhs": b, "equation": "0 = b (mod 257)"}}``
    """
    m = len(equations)
    ids = [eid for (eid, _c, _b) in equations]
    # Augmented matrix [A | b] plus a tracking matrix T (starts as the
    # identity) recording which combination of the *original* rows each
    # current row represents.  All arithmetic mod 257.
    mat = [[c % MOD for c in e[1]] + [e[2] % MOD] for e in equations]
    trk = [[1 if i == j else 0 for j in range(m)] for i in range(m)]

    pivot_cols = []
    row = 0
    for col in range(n):
        if row >= m:
            break
        piv = None
        for r in range(row, m):
            if mat[r][col] != 0:
                piv = r
                break
        if piv is None:
            continue
        if piv != row:
            mat[row], mat[piv] = mat[piv], mat[row]
            trk[row], trk[piv] = trk[piv], trk[row]
        f = inv(mat[row][col])
        mat[row] = [(v * f) % MOD for v in mat[row]]
        trk[row] = [(v * f) % MOD for v in trk[row]]
        for r in range(m):
            if r != row and mat[r][col] != 0:
                g = mat[r][col]
                mat[r] = [(a - g * p) % MOD for a, p in zip(mat[r], mat[row])]
                trk[r] = [(a - g * p) % MOD for a, p in zip(trk[r], trk[row])]
        pivot_cols.append(col)
        row += 1

    rank = len(pivot_cols)

    # Inconsistency: a row 0 0 ... 0 | b with b != 0.
    for r in range(rank, m):
        if all(mat[r][c] == 0 for c in range(n)) and mat[r][n] != 0:
            combination = [
                {"row_id": ids[i], "coeff": trk[r][i]}
                for i in range(m) if trk[r][i] != 0
            ]
            return {
                "status": "inconsistent",
                "rank": rank,
                "conflict": {
                    "combination": combination,
                    "rhs": mat[r][n],
                    "equation": "0 = %d (mod %d)" % (mat[r][n], MOD),
                },
            }

    if rank < n:
        # Non-zero null-space vector: first free variable = 1, others 0,
        # pivot variables from the reduced rows.
        free = [c for c in range(n) if c not in pivot_cols]
        f0 = free[0]
        null_vector = [0] * n
        null_vector[f0] = 1
        for i, pc in enumerate(pivot_cols):
            null_vector[pc] = (-mat[i][f0]) % MOD
        return {
            "status": "underdetermined",
            "rank": rank,
            "null_vector": null_vector,
        }

    # Unique solution: Gauss-Jordan leaves the identity in pivot columns.
    solution = [0] * n
    for i, pc in enumerate(pivot_cols):
        solution[pc] = mat[i][n]
    return {"status": "unique", "rank": rank, "solution": solution}


def rank_of(rows, n):
    """Rank of a list of coefficient rows over GF(257)."""
    res = solve_system([("r%d" % i, list(r), 0) for i, r in enumerate(rows)], n)
    return res["rank"]


def mat_vec(rows, vec):
    """A @ vec over GF(257), one entry per row."""
    return [sum((a * x) for a, x in zip(r, vec)) % MOD for r in rows]
