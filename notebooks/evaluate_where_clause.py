#!/usr/bin/env python3
"""
WHERE Clause Evaluator
======================
Reads hotels.csv, applies the WHERE clause defined below, then:
  - Computes Precision / Recall / F1 vs ground truth (id_i == id_j)
  - Shows sample FP records with suggested AND conditions to exclude them
  - Shows sample FN records with suggested conditions to capture them

Usage:
  Edit WHERE_CLAUSE below, then run:
    python3 evaluate_where_clause.py
"""

import re
import sys
import textwrap

import numpy as np
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION — edit these two values
# ─────────────────────────────────────────────────────────────────────────────

CSV_PATH = "/Users/nakul.patil/Downloads/hotels.csv"
FP_CSV_PATH = "/Users/nakul.patil/Downloads/hotels_fp_aggressive_mapping.csv"
FN_CSV_PATH = "/Users/nakul.patil/Downloads/hotels_fn_loose_mapping.csv"

# Ordered columns to place at the left of FP/FN CSVs (remaining cols appended after)
EXPORT_LEAD_COLS = [
    "id_i",
    "id_j",
    "providerName_i",
    "providerName_j",
    "providerHotelId_i",
    "providerHotelId_j",
    "name_i",
    "name_j",
    "normalized_name_i",
    "normalized_name_j",
    "type_i",
    "type_j",
    "geo_distance_km",
    "starRating_i",
    "starRating_j",
    "contact_address_line1_i",
    "contact_address_line1_j",
    "contact_address_postalCode_i",
    "contact_address_postalCode_j",
    "contact_address_city_name_i",
    "contact_address_city_name_j",
    "contact_address_state_name_i",
    "contact_address_state_name_j",
    "overall_pair_score",
    "name_score_containment",
    "normalized_name_score_containment",
    "name_score_jaccard",
    "normalized_name_score_jaccard",
    "name_score_lcs",
    "normalized_name_score_lcs",
    "name_score_levenshtein",
    "normalized_name_score_levenshtein",
    "name_score_sbert",
    "normalized_name_score_sbert",
    "average_name_score",
    "average_normalized_name_score",
    "address_line1_score",
    "postal_code_match",
    "country_match",
    "address_sbert_score",
    "phone_match_score",
    "email_match_score",
    "fax_match_score",
    "property_type_score",
    "name_unit_score",
    "address_unit_score",
    "supplier_score",
    "star_ratings_score",
]

# WHERE_CLAUSE = """
#     (name_score_containment >= 0.9500
#         AND normalized_name_score_containment >= 0.7000
#         AND name_score_levenshtein >= 0.4900
#         AND normalized_name_score_jaccard >= 0.1500
#         AND name_unit_score >= 0.9500)
#     OR (name_score_sbert >= 0.9500
#         AND name_unit_score >= 0.0500)
#     OR (average_name_score >= 0.5800
#         AND normalized_name_score_containment >= 0.9000)
#     OR (name_score_sbert >= 0.8500
#         AND normalized_name_score_containment >= 0.9000)
#     OR (name_score_sbert >= 0.8500
#         AND name_score_containment >= 0.9500)
#     OR (name_score_jaccard >= 0.8100
#         AND name_unit_score >= 0.0500)
# """

WHERE_CLAUSE = """
(
    (name_score_jaccard >= 0.9 OR name_score_lcs >= 0.9 OR name_score_levenshtein >= 0.95 OR name_score_sbert >= 0.9)
    OR
    (normalized_name_score_jaccard >= 0.95 OR normalized_name_score_lcs >= 0.95 OR normalized_name_score_levenshtein >= 0.95 OR normalized_name_score_sbert >= 0.95)
    OR
    (average_name_score >= 0.8 OR average_normalized_name_score >= 0.9)
    OR
    (normalized_name_score_containment >= 1 AND ((name_score_sbert >= 0.75 AND geo_distance_km <= 0.2) OR (name_score_sbert >= 0.7 AND geo_distance_km <= 0.1)))
    OR
    (name_score_containment >= 0.95 AND normalized_name_score_containment >= 0.95 AND star_ratings_score >= 0.8 AND geo_distance_km <= 0.2)
)
AND
    (
        address_line1_score >= 0.5 OR address_sbert_score >= 0.5 OR geo_distance_km <= 0.2
    )
AND
    (property_type_score >= 0.5)
AND
    (name_unit_score >= 0.5)
AND 
    (address_unit_score >= 0.5 OR (address_unit_score >= 0.25 AND geo_distance_km <= 0.1))
"""

# How many FP / FN samples to print
N_FP_SAMPLES = 0

N_FN_SAMPLES = 20

# How many FP / FN records to skip before printing.
# Increase by N_FP_SAMPLES / N_FN_SAMPLES each run to page through batches.
N_FP_SKIP = 5
N_FN_SKIP = 0

# Feature columns (used for suggestions). Script auto-detects these from the
# CSV, but you can hard-code them here if needed.
FEATURE_COLS = None  # None → auto-detect


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────


def parse_where(clause: str) -> str:
    """Convert SQL-style WHERE clause into a pandas df.eval() expression.

    Converts:
      AND  →  &
      OR   →  |
    Collapses newlines / extra whitespace so df.eval() can parse it.
    """
    # Collapse all whitespace (including newlines) into single spaces
    expr = " ".join(clause.split())
    # Replace AND/OR as whole words (case-insensitive)
    expr = re.sub(r"\bAND\b", "&", expr, flags=re.IGNORECASE)
    expr = re.sub(r"\bOR\b", "|", expr, flags=re.IGNORECASE)
    return expr


def apply_where(df: pd.DataFrame, clause: str) -> pd.Series:
    """Return a boolean Series for rows matching the WHERE clause."""
    expr = parse_where(clause)
    try:
        mask = df.eval(expr, engine="python")
    except Exception as exc:
        print(f"\nERROR: Could not evaluate WHERE clause.\n  {exc}")
        print("Translated expression:\n", textwrap.indent(expr, "  "))
        sys.exit(1)
    return mask.astype(bool)


def print_section(title: str):
    w = 90
    print(f"\n{'=' * w}")
    print(title)
    print("=" * w)


def print_subsection(title: str):
    print(f"\n{'-' * 70}")
    print(title)
    print("-" * 70)


def fmt_val(v) -> str:
    if isinstance(v, float):
        return f"{v:.4f}"
    return str(v)


def score_bar(v: float, width: int = 20) -> str:
    """ASCII score bar for a 0-1 float value."""
    filled = int(round(v * width))
    return f"[{'█' * filled}{'░' * (width - filled)}] {v:.4f}"


def print_row_horizontal(row: pd.Series, all_cols: list, feat_cols: list):
    """Print a record horizontally:
    1. Full row as a wide table — dynamic column widths, no truncation.
    2. Feature scores in a 2-column side-by-side layout.
    """
    # ── Full row table ──────────────────────────────────────────────────
    print("  All columns (horizontal):")

    def fmtcell(v):
        if isinstance(v, float):
            return f"{v:.4f}"
        return str(v)

    TERMINAL_W = 200  # target line width before wrapping to a new block
    GAP = 2  # spaces between columns

    cols = list(all_cols)
    # Compute each column's display width = max(header len, value len)
    col_widths = {c: max(len(c), len(fmtcell(row[c]))) for c in cols}

    # Group columns into rows that fit within TERMINAL_W
    blocks = []
    current_block = []
    current_w = 0
    for c in cols:
        needed = col_widths[c] + (GAP if current_block else 0)
        if current_block and current_w + needed > TERMINAL_W:
            blocks.append(current_block)
            current_block = [c]
            current_w = col_widths[c]
        else:
            current_block.append(c)
            current_w += needed
    if current_block:
        blocks.append(current_block)

    for block in blocks:
        sep = ("  " * GAP).join("-" * col_widths[c] for c in block)
        header = "  ".join(f"{c:<{col_widths[c]}}" for c in block)
        values = "  ".join(f"{fmtcell(row[c]):<{col_widths[c]}}" for c in block)
        print(f"    {sep}")
        print(f"    {header}")
        print(f"    {values}")
    print(f"    {'  '.join('-' * col_widths[c] for c in blocks[-1]) if blocks else ''}")
    print()

    # ── Feature scores 2-per-line ───────────────────────────────────────
    print("  Feature scores:")
    LABEL_W = 34  # feature name label width
    BAR_W = 12  # bar tick count → bar string = 20 chars

    pairs = [(feat_cols[k], feat_cols[k + 1]) for k in range(0, len(feat_cols) - 1, 2)]
    if len(feat_cols) % 2 == 1:
        pairs.append((feat_cols[-1], None))

    def short_bar(v: float) -> str:
        filled = int(round(v * BAR_W))
        return f"[{'█' * filled}{'░' * (BAR_W - filled)}] {v:.4f}"

    for f1, f2 in pairs:
        v1 = float(row[f1])
        b1 = short_bar(v1) if 0 <= v1 <= 1 else f"{v1:.6f}"
        left = f"{f1[:LABEL_W]:<{LABEL_W}}  {b1}"
        if f2 is not None:
            v2 = float(row[f2])
            b2 = short_bar(v2) if 0 <= v2 <= 1 else f"{v2:.6f}"
            right = f"{f2[:LABEL_W]:<{LABEL_W}}  {b2}"
            print(f"    {left}    |    {right}")
        else:
            print(f"    {left}")


# ─────────────────────────────────────────────────────────────────────────────
# SUGGESTION ENGINE
# ─────────────────────────────────────────────────────────────────────────────


def suggest_fix_for_fn(
    row: pd.Series,
    feat_cols: list,
    df: pd.DataFrame,
    y_true: np.ndarray,
    current_mask: np.ndarray,
) -> list[str]:
    """For a False Negative record, suggest conditions that would capture it.

    Strategy:
      For each feature where the FN record has a value > 0, check whether
      adding  `feature >= row_value`  (alone, or combined with the simplest
      existing OR branch) would capture it without adding too many extra FPs.
      Return the top suggestions sorted by extra-FP-count ascending.
    """
    suggestions = []

    for f in feat_cols:
        val = float(row[f])
        if val <= 0:
            continue
        t = round(val, 4)
        cand_mask = df[f].values >= t
        # Would adding OR (f >= t) to the current mask change things?
        new_mask = current_mask | cand_mask
        new_fp = int(np.sum(new_mask & ~y_true))
        new_tp = int(np.sum(new_mask & y_true))
        prev_fp = int(np.sum(current_mask & ~y_true))
        extra_fp = new_fp - prev_fp
        suggestions.append((extra_fp, new_tp, f, t))

    suggestions.sort(key=lambda x: (x[0], -x[1]))

    lines = []
    for extra_fp, new_tp, f, t in suggestions[:5]:
        lines.append(
            f"  Add OR ({f} >= {t:.4f})  →  +{new_tp - int(np.sum(current_mask & y_true))} TP, "
            f"+{extra_fp} extra FP"
        )
    return lines if lines else ["  No obvious single-feature fix found."]


def suggest_fix_for_fp(
    row: pd.Series, feat_cols: list, tp_df: pd.DataFrame
) -> list[str]:
    """For a False Positive record, suggest AND conditions to exclude it.

    Strategy:
      Compare the FP record's feature values against the TP distribution.
      Features where the FP record scores notably LOWER than the TP median
      are candidates for tighter AND thresholds.
    """
    # Only use float-compatible columns for statistics
    float_feat_cols = [f for f in feat_cols if tp_df[f].dtype.kind in ("f", "i", "u")]
    if not float_feat_cols:
        return ["  No numeric score features found."]

    tp_medians = tp_df[float_feat_cols].astype(float).median()
    tp_q25 = tp_df[float_feat_cols].astype(float).quantile(0.25)

    candidates = []
    for f in float_feat_cols:
        fp_val = float(row[f])
        med_val = float(tp_medians[f])
        q25_val = float(tp_q25[f])

        if med_val <= 0:
            continue

        # If FP value is below TP Q25, a tighter threshold would remove it
        if fp_val < q25_val:
            # Suggest threshold = Q25 of TPs (keeps ~75% of TPs, removes this FP)
            t_suggest = round(q25_val, 4)
            candidates.append((q25_val - fp_val, f, fp_val, t_suggest, "below TP Q25"))
        elif fp_val < med_val * 0.9:
            t_suggest = round(med_val * 0.9, 4)
            candidates.append(
                (med_val * 0.9 - fp_val, f, fp_val, t_suggest, "well below TP median")
            )

    candidates.sort(key=lambda x: -x[0])

    lines = []
    for gap, f, fp_val, t_sug, reason in candidates[:5]:
        lines.append(
            f"  Add AND ({f} >= {t_sug:.4f})  [FP value={fp_val:.4f}, {reason}]"
        )
    if not lines:
        lines.append(
            "  FP scores are indistinguishable from TPs on all features "
            "(may be an irreducible false positive)."
        )
    return lines


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────


def main():
    # ── Load data ──────────────────────────────────────────────────────────
    print_section("WHERE CLAUSE EVALUATOR")
    print(f"CSV : {CSV_PATH}")
    print(f"\nWHERE clause:\n{WHERE_CLAUSE}")

    try:
        df = pd.read_csv(CSV_PATH).fillna(0)
    except FileNotFoundError:
        print(f"\nERROR: File not found: {CSV_PATH}")
        sys.exit(1)

    N = len(df)
    y_true = (df["id_i"] == df["id_j"]).values.astype(bool)
    total_pos = int(y_true.sum())
    total_neg = N - total_pos

    print(f"\nRows      : {N:,}")
    print(f"Positives : {total_pos:,}  (id_i == id_j)")
    print(f"Negatives : {total_neg:,}  (id_i != id_j)")

    # Auto-detect feature columns: numeric columns whose values lie in [0, 1]
    # (i.e. score columns), excluding ID / name columns.
    global FEATURE_COLS
    exclude_patterns = {
        "id_i",
        "id_j",
        "name_i",
        "name_j",
        "hotel_id_i",
        "hotel_id_j",
        "index",
        "same_hotel",
    }
    if FEATURE_COLS is None:
        FEATURE_COLS = []
        for c in df.columns:
            if c in exclude_patterns:
                continue
            if not pd.api.types.is_numeric_dtype(df[c]):
                continue
            # Keep only columns whose values are all in [0, 1] (score columns)
            col_vals = df[c].dropna()
            if len(col_vals) == 0:
                continue
            if col_vals.min() >= 0 and col_vals.max() <= 1.0:
                FEATURE_COLS.append(c)
    feat_cols = [f for f in FEATURE_COLS if f in df.columns]

    # ── Apply WHERE clause ─────────────────────────────────────────────────
    mask = apply_where(df, WHERE_CLAUSE)
    matched = int(mask.sum())

    # ── Compute TP / FP / FN ──────────────────────────────────────────────
    tp_mask = mask & y_true
    fp_mask = mask & ~y_true
    fn_mask = ~mask & y_true

    TP = int(tp_mask.sum())
    FP = int(fp_mask.sum())
    FN = int(fn_mask.sum())
    TN = N - TP - FP - FN

    precision = TP / (TP + FP) if (TP + FP) > 0 else 0.0
    recall = TP / (TP + FN) if (TP + FN) > 0 else 0.0
    f1 = (
        2 * precision * recall / (precision + recall)
        if (precision + recall) > 0
        else 0.0
    )

    # ── Summary ───────────────────────────────────────────────────────────
    print_section("EVALUATION RESULTS")
    print(f"  WHERE clause matched  : {matched:,} rows")
    print()
    print(f"  True  Positives (TP)  : {TP:,}   ← matched AND id_i == id_j  ✓")
    print(f"  False Positives (FP)  : {FP:,}   ← matched BUT id_i != id_j  ✗")
    print(f"  False Negatives (FN)  : {FN:,}   ← not matched, id_i == id_j  ✗")
    print(f"  True  Negatives (TN)  : {TN:,}")
    print()
    print(f"  Precision : {precision:.4f}  ({TP}/{TP + FP})")
    print(f"  Recall    : {recall:.4f}  ({TP}/{TP + FN})")
    print(f"  F1-Score  : {f1:.4f}")

    # Irreducible FPs (score 1.0 on all features)
    if feat_cols:
        irr_mask_neg = ~y_true.copy()
        for f in feat_cols:
            tp_max = float(df.loc[y_true, f].max()) if y_true.any() else 0
            irr_mask_neg &= df[f].values >= tp_max
        irr_fp = int(irr_mask_neg.sum())
        if irr_fp > 0:
            max_p = total_pos / (total_pos + irr_fp)
            print(
                f"\n  NOTE: {irr_fp} irreducible FP(s) detected (perfect scores on "
                f"all features)."
            )
            print(f"        Max achievable precision = {max_p:.4f}")

    # ── FALSE POSITIVES ───────────────────────────────────────────────────
    print_section(
        f"FALSE POSITIVES (FP={FP}) — showing {N_FP_SAMPLES} starting from #{N_FP_SKIP + 1}"
    )

    tp_df = df[tp_mask]
    fp_rows = df[fp_mask].reset_index(drop=True)

    if FP == 0:
        print("  No false positives — perfect precision!")
    else:
        batch_fp = fp_rows.iloc[N_FP_SKIP : N_FP_SKIP + N_FP_SAMPLES]
        if len(batch_fp) == 0:
            print(f"  No records at offset {N_FP_SKIP} (total FP={FP}).")
        _display_cols = [c for c in df.columns if c not in ("uid_i", "uid_j")]
        for i, (_, row) in enumerate(batch_fp.iterrows(), N_FP_SKIP + 1):
            print_subsection(f"FP #{i}  (id_i={row['id_i']}, id_j={row['id_j']})")

            print_row_horizontal(row, _display_cols, feat_cols)

            print()
            print("  Suggested fix (add AND condition to exclude this FP):")
            for line in suggest_fix_for_fp(row, feat_cols, tp_df):
                print(line)

    # ── FALSE NEGATIVES ───────────────────────────────────────────────────
    print_section(
        f"FALSE NEGATIVES (FN={FN}) — showing {N_FN_SAMPLES} starting from #{N_FN_SKIP + 1}"
    )

    fn_rows = df[fn_mask].reset_index(drop=True)

    if FN == 0:
        print("  No false negatives — perfect recall!")
    else:
        batch_fn = fn_rows.iloc[N_FN_SKIP : N_FN_SKIP + N_FN_SAMPLES]
        if len(batch_fn) == 0:
            print(f"  No records at offset {N_FN_SKIP} (total FN={FN}).")
        _display_cols = [c for c in df.columns if c not in ("uid_i", "uid_j")]
        for i, (_, row) in enumerate(batch_fn.iterrows(), N_FN_SKIP + 1):
            print_subsection(f"FN #{i}  (id_i={row['id_i']}, id_j={row['id_j']})")

            print_row_horizontal(row, _display_cols, feat_cols)

            print()
            print("  Suggested fix (add OR branch to capture this FN):")
            for line in suggest_fix_for_fn(row, feat_cols, df, y_true, mask.values):
                print(line)

    # ── Export FP / FN CSVs ───────────────────────────────────────────────
    def export_cases(cases_df: pd.DataFrame, path: str, label: str):
        # Build column order: lead cols that exist, then remaining cols
        lead = [c for c in EXPORT_LEAD_COLS if c in cases_df.columns]
        rest = [c for c in cases_df.columns if c not in set(lead)]
        ordered = cases_df[lead + rest]
        ordered.to_csv(path, index=False)
        print(f"  ✓ {label}: {len(ordered):,} rows → {path}")

    print_section("EXPORTING FP / FN CSVs")
    export_cases(df[fp_mask].reset_index(drop=True), FP_CSV_PATH, f"FP ({FP})")
    export_cases(df[fn_mask].reset_index(drop=True), FN_CSV_PATH, f"FN ({FN})")

    # ── Footer ────────────────────────────────────────────────────────────
    print_section("SUMMARY")
    print(f"  Precision : {precision:.4f}")
    print(f"  Recall    : {recall:.4f}")
    print(f"  F1-Score  : {f1:.4f}")
    print(f"  TP={TP}  FP={FP}  FN={FN}  TN={TN}")
    print("=" * 90)


if __name__ == "__main__":
    main()
