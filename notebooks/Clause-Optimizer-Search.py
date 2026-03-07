#!/usr/bin/env python3
"""
WHERE Clause Optimizer
======================
Starts from a user-provided WHERE clause (your best guess), evaluates it,
then refines it by:

  1. Parsing the clause into structured OR-branches, each branch being a list
     of AND-conditions  (feature OP threshold).
  2. Threshold tuning  — sweeps each threshold ± a window to improve F1.
  3. Condition pruning — removes AND-conditions that hurt or don't help.
  4. Condition addition — for each OR-branch, tries adding one extra AND
     condition (from any feature) to reduce FPs without hurting precision too
     much, and adds OR-branches for uncaptured positives.
  5. Branch addition   — greedily adds new OR-branches from the full feature
     space to recover missed positives (FNs) while staying within FP budget.
  6. Branch pruning    — removes any branch that became redundant.
  7. Fine-tune pass    — one final sweep of all thresholds at 0.01 resolution.

Usage:
  1. Set CSV_PATH to your hotels.csv
  2. Edit WHERE_CLAUSE below (or paste your current best clause)
  3. Tune MIN_PRECISION if needed (default 0.0 → maximize F1 freely)
  4. Run:  python3 Clause-Optimizer-Search.py
"""

import re
import sys
import time
import textwrap
from itertools import combinations
from copy import deepcopy

import numpy as np
import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

CSV_PATH = '/Users/nakul.patil/Downloads/hotels.csv'

# Paste your starting WHERE clause here (SQL-style AND / OR, >= / <=)
WHERE_CLAUSE = """
(
    (name_score_containment >= 0.95 OR name_score_jaccard >= 0.9 OR name_score_lcs >= 0.9 OR name_score_levenshtein >= 0.9 OR name_score_sbert >= 0.9)
    OR
    (normalized_name_score_jaccard >= 0.95 OR normalized_name_score_lcs >= 0.95 OR normalized_name_score_levenshtein >= 0.95 OR normalized_name_score_sbert >= 0.95)
    OR
    (average_name_score >= 0.8 OR average_normalized_name_score >= 0.9)
    OR
    (normalized_name_score_containment >= 1 AND name_score_sbert >= 0.75 AND geo_distance_km <= 0.15)
)
AND
    (
        address_line1_score >= 0.5 OR address_sbert_score >= 0.5 OR geo_distance_km <= 0.15
    )
AND
    (property_type_score >= 0.8)
AND
    (name_unit_score >= 0.5 AND address_unit_score >= 0.5)
"""

# Minimum precision to maintain during optimization (0.0 = optimize F1 freely)
MIN_PRECISION = 0.0

# Coarse sweep window around each threshold during tuning (± this value)
TUNE_WINDOW = 0.20
# Fine sweep window for final pass
FINE_WINDOW = 0.10

# Max new AND-conditions to try adding per branch during condition addition
MAX_NEW_AND_PER_BRANCH = 2

# How many new OR-branches to add in branch-addition phase
MAX_NEW_BRANCHES = 20

# Threshold grid steps
COARSE_STEP = 0.02
FINE_STEP = 0.01

# ─────────────────────────────────────────────────────────────────────────────
# WHERE CLAUSE PARSER
# ─────────────────────────────────────────────────────────────────────────────


def _tokenize(expr: str) -> list:
    """Tokenize a SQL WHERE clause into a flat list of tokens."""
    # Normalize whitespace
    expr = ' '.join(expr.split())
    # Insert spaces around parens
    expr = re.sub(r'\(', ' ( ', expr)
    expr = re.sub(r'\)', ' ) ', expr)
    tokens = expr.split()
    return tokens


def _parse_expr(tokens: list, pos: int):
    """Recursive descent parser. Returns (node, next_pos).
    Node types:
      ('AND', [children])
      ('OR',  [children])
      ('COND', feature, op, threshold)
    """
    node, pos = _parse_or(tokens, pos)
    return node, pos


def _parse_or(tokens, pos):
    left, pos = _parse_and(tokens, pos)
    children = [left]
    while pos < len(tokens) and tokens[pos].upper() == 'OR':
        pos += 1
        right, pos = _parse_and(tokens, pos)
        children.append(right)
    if len(children) == 1:
        return children[0], pos
    return ('OR', children), pos


def _parse_and(tokens, pos):
    left, pos = _parse_atom(tokens, pos)
    children = [left]
    while pos < len(tokens) and tokens[pos].upper() == 'AND':
        pos += 1
        right, pos = _parse_atom(tokens, pos)
        children.append(right)
    if len(children) == 1:
        return children[0], pos
    return ('AND', children), pos


def _parse_atom(tokens, pos):
    if tokens[pos] == '(':
        pos += 1  # consume '('
        node, pos = _parse_or(tokens, pos)
        if pos < len(tokens) and tokens[pos] == ')':
            pos += 1  # consume ')'
        return node, pos
    # Should be  feature OP value
    feature = tokens[pos]
    pos += 1
    op = tokens[pos]
    pos += 1
    value = float(tokens[pos])
    pos += 1
    return ('COND', feature, op, value), pos


def parse_where_clause(clause: str):
    """Parse a SQL WHERE clause string into an AST."""
    tokens = _tokenize(clause)
    node, _ = _parse_expr(tokens, 0)
    return node


# ─────────────────────────────────────────────────────────────────────────────
# AST → Structured representation
# ─────────────────────────────────────────────────────────────────────────────
# We represent the top-level clause as a list of OR-branches.
# Each OR-branch is a list of AND-conditions.
# An AND-condition is a dict: {'feature': str, 'op': str, 'threshold': float}
#
# The outer structure is always:  UNION of branches  (OR of AND-clauses).
# If the top-level is a single AND (no top-level OR), we wrap it.

def ast_to_branches(node):
    """Convert AST to list-of-lists: [[cond, ...], ...]  (outer OR, inner AND)."""
    if node[0] == 'COND':
        return [[{'feature': node[1], 'op': node[2], 'threshold': node[3]}]]

    if node[0] == 'AND':
        # Flatten AND children: cross-product of sub-branches
        # For top-level AND this means a single branch with all conditions
        # We collect leaf CONDs; nested ORs we expand
        return _and_to_branches(node[1])

    if node[0] == 'OR':
        branches = []
        for child in node[1]:
            branches.extend(ast_to_branches(child))
        return branches

    return []


def _and_to_branches(children):
    """
    Convert a list of AND-children AST nodes into a list of branches.
    If all children are CONDs or AND nodes → single branch.
    If some children are OR nodes → cross-product expansion.
    """
    # Collect each child as a list of branches, then cross-product
    child_branch_lists = []
    for child in children:
        child_branch_lists.append(ast_to_branches(child))

    # Cross-product: combine one branch from each child
    result = [[]]
    for branch_list in child_branch_lists:
        new_result = []
        for existing in result:
            for branch in branch_list:
                new_result.append(existing + branch)
        result = new_result
    return result


def branches_to_str(branches, indent=4) -> str:
    """Format branches back into a readable WHERE clause string."""
    pad = ' ' * indent
    or_parts = []
    for branch in branches:
        and_parts = []
        for cond in branch:
            and_parts.append(
                f"{cond['feature']} {cond['op']} {cond['threshold']:.4f}")
        or_parts.append(
            f"\n{pad}  (" + f"\n{pad}    AND ".join(and_parts) + f"\n{pad}  )")
    where = f"\n{pad}OR".join(or_parts)
    return where


# ─────────────────────────────────────────────────────────────────────────────
# MASK EVALUATION
# ─────────────────────────────────────────────────────────────────────────────

def branches_to_mask(branches, col, N):
    """Evaluate branches (list-of-list-of-cond) → boolean numpy mask."""
    combined = np.zeros(N, dtype=bool)
    for branch in branches:
        mask = np.ones(N, dtype=bool)
        for cond in branch:
            feat = cond['feature']
            op = cond['op']
            t = cond['threshold']
            if feat not in col:
                continue
            if op in ('>=', '>'):
                mask &= col[feat] >= t
            elif op in ('<=', '<'):
                mask &= col[feat] <= t
        combined |= mask
    return combined


def evaluate(y_true, mask, total_pos):
    tp = int(np.sum(y_true & mask))
    captured = int(np.sum(mask))
    fp = captured - tp
    fn = total_pos - tp
    p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    r = tp / total_pos if total_pos > 0 else 0.0
    f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0.0
    return p, r, f1, tp, fp, fn


def score(y_true, branches, col, N, total_pos, min_precision=0.0):
    """Return (f1, p, r, tp, fp, fn) for a set of branches."""
    mask = branches_to_mask(branches, col, N)
    p, r, f1, tp, fp, fn = evaluate(y_true, mask, total_pos)
    if min_precision > 0 and p < min_precision:
        return -1.0, p, r, tp, fp, fn  # infeasible
    return f1, p, r, tp, fp, fn


# ─────────────────────────────────────────────────────────────────────────────
# MAIN OPTIMIZER
# ─────────────────────────────────────────────────────────────────────────────

def optimize_where_clause(csv_path: str, where_clause: str,
                          min_precision: float = 0.0):
    t0 = time.time()

    print("=" * 90)
    print("WHERE CLAUSE OPTIMIZER")
    print("=" * 90)
    print(f"CSV : {csv_path}")
    print(f"\nStarting WHERE clause:\n{where_clause}")

    # ── Load data ──────────────────────────────────────────────────────────
    try:
        df = pd.read_csv(csv_path).fillna(0)
    except FileNotFoundError:
        print(f"\nERROR: File not found: {csv_path}")
        sys.exit(1)

    N = len(df)
    y_true = (df['id_i'] == df['id_j']).values.astype(bool)
    total_pos = int(y_true.sum())
    total_neg = N - total_pos

    print(
        f"\nRows: {N:,} | Positives: {total_pos:,} | Negatives: {total_neg:,}")

    # ── Auto-detect feature columns ────────────────────────────────────────
    exclude = {'id_i', 'id_j', 'name_i', 'name_j', 'hotel_id_i', 'hotel_id_j',
               'index', 'same_hotel', 'uid_i', 'uid_j'}
    all_features = []
    for c in df.columns:
        if c in exclude:
            continue
        if not pd.api.types.is_numeric_dtype(df[c]):
            continue
        all_features.append(c)

    # Pre-compute column arrays
    col = {f: df[f].values.astype(float) for f in all_features}

    # ── Parse initial clause ───────────────────────────────────────────────
    print("\n" + "─" * 70)
    print("Parsing initial WHERE clause...")
    try:
        ast = parse_where_clause(where_clause)
        branches = ast_to_branches(ast)
    except Exception as e:
        print(f"ERROR parsing WHERE clause: {e}")
        sys.exit(1)

    # Sanitize: keep only conditions with known features
    branches = [
        [c for c in branch if c['feature'] in col]
        for branch in branches
        if any(c['feature'] in col for c in branch)
    ]

    print(f"  → {len(branches)} OR-branches parsed")
    for i, branch in enumerate(branches):
        conds = ", ".join(f"{c['feature']} {c['op']} {c['threshold']}"
                          for c in branch)
        print(f"    Branch {i+1}: {conds}")

    # Baseline evaluation
    f1_init, p_init, r_init, tp_init, fp_init, fn_init = score(
        y_true, branches, col, N, total_pos, min_precision)
    print(f"\nBaseline: P={p_init:.4f}  R={r_init:.4f}  F1={f1_init:.4f}"
          f"  TP={tp_init}  FP={fp_init}  FN={fn_init}")

    best_branches = deepcopy(branches)
    best_f1 = f1_init

    # ── Irreducible FP detection ───────────────────────────────────────────
    irr_mask = ~y_true.copy()
    for f in all_features:
        arr = col[f]
        if y_true.any():
            tp_max = float(arr[y_true].max())
            irr_mask &= (arr >= tp_max)
    IRR_FP = int(irr_mask.sum())
    if IRR_FP > 0:
        max_p = total_pos / (total_pos + IRR_FP)
        print(f"\nNote: {IRR_FP} irreducible FPs detected → max achievable "
              f"precision = {max_p:.4f}")

    # ── Pre-compute threshold grids ────────────────────────────────────────
    coarse_t = np.round(np.arange(COARSE_STEP, 1.0 +
                        COARSE_STEP / 2, COARSE_STEP), 4)
    fine_t = np.round(np.arange(FINE_STEP,   1.0 +
                      FINE_STEP / 2,   FINE_STEP),   4)

    def _sweep_threshold(branches, bi, ci, grid, window):
        """Sweep threshold for condition ci in branch bi. Returns best_f1, best_t."""
        orig_t = branches[bi][ci]['threshold']
        best_t = orig_t
        bf = score(y_true, branches, col, N, total_pos, min_precision)[0]
        for t in grid:
            if abs(t - orig_t) > window:
                continue
            branches[bi][ci]['threshold'] = float(t)
            f = score(y_true, branches, col, N, total_pos, min_precision)[0]
            if f > bf:
                bf = f
                best_t = float(t)
        branches[bi][ci]['threshold'] = best_t
        return bf, best_t

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 1: THRESHOLD TUNING (coarse)
    # ══════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("PHASE 1: Threshold tuning (coarse, ±{:.2f}, step={})".format(
        TUNE_WINDOW, COARSE_STEP))
    print("=" * 90)

    improved = True
    passes = 0
    while improved and passes < 10:
        improved = False
        passes += 1
        for bi in range(len(best_branches)):
            for ci in range(len(best_branches[bi])):
                prev_t = best_branches[bi][ci]['threshold']
                new_f1, new_t = _sweep_threshold(
                    best_branches, bi, ci, coarse_t, TUNE_WINDOW)
                if new_t != prev_t:
                    improved = True
                    if new_f1 > best_f1:
                        best_f1 = new_f1

    f1_p1, p_p1, r_p1, tp_p1, fp_p1, fn_p1 = score(
        y_true, best_branches, col, N, total_pos, min_precision)
    if f1_p1 > f1_init:
        print(f"  Improved: P={p_p1:.4f}  R={r_p1:.4f}  F1={f1_p1:.4f}"
              f"  TP={tp_p1}  FP={fp_p1}")
    else:
        print(f"  No improvement from threshold tuning.")
        f1_p1, p_p1, r_p1, tp_p1, fp_p1, fn_p1 = f1_init, p_init, r_init, tp_init, fp_init, fn_init

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 2: CONDITION PRUNING
    # ══════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("PHASE 2: Condition pruning (remove redundant AND conditions)")
    print("=" * 90)

    pruned = True
    total_pruned = 0
    while pruned:
        pruned = False
        for bi in range(len(best_branches)):
            branch = best_branches[bi]
            if len(branch) <= 1:
                continue
            for ci in range(len(branch) - 1, -1, -1):
                trial = deepcopy(best_branches)
                trial[bi] = [c for j, c in enumerate(trial[bi]) if j != ci]
                f, p, r, tp, fp, fn = score(
                    y_true, trial, col, N, total_pos, min_precision)
                if f >= best_f1:
                    removed_cond = best_branches[bi][ci]
                    print(f"  Branch {bi+1}: removed condition "
                          f"'{removed_cond['feature']} {removed_cond['op']} "
                          f"{removed_cond['threshold']:.4f}' "
                          f"(F1={f:.4f} >= {best_f1:.4f})")
                    best_branches = trial
                    best_f1 = f
                    pruned = True
                    total_pruned += 1
                    break
            if pruned:
                break

    if total_pruned == 0:
        print("  No conditions pruned.")

    # Prune empty branches
    best_branches = [b for b in best_branches if b]

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 3: CONDITION ADDITION (tighten branches with extra AND)
    # ══════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("PHASE 3: Condition addition (add AND conditions to existing branches)")
    print("=" * 90)

    for bi in range(len(best_branches)):
        existing_feats = {c['feature'] for c in best_branches[bi]}
        added = 0

        for feat in all_features:
            if feat in existing_feats:
                continue
            if added >= MAX_NEW_AND_PER_BRANCH:
                break

            # Determine operator: if feature contains 'distance' → <=, else >=
            op = '<=' if 'distance' in feat.lower() else '>='
            arr = col[feat]

            for t in coarse_t:
                if op == '<=':
                    t_use = float(
                        round(arr.max() * t + arr.min() * (1 - t), 4))
                else:
                    t_use = float(t)

                trial = deepcopy(best_branches)
                trial[bi] = trial[bi] + \
                    [{'feature': feat, 'op': op, 'threshold': t_use}]
                f, p, r, tp, fp, fn = score(
                    y_true, trial, col, N, total_pos, min_precision)
                if f > best_f1:
                    best_branches = trial
                    best_f1 = f
                    existing_feats.add(feat)
                    added += 1
                    print(f"  Branch {bi+1}: added '{feat} {op} {t_use:.4f}' "
                          f"(F1 {score(y_true, best_branches, col, N, total_pos, min_precision)[0]:.4f})")
                    break

    f1_p3, p_p3, r_p3, tp_p3, fp_p3, fn_p3 = score(
        y_true, best_branches, col, N, total_pos, min_precision)
    print(f"\n  After phase 3: P={p_p3:.4f}  R={r_p3:.4f}  F1={f1_p3:.4f}"
          f"  TP={tp_p3}  FP={fp_p3}")

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 4: BRANCH ADDITION (new OR-branches for uncaptured positives)
    # ══════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("PHASE 4: Branch addition (new OR-branches for uncaptured FNs)")
    print("=" * 90)

    current_mask = branches_to_mask(best_branches, col, N)
    uncaptured = y_true & ~current_mask
    unc_count = int(uncaptured.sum())
    print(f"  Uncaptured positives: {unc_count}")

    _, _, _, _, cur_fp, _ = score(
        y_true, best_branches, col, N, total_pos, min_precision)
    fp_budget = max(IRR_FP, cur_fp)

    branches_added = 0
    for step in range(MAX_NEW_BRANCHES):
        current_mask = branches_to_mask(best_branches, col, N)
        uncaptured = y_true & ~current_mask
        if not uncaptured.any():
            print("  All positives covered!")
            break

        best_new_branch = None
        best_gain = 0

        # Single-feature new branches
        for feat in all_features:
            arr = col[feat]
            op = '<=' if 'distance' in feat.lower() else '>='
            for t in coarse_t:
                if op == '>=':
                    cand_mask = arr >= t
                else:
                    cand_mask = arr <= (arr.max() * t)
                trial = current_mask | cand_mask
                trial_fp = int(np.sum(trial & ~y_true))
                if trial_fp > fp_budget:
                    continue
                new_tp = int(np.sum(uncaptured & cand_mask))
                if new_tp > best_gain:
                    best_gain = new_tp
                    best_new_branch = [{'feature': feat, 'op': op,
                                        'threshold': float(t) if op == '>=' else float(arr.max() * t)}]

        # Two-feature new branches (pairs of high-value features)
        high_val_feats = [f for f in all_features
                          if col[f][y_true].mean() > 0.5] if y_true.any() else []
        for f1, f2 in combinations(high_val_feats[:12], 2):
            op1 = '<=' if 'distance' in f1.lower() else '>='
            op2 = '<=' if 'distance' in f2.lower() else '>='
            for t1 in coarse_t[::2]:  # subsample for speed
                m1 = col[f1] >= t1 if op1 == '>=' else col[f1] <= col[f1].max() * t1
                for t2 in coarse_t[::2]:
                    m2 = col[f2] >= t2 if op2 == '>=' else col[f2] <= col[f2].max() * t2
                    cand_mask = m1 & m2
                    trial = current_mask | cand_mask
                    trial_fp = int(np.sum(trial & ~y_true))
                    if trial_fp > fp_budget:
                        continue
                    new_tp = int(np.sum(uncaptured & cand_mask))
                    if new_tp > best_gain:
                        best_gain = new_tp
                        best_new_branch = [
                            {'feature': f1, 'op': op1, 'threshold': float(
                                t1) if op1 == '>=' else float(col[f1].max() * t1)},
                            {'feature': f2, 'op': op2, 'threshold': float(
                                t2) if op2 == '>=' else float(col[f2].max() * t2)},
                        ]

        if best_new_branch is None or best_gain == 0:
            print(f"  No more beneficial branches can be added within FP budget.")
            break

        trial_branches = best_branches + [best_new_branch]
        f_trial, p_trial, r_trial, tp_trial, fp_trial, fn_trial = score(
            y_true, trial_branches, col, N, total_pos, min_precision)

        if f_trial >= best_f1:
            best_branches = trial_branches
            best_f1 = f_trial
            fp_budget = max(fp_budget, fp_trial)
            branches_added += 1
            conds_str = " AND ".join(
                f"{c['feature']} {c['op']} {c['threshold']:.4f}"
                for c in best_new_branch)
            print(f"  Step {step+1}: added branch ({conds_str})  "
                  f"+{best_gain}TP → P={p_trial:.4f}  R={r_trial:.4f}  "
                  f"F1={f_trial:.4f}")
        else:
            print(f"  Step {step+1}: best candidate +{best_gain}TP but "
                  f"F1 would drop ({best_f1:.4f}→{f_trial:.4f}), stopping.")
            break

    if branches_added == 0:
        print("  No new branches added.")

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 5: BRANCH PRUNING (remove redundant OR-branches)
    # ══════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("PHASE 5: Branch pruning (remove redundant OR-branches)")
    print("=" * 90)

    pruned_branches = True
    total_branch_pruned = 0
    while pruned_branches and len(best_branches) > 1:
        pruned_branches = False
        for bi in range(len(best_branches) - 1, -1, -1):
            trial = [b for j, b in enumerate(best_branches) if j != bi]
            f, p, r, tp, fp, fn = score(
                y_true, trial, col, N, total_pos, min_precision)
            if f >= best_f1:
                conds_str = " AND ".join(
                    f"{c['feature']} {c['op']} {c['threshold']:.4f}"
                    for c in best_branches[bi])
                print(f"  Removed redundant branch {bi+1}: ({conds_str})")
                best_branches = trial
                best_f1 = f
                pruned_branches = True
                total_branch_pruned += 1
                break

    if total_branch_pruned == 0:
        print("  No branches pruned.")

    # ══════════════════════════════════════════════════════════════════════
    # PHASE 6: FINAL FINE-TUNE (fine step, narrow window)
    # ══════════════════════════════════════════════════════════════════════
    print("\n" + "=" * 90)
    print("PHASE 6: Final fine-tune (step={}, ±{})".format(
        FINE_STEP, FINE_WINDOW))
    print("=" * 90)

    improved = True
    passes = 0
    while improved and passes < 15:
        improved = False
        passes += 1
        for bi in range(len(best_branches)):
            for ci in range(len(best_branches[bi])):
                prev_t = best_branches[bi][ci]['threshold']
                new_f1, new_t = _sweep_threshold(
                    best_branches, bi, ci, fine_t, FINE_WINDOW)
                if new_t != prev_t:
                    improved = True
                    if new_f1 > best_f1:
                        best_f1 = new_f1

    f1_final, p_final, r_final, tp_final, fp_final, fn_final = score(
        y_true, best_branches, col, N, total_pos, min_precision)

    # ══════════════════════════════════════════════════════════════════════
    # REPORT
    # ══════════════════════════════════════════════════════════════════════
    sep = "=" * 90
    print(f"\n{sep}")
    print("FINAL RESULTS")
    print(sep)

    print(f"\n  {'Metric':<12} {'Before':>10} {'After':>10}")
    print(f"  {'-'*34}")
    print(f"  {'Precision':<12} {p_init:>10.4f} {p_final:>10.4f}")
    print(f"  {'Recall':<12} {r_init:>10.4f} {r_final:>10.4f}")
    print(f"  {'F1-Score':<12} {f1_init:>10.4f} {f1_final:>10.4f}")
    print(f"  {'TP':<12} {tp_init:>10} {tp_final:>10}")
    print(f"  {'FP':<12} {fp_init:>10} {fp_final:>10}")
    print(f"  {'FN':<12} {fn_init:>10} {fn_final:>10}")

    print(f"\n{sep}")
    print(f"OPTIMIZED WHERE CLAUSE ({len(best_branches)} OR-branches):")
    print(sep)
    print()

    or_strs = []
    for branch in best_branches:
        cond_strs = [f"{c['feature']} {c['op']} {c['threshold']:.4f}"
                     for c in branch]
        if len(cond_strs) == 1:
            or_strs.append(cond_strs[0])
        else:
            inner = "\n        AND ".join(cond_strs)
            or_strs.append(f"(\n        {inner}\n    )")

    where_out = "\n    OR\n    ".join(or_strs)
    print(f"WHERE\n    {where_out}")

    # Also print as Python string ready to paste into evaluate_where_clause.py
    print(f"\n{sep}")
    print("COPY-PASTE READY (for evaluate_where_clause.py):")
    print(sep)
    print()
    print('WHERE_CLAUSE = """')
    for i, branch in enumerate(best_branches):
        cond_strs = [f"    {c['feature']} {c['op']} {c['threshold']:.4f}"
                     for c in branch]
        prefix = "    (" if i == 0 else "    OR ("
        print(prefix)
        print("\n        AND ".join(cond_strs))
        print("    )")
    print('"""')

    elapsed = time.time() - t0
    print(f"\nTime: {elapsed:.1f}s")
    print(sep)

    return best_branches, p_final, r_final, f1_final


if __name__ == "__main__":
    optimize_where_clause(CSV_PATH, WHERE_CLAUSE, MIN_PRECISION)
