"""
Multi-Resolution Greedy Search v4.1 — Bucket-Aware OR+AND Optimizer
=====================================================================
Rule structure per OR-branch:
  (name_condition) AND (norm_condition) AND extra_AND_conditions

Where name/norm conditions can be:
  - OR mode: any feature in bucket >= shared threshold
  - single mode: specific_feature >= threshold
  - none mode: no condition from this bucket (always true)

Extra AND conditions can include ANY feature (misc, name, or norm
individuals) used to eliminate false positives from the base rule.

Key design:
  - Name / norm_name features: OR within bucket (or individual or none)
  - Misc features: AND (checked during FP elimination)
  - ALL features (including name/norm individuals) used for FP elimination
  - Irreducible FPs auto-detected (records with perfect scores on all features)
  - Search patterns: A(OR+OR), B(OR+single), C(single+single),
    D(single-bucket only), E(any single feature)
  - Targeted per-record search for uncaptured positives

Phases:
  1. Pre-compute atomic masks + bucket OR masks
  2. Detect irreducible FPs
  3. Structured search (A-E) with full-feature FP elimination
  4. Greedy OR-coverage
  4.5. Targeted search for uncaptured positives
  5. Fine-tune thresholds
  6. Report
"""

import pandas as pd
import numpy as np
import time
from itertools import combinations


def evaluate(y_true, mask, total_pos):
    """Fast P/R/F1 from boolean numpy arrays."""
    tp = int(np.sum(y_true & mask))
    captured = int(np.sum(mask))
    fp = captured - tp
    fn = total_pos - tp
    p = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    r = tp / total_pos if total_pos > 0 else 0.0
    f1 = 2 * p * r / (p + r) if (p + r) > 0 else 0.0
    return p, r, f1, tp, fp, fn


def find_optimal_where_clause(csv_path: str, beam_width: int = 30):
    t0 = time.time()

    print("=" * 90)
    print("MULTI-RESOLUTION GREEDY SEARCH v4.1 — BUCKET-AWARE OR+AND OPTIMIZER")
    print("=" * 90)

    df = pd.read_csv(csv_path).fillna(0)
    y_true = (df['id_i'] == df['id_j']).values.astype(bool)
    N = len(df)
    total_pos = int(y_true.sum())
    total_neg = N - total_pos

    print(
        f"Data: {N:,} rows | Positives: {total_pos:,} | Negatives: {total_neg:,}")
    print(f"Baseline (predict all): P={total_pos / N:.4f}")

    # ════════════════════ DEFINE BUCKETS ════════════════════
    buckets = {
        "name": [
            'name_score_containment', 'name_score_jaccard', 'name_score_lcs',
            'name_score_levenshtein', 'name_score_sbert', 'average_name_score'
        ],
        "norm_name": [
            'normalized_name_score_containment', 'normalized_name_score_jaccard',
            'normalized_name_score_lcs', 'normalized_name_score_levenshtein',
            'normalized_name_score_sbert', 'average_normalized_name_score'
        ],
        "misc": [
            'property_type_score', 'name_unit_score',
            'address_unit_score', 'supplier_score', 'star_ratings_score'
        ]
    }

    for bname in list(buckets.keys()):
        buckets[bname] = [f for f in buckets[bname] if f in df.columns]
        if not buckets[bname]:
            del buckets[bname]

    name_feats = buckets.get('name', [])
    norm_feats = buckets.get('norm_name', [])
    misc_feats = buckets.get('misc', [])
    active_misc = [f for f in misc_feats if df[f].max() > 0.001]
    all_features = name_feats + norm_feats + misc_feats

    print(f"\nFeatures: {len(all_features)} across {len(buckets)} buckets")
    for bname, feats in buckets.items():
        mode = "OR" if bname in ('name', 'norm_name') else "AND"
        print(f"  {bname} ({mode}): {', '.join(feats)}")
    inactive = set(misc_feats) - set(active_misc)
    if inactive:
        print(f"  Inactive misc (always ~0): {', '.join(inactive)}")

    # ════════════════════ PHASE 1: PRE-COMPUTE ════════════════════
    print(f"\n{'=' * 90}")
    print("PHASE 1: Pre-compute atomic masks + bucket OR masks")
    print(f"{'=' * 90}")

    col = {f: df[f].values.astype(float) for f in all_features}

    STEP = 0.02
    thresholds = np.round(np.arange(STEP, 1.0 + STEP / 2, STEP), 4)

    atomic = {}
    for f in all_features:
        for t in thresholds:
            atomic[(f, float(round(t, 4)))] = col[f] >= t

    name_or = {}
    norm_or = {}
    for t in thresholds:
        tk = float(round(t, 4))
        m = np.zeros(N, dtype=bool)
        for f in name_feats:
            m |= atomic[(f, tk)]
        name_or[tk] = m

        m = np.zeros(N, dtype=bool)
        for f in norm_feats:
            m |= atomic[(f, tk)]
        norm_or[tk] = m

    print(f"  {len(atomic):,} atomic masks + {2 * len(thresholds)} bucket-OR masks")

    # ════════════════════ PHASE 2: DETECT IRREDUCIBLE FPs ════════════════════
    print(f"\n{'=' * 90}")
    print("PHASE 2: Detect irreducible false positives")
    print(f"{'=' * 90}")

    irr_mask = ~y_true.copy()
    for f in all_features:
        tp_max = float(col[f][y_true].max()) if y_true.any() else 0
        irr_mask &= (col[f] >= tp_max)
    irr_idx = set(np.where(irr_mask)[0])
    IRR_FP = len(irr_idx)

    if IRR_FP == 0:
        print("  No irreducible FPs — 100% precision may be achievable!")
    else:
        max_p = total_pos / (total_pos + IRR_FP)
        print(f"  {IRR_FP} irreducible FPs (score >= TP max on ALL features)")
        print(
            f"  Max achievable precision: {total_pos}/{total_pos + IRR_FP} = {max_p:.4f}")
        for idx in sorted(irr_idx):
            r = df.iloc[idx]
            print(f"    Row {idx}: '{r.get('name_i', '?')}' vs '{r.get('name_j', '?')}' "
                  f"(id_i={r['id_i']}, id_j={r['id_j']})")
        print(f"  Target: maximize recall with FP = {IRR_FP}")

    # ════════════════════ clear STRUCTURED RULE SEARCH ════════════════════
    print(f"\n{'=' * 90}")
    print("PHASE 3: Structured rule search — ALL features for FP elimination")
    print(f"{'=' * 90}")

    pool = {}  # key -> (spec, mask, tp, fp)

    def try_eliminate_extra_fps(base_mask, irr_idx_set, name_spec, norm_spec):
        """Given a base mask, add AND conditions on ANY feature to eliminate
        FPs not in the irreducible set. Uses ALL features for elimination,
        not just misc."""
        captured_idx = np.where(base_mask)[0]
        fp_idx = captured_idx[~y_true[captured_idx]]
        extra_fp = [i for i in fp_idx if i not in irr_idx_set]

        if not extra_fp:
            tp = int(np.sum(y_true & base_mask))
            fp = len(fp_idx)
            if tp > 0:
                return ({'name': name_spec, 'norm': norm_spec, 'misc': {}},
                        base_mask, tp, fp)
            return None

        # Determine primary features (already used in name/norm spec)
        used_primary = set()
        if name_spec.get('mode') == 'single':
            used_primary.add(name_spec['feature'])
        if norm_spec.get('mode') == 'single':
            used_primary.add(norm_spec['feature'])

        # Build elimination feature list: misc first, then name/norm individuals
        elim_features = []
        for f in active_misc:
            if f not in used_primary:
                elim_features.append(f)
        for f in name_feats + norm_feats:
            if f not in used_primary and f not in elim_features:
                elim_features.append(f)

        cur_mask = base_mask.copy()
        extra_thresholds = {}
        remaining = set(extra_fp)

        # Single-feature analytical elimination
        for ef in elim_features:
            if not remaining:
                break
            rem_arr = np.array(sorted(remaining))
            efp_vals = col[ef][rem_arr]
            max_efp = float(np.max(efp_vals))

            t_elim = np.ceil(max_efp * 100 + 0.001) / 100.0
            if t_elim > 1.0:
                continue

            trial = cur_mask & (col[ef] >= t_elim)
            tp_surv = int(np.sum(y_true & trial))
            if tp_surv == 0:
                continue

            cur_mask = trial
            extra_thresholds[ef] = round(t_elim, 4)
            remaining = set(i for i in remaining if col[ef][i] >= t_elim)

        if remaining:
            # 2-feature elimination for stubborn extra FPs
            for ef1, ef2 in combinations(elim_features, 2):
                if not remaining:
                    break
                if ef1 in extra_thresholds or ef2 in extra_thresholds:
                    continue
                rem_arr = np.array(sorted(remaining))
                for t1 in thresholds:
                    survive_t1 = col[ef1][rem_arr] >= t1
                    if not survive_t1.any():
                        trial = cur_mask & (col[ef1] >= t1)
                        tp_s = int(np.sum(y_true & trial))
                        if tp_s > 0:
                            cur_mask = trial
                            extra_thresholds[ef1] = float(t1)
                            remaining = set()
                        break
                    surv_idx = rem_arr[survive_t1]
                    max_v2 = float(np.max(col[ef2][surv_idx]))
                    t2 = np.ceil(max_v2 * 100 + 0.001) / 100.0
                    if t2 > 1.0:
                        continue
                    trial = cur_mask & (col[ef1] >= t1) & (col[ef2] >= t2)
                    tp_s = int(np.sum(y_true & trial))
                    if tp_s > 0:
                        cur_mask = trial
                        extra_thresholds[ef1] = float(t1)
                        extra_thresholds[ef2] = round(t2, 4)
                        remaining = set()
                        break

        if not remaining:
            tp = int(np.sum(y_true & cur_mask))
            fp = int(np.sum(cur_mask & ~y_true))
            if tp > 0 and fp <= IRR_FP:
                return ({'name': name_spec, 'norm': norm_spec,
                         'misc': extra_thresholds},
                        cur_mask, tp, fp)
        return None

    NONE_SPEC = {'mode': 'none'}

    # ── A. Bucket-OR combinations (name_OR AND norm_OR) ──
    print("  A. Bucket-OR: any(name >= t) AND any(norm >= t) + elimination...")
    a_count = 0
    for t_n in thresholds:
        tn = float(round(t_n, 4))
        for t_r in thresholds:
            tr = float(round(t_r, 4))
            base = name_or[tn] & norm_or[tr]
            if int(np.sum(y_true & base)) == 0:
                continue
            result = try_eliminate_extra_fps(
                base, irr_idx,
                {'mode': 'OR', 'threshold': tn, 'features': name_feats},
                {'mode': 'OR', 'threshold': tr, 'features': norm_feats})
            if result:
                spec, mask, tp, fp = result
                key = f"A_{tn}_{tr}"
                if key not in pool or tp > pool[key][2]:
                    pool[key] = (spec, mask, tp, fp)
                a_count += 1
    print(f"    {a_count} valid rules")

    # ── B. Mixed: OR + individual (full threshold range) ──
    print("  B. Mixed: OR + individual (full thresholds) + elimination...")
    b_count = 0
    # name_OR AND individual_norm
    for t_n in thresholds:
        tn = float(round(t_n, 4))
        for nf in norm_feats:
            for t_r in thresholds:
                tr = float(round(t_r, 4))
                base = name_or[tn] & atomic[(nf, tr)]
                if int(np.sum(y_true & base)) == 0:
                    continue
                result = try_eliminate_extra_fps(
                    base, irr_idx,
                    {'mode': 'OR', 'threshold': tn, 'features': name_feats},
                    {'mode': 'single', 'threshold': tr, 'feature': nf})
                if result:
                    spec, mask, tp, fp = result
                    key = f"B1_{tn}_{nf}_{tr}"
                    if key not in pool or tp > pool[key][2]:
                        pool[key] = (spec, mask, tp, fp)
                    b_count += 1

    # individual_name AND norm_OR
    for nf in name_feats:
        for t_n in thresholds:
            tn = float(round(t_n, 4))
            for t_r in thresholds:
                tr = float(round(t_r, 4))
                base = atomic[(nf, tn)] & norm_or[tr]
                if int(np.sum(y_true & base)) == 0:
                    continue
                result = try_eliminate_extra_fps(
                    base, irr_idx,
                    {'mode': 'single', 'threshold': tn, 'feature': nf},
                    {'mode': 'OR', 'threshold': tr, 'features': norm_feats})
                if result:
                    spec, mask, tp, fp = result
                    key = f"B2_{nf}_{tn}_{tr}"
                    if key not in pool or tp > pool[key][2]:
                        pool[key] = (spec, mask, tp, fp)
                    b_count += 1
    print(f"    {b_count} valid rules")

    # ── C. Individual name AND individual norm (full thresholds) ──
    print("  C. Individual: name_feat AND norm_feat (full thresholds) + elimination...")
    c_count = 0
    for nf in name_feats:
        for t_n in thresholds:
            tn = float(round(t_n, 4))
            for rf in norm_feats:
                for t_r in thresholds:
                    tr = float(round(t_r, 4))
                    base = atomic[(nf, tn)] & atomic[(rf, tr)]
                    if int(np.sum(y_true & base)) == 0:
                        continue
                    result = try_eliminate_extra_fps(
                        base, irr_idx,
                        {'mode': 'single', 'threshold': tn, 'feature': nf},
                        {'mode': 'single', 'threshold': tr, 'feature': rf})
                    if result:
                        spec, mask, tp, fp = result
                        key = f"C_{nf}_{tn}_{rf}_{tr}"
                        if key not in pool or tp > pool[key][2]:
                            pool[key] = (spec, mask, tp, fp)
                        c_count += 1
    print(f"    {c_count} valid rules")

    # ── D. Single-bucket only (name OR norm, no cross-bucket required) ──
    print("  D. Single-bucket: name-only OR norm-only + elimination...")
    d_count = 0
    # Name bucket OR only
    for t_n in thresholds:
        tn = float(round(t_n, 4))
        base = name_or[tn]
        if int(np.sum(y_true & base)) == 0:
            continue
        result = try_eliminate_extra_fps(
            base, irr_idx,
            {'mode': 'OR', 'threshold': tn, 'features': name_feats},
            NONE_SPEC)
        if result:
            spec, mask, tp, fp = result
            key = f"D_name_OR_{tn}"
            if key not in pool or tp > pool[key][2]:
                pool[key] = (spec, mask, tp, fp)
            d_count += 1

    # Norm bucket OR only
    for t_r in thresholds:
        tr = float(round(t_r, 4))
        base = norm_or[tr]
        if int(np.sum(y_true & base)) == 0:
            continue
        result = try_eliminate_extra_fps(
            base, irr_idx,
            NONE_SPEC,
            {'mode': 'OR', 'threshold': tr, 'features': norm_feats})
        if result:
            spec, mask, tp, fp = result
            key = f"D_norm_OR_{tr}"
            if key not in pool or tp > pool[key][2]:
                pool[key] = (spec, mask, tp, fp)
            d_count += 1

    # Individual name feature only
    for nf in name_feats:
        for t_n in thresholds:
            tn = float(round(t_n, 4))
            base = atomic[(nf, tn)]
            if int(np.sum(y_true & base)) == 0:
                continue
            result = try_eliminate_extra_fps(
                base, irr_idx,
                {'mode': 'single', 'threshold': tn, 'feature': nf},
                NONE_SPEC)
            if result:
                spec, mask, tp, fp = result
                key = f"D_{nf}_{tn}"
                if key not in pool or tp > pool[key][2]:
                    pool[key] = (spec, mask, tp, fp)
                d_count += 1

    # Individual norm feature only
    for rf in norm_feats:
        for t_r in thresholds:
            tr = float(round(t_r, 4))
            base = atomic[(rf, tr)]
            if int(np.sum(y_true & base)) == 0:
                continue
            result = try_eliminate_extra_fps(
                base, irr_idx,
                NONE_SPEC,
                {'mode': 'single', 'threshold': tr, 'feature': rf})
            if result:
                spec, mask, tp, fp = result
                key = f"D_{rf}_{tr}"
                if key not in pool or tp > pool[key][2]:
                    pool[key] = (spec, mask, tp, fp)
                d_count += 1
    print(f"    {d_count} valid rules")

    # ── E. Any single feature (including misc) + elimination ──
    print("  E. Any single feature (incl. misc) + elimination...")
    e_count = 0
    for f in all_features:
        for t in thresholds:
            tk = float(round(t, 4))
            base = atomic[(f, tk)]
            if int(np.sum(y_true & base)) == 0:
                continue
            # Determine which spec slot this feature belongs to
            if f in name_feats:
                ns = {'mode': 'single', 'threshold': tk, 'feature': f}
                rs = NONE_SPEC
            elif f in norm_feats:
                ns = NONE_SPEC
                rs = {'mode': 'single', 'threshold': tk, 'feature': f}
            else:
                ns = NONE_SPEC
                rs = NONE_SPEC
            result = try_eliminate_extra_fps(base, irr_idx, ns, rs)
            if result:
                spec, mask, tp, fp = result
                # For misc features, store the base in misc dict too
                if f in misc_feats and f not in spec['misc']:
                    spec['misc'][f] = tk
                key = f"E_{f}_{tk}"
                if key not in pool or tp > pool[key][2]:
                    pool[key] = (spec, mask, tp, fp)
                e_count += 1
    print(f"    {e_count} valid rules")

    pool_size = len(pool)
    print(f"\n  Total pool: {pool_size} candidate rules")

    if pool:
        top = sorted(pool.values(), key=lambda x: -x[2])[:10]
        print(f"  Top 10 by TP:")
        for spec, _, tp, fp in top:
            parts = []
            ns = spec['name']
            if ns['mode'] == 'OR':
                parts.append(f"any_name>={ns['threshold']}")
            elif ns['mode'] == 'single':
                parts.append(f"{ns['feature']}>={ns['threshold']}")
            rs = spec['norm']
            if rs['mode'] == 'OR':
                parts.append(f"any_norm>={rs['threshold']}")
            elif rs['mode'] == 'single':
                parts.append(f"{rs['feature']}>={rs['threshold']}")
            for k, v in spec['misc'].items():
                parts.append(f"{k}>={v:.2f}")
            print(f"    TP={tp:>4d} FP={fp} | {' AND '.join(parts)}")

    # ════════════════════ PHASE 4: GREEDY OR-COVERAGE ════════════════════
    print(f"\n{'=' * 90}")
    print(f"PHASE 4: Greedy OR-coverage (FP <= {IRR_FP})")
    print(f"{'=' * 90}")

    entries = sorted(pool.values(), key=lambda x: -x[2])
    print(f"  {len(entries)} candidate rules")

    selected = []
    sel_masks = []
    combined = np.zeros(N, dtype=bool)
    combined_tp = 0

    for step in range(100):
        remaining = total_pos - combined_tp
        if remaining == 0:
            print(f"  All {total_pos} positives covered!")
            break

        uncaptured = y_true & ~combined
        best_dtp = 0
        best_entry = None

        for spec, mask, _, _ in entries:
            dtp = int(np.sum(uncaptured & mask))
            if dtp <= best_dtp:
                continue
            trial = combined | mask
            trial_fp = int(np.sum(trial & ~y_true))
            if trial_fp > IRR_FP:
                continue
            best_dtp = dtp
            best_entry = (spec, mask)

        if best_entry is None or best_dtp == 0:
            break

        spec, mask = best_entry
        combined = combined | mask
        combined_tp += best_dtp
        selected.append(spec)
        sel_masks.append(mask)

        fp_now = int(np.sum(combined & ~y_true))
        r = combined_tp / total_pos
        remaining = total_pos - combined_tp
        print(f"  Step {step + 1}: +{best_dtp}TP -> TP={combined_tp} FP={fp_now} "
              f"P={combined_tp / (combined_tp + fp_now):.4f} R={r:.4f} "
              f"(remaining={remaining})")

    # ════════════ PHASE 4.5: TARGETED SEARCH FOR UNCAPTURED ════════════
    uncaptured_pos = y_true & ~combined
    uncaptured_count = int(uncaptured_pos.sum())

    if uncaptured_count > 0:
        print(f"\n{'=' * 90}")
        print(
            f"PHASE 4.5: Targeted search for {uncaptured_count} uncaptured positives")
        print(f"{'=' * 90}")
        uncaptured_idx = np.where(uncaptured_pos)[0]

        # For each uncaptured positive, build rules from its actual values
        targeted_found = 0
        for batch_start in range(0, len(uncaptured_idx), 50):
            if not (y_true & ~combined).any():
                break
            batch = uncaptured_idx[batch_start:batch_start + 50]
            batch = [ui for ui in batch if y_true[ui] and not combined[ui]]
            if not batch:
                continue

            # Try single-feature rules using actual values as thresholds
            for f in all_features:
                for ui in batch:
                    if combined[ui]:
                        continue
                    val = col[f][ui]
                    if val <= 0:
                        continue
                    t = round(float(val), 4)
                    mask = col[f] >= t
                    trial = combined | mask
                    trial_fp = int(np.sum(trial & ~y_true))
                    if trial_fp > IRR_FP:
                        continue
                    new_tp = int(np.sum((y_true & ~combined) & mask))
                    if new_tp > 0:
                        if f in name_feats:
                            ns = {'mode': 'single',
                                  'threshold': t, 'feature': f}
                            rs = NONE_SPEC
                        elif f in norm_feats:
                            ns = NONE_SPEC
                            rs = {'mode': 'single',
                                  'threshold': t, 'feature': f}
                        else:
                            ns = NONE_SPEC
                            rs = NONE_SPEC
                        spec = {'name': ns, 'norm': rs, 'misc': {}}
                        if f in misc_feats:
                            spec['misc'][f] = t
                        combined = combined | mask
                        combined_tp += new_tp
                        selected.append(spec)
                        sel_masks.append(mask)
                        targeted_found += 1
                        fp_now = int(np.sum(combined & ~y_true))
                        r = combined_tp / total_pos
                        remaining = int((y_true & ~combined).sum())
                        print(f"    Single({f}>={t:.4f}): +{new_tp}TP -> "
                              f"TP={combined_tp} FP={fp_now} R={r:.4f} "
                              f"(remaining={remaining})")
                        break
                if combined[batch[0]] if batch else True:
                    break

            # Try pair-feature rules from actual values
            for ui in batch:
                if combined[ui]:
                    continue
                cand = []
                for f in all_features:
                    val = col[f][ui]
                    if val > 0:
                        cand.append((f, round(float(val), 4)))
                best_pair = None
                best_pair_tp = 0
                for i, (f1, t1) in enumerate(cand):
                    m1 = col[f1] >= t1
                    for j in range(i + 1, len(cand)):
                        f2, t2 = cand[j]
                        pair_mask = m1 & (col[f2] >= t2)
                        trial = combined | pair_mask
                        trial_fp = int(np.sum(trial & ~y_true))
                        if trial_fp > IRR_FP:
                            continue
                        new_tp = int(np.sum((y_true & ~combined) & pair_mask))
                        if new_tp > best_pair_tp:
                            best_pair_tp = new_tp
                            best_pair = (f1, t1, f2, t2, pair_mask, new_tp)
                if best_pair:
                    f1, t1, f2, t2, pair_mask, new_tp = best_pair
                    misc_d = {}
                    if f1 in name_feats:
                        ns = {'mode': 'single', 'threshold': t1, 'feature': f1}
                    else:
                        ns = NONE_SPEC
                        misc_d[f1] = t1
                    if f2 in norm_feats:
                        rs = {'mode': 'single', 'threshold': t2, 'feature': f2}
                    elif f2 in name_feats and ns['mode'] == 'none':
                        ns = {'mode': 'single', 'threshold': t2, 'feature': f2}
                    else:
                        rs = NONE_SPEC if 'rs' not in dir() or rs == NONE_SPEC else rs
                        misc_d[f2] = t2
                    if 'rs' not in dir():
                        rs = NONE_SPEC
                    spec = {'name': ns, 'norm': rs, 'misc': misc_d}
                    combined = combined | pair_mask
                    combined_tp += new_tp
                    selected.append(spec)
                    sel_masks.append(pair_mask)
                    targeted_found += 1
                    fp_now = int(np.sum(combined & ~y_true))
                    r = combined_tp / total_pos
                    remaining = int((y_true & ~combined).sum())
                    print(f"    Pair({f1}>={t1:.4f},{f2}>={t2:.4f}): +{new_tp}TP -> "
                          f"TP={combined_tp} FP={fp_now} R={r:.4f} "
                          f"(remaining={remaining})")

        uncaptured_final = int((y_true & ~combined).sum())
        if uncaptured_final > 0:
            print(
                f"  {uncaptured_final} positives still uncaptured after targeted search")

    if total_pos - combined_tp > 0:
        print(f"\n  {total_pos - combined_tp} positives could not be captured "
              f"with FP <= {IRR_FP}")

    # ════════════════════ PHASE 5: FINE-TUNE ════════════════════
    print(f"\n{'=' * 90}")
    print("PHASE 5: Fine-tune thresholds (step=0.01)")
    print(f"{'=' * 90}")

    if not selected:
        print("  No rules to fine-tune.")
    else:
        fine_t = np.round(np.arange(0.01, 1.005, 0.01), 4)

        def build_mask_from_spec(spec):
            """Build numpy mask from a rule spec."""
            ns = spec['name']
            if ns.get('mode') == 'none':
                m = np.ones(N, dtype=bool)
            elif ns['mode'] == 'OR':
                m = np.zeros(N, dtype=bool)
                if 'per_feat' in ns:
                    for feat, t in ns['per_feat'].items():
                        m |= (col[feat] >= t)
                else:
                    for feat in ns['features']:
                        m |= (col[feat] >= ns['threshold'])
            else:
                m = col[ns['feature']] >= ns['threshold']

            rs = spec['norm']
            if rs.get('mode') == 'none':
                pass  # no norm condition
            elif rs['mode'] == 'OR':
                nm = np.zeros(N, dtype=bool)
                if 'per_feat' in rs:
                    for feat, t in rs['per_feat'].items():
                        nm |= (col[feat] >= t)
                else:
                    for feat in rs['features']:
                        nm |= (col[feat] >= rs['threshold'])
                m &= nm
            else:
                m &= col[rs['feature']] >= rs['threshold']

            for mf, mt in spec.get('misc', {}).items():
                if mt > 0:
                    m &= col[mf] >= mt
            return m

        def evaluate_all_rules():
            comb = np.zeros(N, dtype=bool)
            for spec in selected:
                comb |= build_mask_from_spec(spec)
            return evaluate(y_true, comb, total_pos)

        before = evaluate_all_rules()
        print(
            f"  Before: P={before[0]:.4f} R={before[1]:.4f} TP={before[3]} FP={before[4]}")

        best_tp = before[3]
        improved = True
        pass_n = 0
        while improved and pass_n < 5:
            improved = False
            pass_n += 1
            for spec in selected:
                # Tune name threshold
                ns = spec['name']
                if ns.get('mode') not in ('none',):
                    orig = ns['threshold']
                    best_t_val = orig
                    for t in fine_t:
                        if abs(t - orig) > 0.25:
                            continue
                        ns['threshold'] = float(t)
                        m = evaluate_all_rules()
                        if m[4] <= IRR_FP and m[3] > best_tp:
                            best_tp = m[3]
                            best_t_val = float(t)
                    ns['threshold'] = best_t_val
                    if best_t_val != orig:
                        improved = True

                # Tune norm threshold
                rs = spec['norm']
                if rs.get('mode') not in ('none',):
                    orig = rs['threshold']
                    best_t_val = orig
                    for t in fine_t:
                        if abs(t - orig) > 0.25:
                            continue
                        rs['threshold'] = float(t)
                        m = evaluate_all_rules()
                        if m[4] <= IRR_FP and m[3] > best_tp:
                            best_tp = m[3]
                            best_t_val = float(t)
                    rs['threshold'] = best_t_val
                    if best_t_val != orig:
                        improved = True

                # Tune misc thresholds
                for mf in list(spec.get('misc', {}).keys()):
                    orig = spec['misc'][mf]
                    best_t_val = orig
                    for t in fine_t:
                        if abs(t - orig) > 0.25:
                            continue
                        spec['misc'][mf] = float(t)
                        m = evaluate_all_rules()
                        if m[4] <= IRR_FP and m[3] > best_tp:
                            best_tp = m[3]
                            best_t_val = float(t)
                    spec['misc'][mf] = best_t_val
                    if best_t_val != orig:
                        improved = True

        after = evaluate_all_rules()
        print(
            f"  After:  P={after[0]:.4f} R={after[1]:.4f} TP={after[3]} FP={after[4]}")

        # ── Phase 5b: Per-feature thresholds within OR buckets ──
        print(f"\n  5b. Per-feature threshold tuning within OR buckets...")
        for spec in selected:
            for bucket_key in ['name', 'norm']:
                bs = spec[bucket_key]
                if bs.get('mode') != 'OR':
                    continue
                shared_t = bs['threshold']
                feats = bs['features']
                per_feat = {f: shared_t for f in feats}
                bs['per_feat'] = per_feat

                for feat in feats:
                    orig = per_feat[feat]
                    best_t_val = orig
                    for t in fine_t:
                        if abs(t - orig) > 0.30:
                            continue
                        per_feat[feat] = float(t)
                        m = evaluate_all_rules()
                        if m[4] <= IRR_FP and m[3] > best_tp:
                            best_tp = m[3]
                            best_t_val = float(t)
                    per_feat[feat] = best_t_val

        after_5b = evaluate_all_rules()
        if after_5b[3] > after[3]:
            print(f"  After 5b: P={after_5b[0]:.4f} R={after_5b[1]:.4f} "
                  f"TP={after_5b[3]} FP={after_5b[4]}")
        else:
            print(f"  After 5b: no additional improvement")

    # ════════════════════ PHASE 6: REPORT ════════════════════
    if selected:
        comb = np.zeros(N, dtype=bool)
        for spec in selected:
            comb |= build_mask_from_spec(spec)
        p, r, f1, tp, fp, fn = evaluate(y_true, comb, total_pos)
    else:
        p, r, f1, tp, fp, fn = 0, 0, 0, 0, 0, total_pos

    # Format WHERE clause
    or_parts = []
    for spec in selected:
        and_parts = []

        ns = spec['name']
        if ns.get('mode') == 'OR':
            if 'per_feat' in ns:
                name_ors = [f"{feat} >= {t:.4f}" for feat, t in ns['per_feat'].items()
                            if t > 0]
            else:
                name_ors = [
                    f"{feat} >= {ns['threshold']:.4f}" for feat in ns['features']]
            if name_ors:
                and_parts.append("(" + " OR ".join(name_ors) + ")")
        elif ns.get('mode') == 'single':
            and_parts.append(f"{ns['feature']} >= {ns['threshold']:.4f}")

        rs = spec['norm']
        if rs.get('mode') == 'OR':
            if 'per_feat' in rs:
                norm_ors = [f"{feat} >= {t:.4f}" for feat, t in rs['per_feat'].items()
                            if t > 0]
            else:
                norm_ors = [
                    f"{feat} >= {rs['threshold']:.4f}" for feat in rs['features']]
            if norm_ors:
                and_parts.append("(" + " OR ".join(norm_ors) + ")")
        elif rs.get('mode') == 'single':
            and_parts.append(f"{rs['feature']} >= {rs['threshold']:.4f}")

        for mf, mt in spec.get('misc', {}).items():
            if mt > 0:
                and_parts.append(f"{mf} >= {mt:.4f}")

        clause = "\n        AND ".join(and_parts)
        or_parts.append(clause)

    if len(or_parts) > 1:
        where_str = ("\n    OR\n      ").join(
            [f"({part})" if "\n" in part or " AND " in part else part
             for part in or_parts])
    elif len(or_parts) == 1:
        where_str = or_parts[0]
    else:
        where_str = "FALSE  -- no valid rules found"

    print(f"\n{'=' * 90}")
    print("FINAL RESULTS")
    print(f"{'=' * 90}")
    print(f"\nOPTIMAL WHERE CLAUSE ({len(selected)} OR-branches):\n")
    print(f"  WHERE {where_str}")
    print(f"\n  Precision : {p:.4f}  ({tp}/{tp + fp})")
    print(f"  Recall    : {r:.4f}  ({tp}/{total_pos})")
    print(f"  F1-Score  : {f1:.4f}")
    print(f"  True  Positives (id_i==id_j captured) : {tp:,}")
    print(f"  False Positives (id_i!=id_j leaked)   : {fp:,}")
    print(f"  False Negatives (id_i==id_j missed)   : {fn:,}")

    elapsed = time.time() - t0
    print(f"\n{'=' * 90}")
    if IRR_FP == 0 and p == 1.0:
        if r == 1.0:
            print("PERFECT 100% PRECISION + 100% RECALL!")
        else:
            print(f"100% PRECISION with {r * 100:.2f}% recall "
                  f"(TP={tp} FN={fn}, 0 false positives)")
    elif fp <= IRR_FP:
        print(f"BEST ACHIEVABLE: P={p * 100:.2f}% R={r * 100:.2f}% "
              f"(FP={fp} = irreducible minimum of {IRR_FP})")
    else:
        print(f"Best: P={p * 100:.2f}%  R={r * 100:.2f}%  F1={f1:.4f}")
    print(f"Time: {elapsed:.1f}s")
    print("=" * 90)


if __name__ == "__main__":
    find_optimal_where_clause('/Users/nakul.patil/Downloads/hotels.csv')
