# Hotel Pair Scoring Logic (Short Reference)

This is the current scoring behavior used by the pair scoring job.

## Scope

- Active scorer entrypoint: `spark/jobs/ingestion/run_scoring_job.py`
- Active processor: `src/hotel_data/pipeline/preprocessor/processors/hotel_pair_scorer_processor.py`
- Name algorithms: `src/hotel_data/pipeline/preprocessor/utils/name_utils.py`
- Mismatch rules: `src/hotel_data/pipeline/scoring/scorers/mismatch_rules.py`

## Pair Generation

- Candidate pairs are created by `GeoHashBlocker`.
- Exact duplicate pair rows are removed with `dropDuplicates(["uid_i", "uid_j"])`.
- Pairs are geo-filtered to `geo_distance_km <= 0.5`.

## Name Scores

For both raw `name_*` and `normalized_name_*`:

- `*_score_containment`
Formula: `|intersection(tokens_i, tokens_j)| / min(|set_i|, |set_j|)`
- `*_score_jaccard`
Formula: `|intersection| / |union|`
- `*_score_lcs`
Formula: `longest_common_substring_len / max(len(str_i), len(str_j))`
- `*_score_levenshtein`
Formula: `max(strict_ratio, damped_partial_ratio)` where damping uses weighted token overlap.
- `*_score_sbert`
Formula: cosine similarity between embeddings with empty-value handling.

Empty handling for name scores:

- Both empty -> `0.5`
- One empty -> `0.0`

Aggregate name scores:

- `average_name_score = (jaccard + lcs + levenshtein + sbert) / 4`
- `average_normalized_name_score = (normalized_jaccard + normalized_lcs + normalized_levenshtein + normalized_sbert) / 4`

## Address Scores

- `address_line1_score`
Token-sort fuzzy score from `token_sort_score(...)`.
- `address_sbert_score`
Cosine similarity of address embeddings.
- `postal_code_match`
`1.0` if equal, `0.0` if different, `0.5` if either side missing.
- `country_match`
`1.0` if equal, `0.0` if different, `0.5` if either side missing.

## Contact Scores

- `phone_match_score`, `email_match_score`, `fax_match_score`
Rules: overlap -> `1.0`, no overlap -> `0.0`, missing on either side -> `0.5`.

## Mismatch / Guardrail Scores

- `property_type_score`
Rules:
- Same known type -> `1.0`
- Soft-compatible groups (example: `hotel` vs `resort`) -> `0.8`
- Hard mismatch -> `0.0`
- Unknown/missing placeholders (example: `unknown`, `n/a`, `null`) -> `0.5`

- `name_unit_score`
Uses number/roman unit consistency from hotel names.
Rules include:
- Matching/safe subset units -> high (`1.0` or `0.9`)
- Contradictory units on same base name -> `0.0`
- Base text different -> neutral `1.0`

- `address_unit_score`
Uses gradual numeric unit consistency from address lines, with postal-code exclusion.

Rules:

- Extract numeric unit tokens from `contact_address_line1_i/j`
- Remove postal code numeric tokens using `contact_address_postalCode_i/j`
- If neither side has numeric tokens (`{} vs {}`): `1.0`
- If only one side has numeric tokens: `0.9`
- If both sides have numeric tokens:
Formula branches:
- `nums_i == nums_j` -> `1.0`
- `intersection(nums_i, nums_j) == {}` -> `0.0`
- `nums_i ⊂ nums_j` or `nums_j ⊂ nums_i` -> `0.85`
- otherwise partial overlap -> `( |intersection| / |union| ) * 0.9`

Important:

- Postal code digits are intentionally excluded from `address_unit_score`.
- Postal consistency is handled separately by `postal_code_match`.

## Other Signals

- `supplier_score`
`1` if same provider, else `0`.
- `star_ratings_score`
Computed via `star_rating_score(...)` utility.

## Final Notes

- This pipeline emits feature scores; downstream rules decide final match/no-match.
- When tuning thresholds, treat these as independent signals and avoid double-penalizing the same evidence source.

## Worked Examples

Use these examples to quickly correlate the rules with expected outputs.

### Example 1: Address units conflict (postal excluded)

- `contact_address_postalCode_i = 400070`
- `contact_address_postalCode_j = 400070`
- `contact_address_line1_i = 4rd floor kamal mansion hnaa rd, colaba, mumbai 400070`
- `contact_address_line1_j = 121 kartar bhavan shahid bhagat singh road, colaba, mumbai 400070`

Expected:

- `postal_code_match = 1.0` (same postal)
- `address_unit_score = 0.0`

Why:

- Numeric unit tokens after postal removal are effectively `{4}` vs `{121}`.
- Both sides have units and they are disjoint.
- Jaccard overlap = `0 / 2 = 0.0`.

### Example 2: One-sided unit evidence in address

- `contact_address_postalCode_i = 400070`
- `contact_address_postalCode_j = 400070`
- `contact_address_line1_i = shop no- 1 lal bahadur shastri marg`
- `contact_address_line1_j = lal bahadur shastri marg, kurla west, mumbai, maharashtra 400070`

Expected:

- `postal_code_match = 1.0`
- `address_unit_score = 0.9`

Why:

- After postal removal, one side still has unit `{1}`, the other has no unit token.
- One-sided numeric evidence is treated as ambiguous (`0.9`).

### Example 3: Partial overlap gives gradual value

- `contact_address_postalCode_i = 400051`
- `contact_address_postalCode_j = null`
- `contact_address_line1_i = 9 floor building no.11 ... 400051`
- `contact_address_line1_j = building no- 11 shop no 819 ...`

Expected:

- Numeric tokens after postal removal: `{9, 11}` vs `{11, 819}`
- `address_unit_score = (1 / 3) * 0.9 = 0.3`

Why:

- Overlap is partial, not full match.
- Weighted partial-overlap score = `Jaccard * 0.9 = (1 / 3) * 0.9 = 0.3`.

### Example 4: Subset relation is high but not perfect

- `nums_i = {11}`
- `nums_j = {11, 13}`

Expected:

- `address_unit_score = 0.85`

Why:

- One side is a strict subset of the other.
- This is treated as near match with ambiguity, not perfect `1.0`.

### Example 5: Unknown property type

- `type_i = hotel`
- `type_j = unknown`

Expected:

- `property_type_score = 0.5`

Why:

- `unknown`/`n/a`/`null` markers are treated as missing type evidence.
- Missing/unknown type is neutral, not a hard mismatch.

### Example 6: Normalized containment can be 1.0 for subset names

- `normalized_name_i = hotel o inn chhatrapati shivaji international airport`
- `normalized_name_j = hotel inn-near international airport`

Expected:

- `normalized_name_score_containment = 1.0`

Why:

- Hyphen is split (`inn-near -> inn near`) and stop words are removed (`near`).
- Token set of `j` becomes a subset of token set of `i`.
- Containment uses `intersection / min_len`, so a full subset gives `1.0`.

### Example 7: Type soft match vs hard mismatch

- Pair A: `type_i = hotel`, `type_j = resort` -> `property_type_score = 0.8`
- Pair B: `type_i = condo`, `type_j = hotel` -> `property_type_score = 0.0`

Why:

- `hotel/resort` is in configured soft-compatible groups.
- `condo/hotel` is a hard mismatch outside soft groups.