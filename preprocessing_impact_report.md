# Preprocessing Impact Report

## Summary
The preprocessing step successfully improved citation parsing by normalizing common patterns before running the Rust analysis.

## Preprocessing Changes (Python)
- **Total citations processed**: 45,020
- **Citations changed**: 461 (1.0%)
- **Changes made**:
  - Fixed "aCP" → "a CP" patterns (missing space after 'a')
  - Fixed "aBauR" → "a BauR" patterns (mixed-case abbreviations)
  - Stripped trailing footnote digits from law titles (e.g., "administration3" → "administration")

## Analysis Results Comparison

### Original CSV (data_filtered.csv)
- **Successfully parsed**: 39,512 / 45,020 (87.8%)
- **Unparseable**: 5,508 (12.2%)
- **Unique laws**: 2,919
  - Federal laws (RS): 833
  - Cantonal/regional: 2,086
- **Total pairwise comparisons**: 11,474,527
- **Same article matches**: 511,752 (4.46%)

### Preprocessed CSV (data_filtered_citations_changed.csv)
- **Successfully parsed**: 39,793 / 45,020 (88.4%)
- **Unparseable**: 5,227 (11.6%)
- **Unique laws**: 2,921
  - Federal laws (RS): 834
  - Cantonal/regional: 2,087
- **Total pairwise comparisons**: 11,776,852
- **Same article matches**: 520,059 (4.42%)

## Impact of Preprocessing

### Citations Rescued
- **Total rescued**: 281 citations (5,508 → 5,227 unparseable)
- **Parsing rate improvement**: 87.8% → 88.4% (+0.6 percentage points)
- **Rescue success rate**: 281 / 461 = 61% of changed citations became parseable

### Law Coverage
- **Federal laws discovered**: +1 (833 → 834)
- **Cantonal laws discovered**: +1 (2,086 → 2,087)
- **New unique laws**: +2

### Comparison Impact
- **Additional comparisons**: +302,325 comparisons (11,474,527 → 11,776,852)
- **Additional matches**: +8,307 same-article matches (511,752 → 520,059)

## Key Patterns Fixed

1. **"aCP" patterns**: Citations like "43 aCP" were normalized to "43 a CP", making the "CP" abbreviation detectable
2. **Mixed-case abbreviations**: Citations like "Art. 41 aBauR" became "Art. 41 a BauR", properly separating the article marker from the law abbreviation
3. **Trailing footnotes**: Law titles with footnote numbers (e.g., "Loi sur l'administration3") were cleaned to enable better title matching

## Conclusion

The preprocessing step successfully:
- ✅ Reduced unparseable citations by 281 (5.1% reduction in failures)
- ✅ Improved overall parsing rate from 87.8% to 88.4%
- ✅ Enabled 302K+ additional pairwise comparisons
- ✅ Discovered 8K+ additional same-article matches

While the improvement appears modest (0.6 percentage points), it represents a **5.1% reduction in parsing failures**, demonstrating that normalization of common citation patterns is an effective preprocessing step.
