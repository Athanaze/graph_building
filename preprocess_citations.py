#!/usr/bin/env python3
"""
Preprocess citations in data_filtered.csv by applying normalization fixes.
This improves citation quality before running the Rust analysis.

Outputs:
  - data_filtered_citations_changed.csv: CSV with normalized citations
  - preprocessing_failures.jsonl: Citations that couldn't be normalized
"""

import csv
import json
import re
from collections import defaultdict

def normalize_citation(citation):
    """Apply normalization fixes to a citation."""
    if not citation or not isinstance(citation, str):
        return citation, False, False

    original = citation

    # Fix pattern 1: "43 aCP" -> "43 a CP" (digit + space + a + uppercase)
    citation = re.sub(r'(\d+)\s+a([A-ZÄÖÜ][A-ZÄÖÜa-zäöüß]{1,})\b', r'\1 a \2', citation)

    # Fix pattern 2: " aBauR" -> " a BauR" (space + a + uppercase)
    citation = re.sub(r'(\s)a([A-ZÄÖÜ][A-ZÄÖÜa-zäöüß]{1,})\b', r'\1a \2', citation)

    # Fix pattern 3: "Art.6VwVG" -> "Art. 6 VwVG" (missing spaces)
    # Match: Art/art + optional dot + digit(s) + uppercase letter sequence
    citation = re.sub(r'\b([Aa]rt)\.?(\d+)([A-ZÄÖÜ][A-ZÄÖÜa-zäöü]{1,})\b', r'\1. \2 \3', citation)

    # Strip trailing footnote digits from law titles
    # e.g., "Loi sur l'administration3" -> "Loi sur l'administration"
    citation = re.sub(r'([a-zàâäéèêëïîôùûüÿœæç])\d+$', r'\1', citation)

    # Check if citation is just digits (after all normalizations)
    # These are likely article numbers without law context and should be removed
    is_digit_only = citation.strip().replace(' ', '').replace('.', '').isdigit()

    changed = citation != original
    return citation, changed, is_digit_only

def is_garbage_citation(citation):
    """
    Check if a citation is garbage that should be filtered out.
    Returns (is_garbage, reason)
    """
    if not citation or not isinstance(citation, str):
        return False, None

    citation_lower = citation.lower()

    # 1. Just paragraph/letter markers without article (e.g., "Abs. 3", "al. 2")
    if re.match(r'^(Abs\.|Al\.|al\.|lit\.|let\.|Lit\.|Let\.)\s', citation):
        return True, 'fragment_marker'

    # 2. Dates (ONLY if it's mainly a date, not a law with date in title)
    # Law titles like "loi du 14 décembre 1990" are VALID, so only flag if:
    # - Contains month name AND
    # - Does NOT contain "Art." or article marker
    month_patterns = ['januar', 'februar', 'märz', 'april', 'mai', 'juni', 'juli', 'august',
                      'september', 'oktober', 'november', 'dezember', 'janvier', 'février',
                      'mars', 'avril', 'mai', 'juin', 'juillet', 'août', 'septembre',
                      'octobre', 'novembre', 'décembre', 'gennaio', 'febbraio', 'marzo',
                      'aprile', 'maggio', 'giugno', 'luglio', 'agosto', 'settembre',
                      'ottobre', 'novembre', 'dicembre']
    has_month = any(month in citation_lower for month in month_patterns)
    has_article = bool(re.search(r'\b[Aa]rt\.?\s*\d', citation))
    if has_month and not has_article:
        return True, 'date_reference'

    # 3. Page references (207ff., 218f., etc.)
    if re.match(r'^\d+\s*(ff?\.?)$', citation):
        return True, 'page_reference'

    # 4. Incomplete sentence fragments
    incomplete_endings = ['de la', 'du', 'des', "de l'", 'della', 'del', 'ist (', 'sind (',
                          'sowie ', 'et art', 'e art']
    if any(citation.endswith(ending) for ending in incomplete_endings):
        return True, 'incomplete_fragment'

    # 5. Proposal/motion markers (not actual law)
    proposal_words = ['proposition', 'motion', 'postulat', 'initiative', 'anfrage']
    if any(word in citation_lower for word in proposal_words):
        return True, 'legislative_proposal'

    # 6. Incomplete fragments (ends with "Abs" without period, or "Art" without period/number)
    if re.search(r'(Abs|abs|Art|art)\s*$', citation):
        return True, 'incomplete_fragment'

    # 7. Non-citation text (no article marker at all, and contains lots of text)
    if len(citation) > 50 and not re.search(r'\b[Aa]rt\.?\s*\d', citation):
        # Long text without article reference
        return True, 'non_citation_text'

    return False, None

def extract_law_from_citations(citations):
    """
    Extract a Swiss law reference from a list of citations.
    Returns the law abbreviation/RS number if found, None otherwise.
    """
    # Swiss law patterns to look for
    swiss_law_abbrevs = [
        'stgb', 'zgb', 'or', 'bv', 'schkg', 'zpo', 'stpo',
        'uvg', 'ahvg', 'kvg', 'bvg', 'avig', 'urg', 'dsg',
        'usg', 'mwstg', 'dbg', 'vwvg', 'pa', 'cp', 'cc',
        'cst', 'cste', 'but', 'cost'
    ]

    # Check each citation for a Swiss law reference
    for citation in citations:
        citation_lower = citation.lower()

        # Check for RS/SR number
        if 'rs ' in citation_lower or 'sr ' in citation_lower:
            # Extract RS number
            match = re.search(r'\b(?:rs|sr)\s*(\d+(?:\.\d+)*)\b', citation_lower)
            if match:
                return f"RS {match.group(1)}"

        # Check for known abbreviations
        for abbrev in swiss_law_abbrevs:
            # Look for word boundary to avoid partial matches
            pattern = r'\b' + re.escape(abbrev) + r'\b'
            if re.search(pattern, citation_lower):
                return abbrev.upper()

    return None

def enrich_article_only_citations(citations):
    """
    For article-only citations (e.g., "art. 128"), try to associate them
    with a law found in the same element's citations.
    Returns (enriched_citations, enrichment_count)

    CONSERVATIVE: Only enrich if there's exactly ONE Swiss law in the element
    to avoid incorrect associations.
    """
    # Find ALL Swiss law references in citations
    all_laws = []
    for citation in citations:
        citation_lower = citation.lower()

        # Check for RS/SR number
        if 'rs ' in citation_lower or 'sr ' in citation_lower:
            match = re.search(r'\b(?:rs|sr)\s*(\d+(?:\.\d+)*)\b', citation_lower)
            if match:
                all_laws.append(f"RS {match.group(1)}")
                continue

        # Check for known abbreviations
        swiss_law_abbrevs = [
            'stgb', 'zgb', 'or', 'bv', 'schkg', 'zpo', 'stpo',
            'uvg', 'ahvg', 'kvg', 'bvg', 'avig', 'urg', 'dsg',
            'usg', 'mwstg', 'dbg', 'vwvg', 'pa', 'cp', 'cc',
            'cst', 'cste', 'but', 'cost', 'bgg', 'svg', 'emrk', 'cedh'
        ]
        for abbrev in swiss_law_abbrevs:
            if re.search(r'\b' + re.escape(abbrev) + r'\b', citation_lower):
                all_laws.append(abbrev.upper())
                break

    # Only enrich if there's exactly ONE unique law (safe assumption)
    unique_laws = list(set(all_laws))
    if len(unique_laws) != 1:
        return citations, 0

    law_ref = unique_laws[0]

    enriched = []
    enrichment_count = 0

    for citation in citations:
        # Check if it's a truly article-only citation (no law abbreviation at all)
        citation_lower = citation.lower().strip()

        # Check for Swiss law abbreviations (comprehensive list)
        swiss_law_indicators = [
            'stgb', 'zgb', 'or', 'bv', 'schkg', 'zpo', 'stpo',
            'uvg', 'ahvg', 'kvg', 'bvg', 'avig', 'urg', 'dsg',
            'usg', 'mwstg', 'dbg', 'vwvg', 'pa', 'cp', 'cc',
            'bgg', 'svg', 'emrk', 'cedh', 'cost', 'cst', 'cste', 'but'
        ]

        has_law_abbrev = any(abbrev in citation_lower for abbrev in swiss_law_indicators)
        has_rs_sr = 'rs ' in citation_lower or 'sr ' in citation_lower or 'rs.' in citation_lower or 'sr.' in citation_lower

        # Also check for abbreviation patterns (same logic as fragment detection):
        # - All caps: StGB, ZGB, OR, BV (2+ consecutive uppercase)
        # - Mixed case: VwVG, SchKG, BauR (word with 2+ uppercase letters)
        has_uppercase_abbrev = bool(re.search(r'\b[A-ZÄÖÜ]{2,}\b', citation))  # All caps
        has_mixed_case_abbrev = bool(re.search(r'\b[A-ZÄÖÜ][a-zäöü]*[A-ZÄÖÜ]', citation))  # Mixed case

        is_article_only = (
            citation_lower.startswith('art') and
            len(citation) < 25 and
            not has_law_abbrev and
            not has_rs_sr and
            not has_uppercase_abbrev and
            not has_mixed_case_abbrev
        )

        if is_article_only:
            # Enrich it with the law reference
            enriched_citation = f"{citation} {law_ref}"
            enriched.append(enriched_citation)
            enrichment_count += 1
        else:
            enriched.append(citation)

    return enriched, enrichment_count

def process_csv(input_file, output_file, failures_file):
    """Process the CSV file and normalize citations."""

    import os

    # Ensure output directories exist
    os.makedirs('CSVs', exist_ok=True)
    os.makedirs('logs', exist_ok=True)

    stats = {
        'total_rows': 0,
        'articles_de_loi_rows': 0,
        'citations_processed': 0,
        'citations_changed': 0,
        'citations_removed_digit_only': 0,
        'citations_removed_garbage': 0,
        'citations_enriched': 0,
        'citations_kept': 0,
        'jurisprudence_kept': 0,
        'doctrine_kept': 0
    }

    failures = []
    removed_examples = []
    garbage_examples = []
    enriched_examples = []

    print(f"Reading {input_file}...")

    # Open transformations log file
    transformations_file = open('logs/citation_transformations.txt', 'w', encoding='utf-8')

    with open(input_file, 'r', encoding='utf-8') as infile, \
         open(output_file, 'w', encoding='utf-8', newline='') as outfile:

        reader = csv.DictReader(infile)
        # Exclude part_content from output (not needed after normalization)
        fieldnames = [f for f in reader.fieldnames if f != 'part_content']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()

        for row in reader:
            stats['total_rows'] += 1

            # Get part_content for debugging transformations
            part_content = row.get('part_content', '')

            # Parse the analysis JSON
            try:
                analysis = json.loads(row['analysis'])
            except (json.JSONDecodeError, KeyError):
                writer.writerow(row)
                continue

            # Only process "articles de loi"
            if 'articles de loi' not in analysis or not analysis['articles de loi']:
                # Keep jurisprudence and doctrine as-is
                if 'jurisprudence' in analysis and analysis['jurisprudence']:
                    stats['jurisprudence_kept'] += 1
                if 'doctrine' in analysis and analysis['doctrine']:
                    stats['doctrine_kept'] += 1
                writer.writerow(row)
                continue

            stats['articles_de_loi_rows'] += 1

            # Normalize each citation
            articles = analysis['articles de loi']
            if not isinstance(articles, list):
                writer.writerow(row)
                continue

            normalized_articles = []
            for citation in articles:
                stats['citations_processed'] += 1

                normalized, changed, is_digit_only = normalize_citation(citation)

                # Skip digit-only citations (e.g., "125", "3.14")
                if is_digit_only:
                    stats['citations_removed_digit_only'] += 1
                    if len(removed_examples) < 20:  # Keep first 20 examples
                        removed_examples.append(citation)
                    continue

                # Skip garbage citations (repealed markers, fragments, dates, etc.)
                is_garbage, garbage_reason = is_garbage_citation(normalized)
                if is_garbage:
                    stats['citations_removed_garbage'] += 1
                    if len(garbage_examples) < 20:  # Keep first 20 examples
                        garbage_examples.append(f"{citation} ({garbage_reason})")
                    continue

                normalized_articles.append(normalized)
                stats['citations_kept'] += 1

                if changed:
                    stats['citations_changed'] += 1
                    # Log the transformation with part_content for debugging
                    transformations_file.write(f"{citation} | {normalized} | {part_content}\n")

                # Track citations that are still just fragments (might need context)
                # These are very short citations without clear law references
                # Common Swiss law abbreviations that should NOT be flagged as fragments
                known_abbrevs = [
                    'rs ', 'sr ', 'constitution', 'verfassung', 'costituzione',
                    # Constitution abbreviations in all languages
                    'cst.', 'cste.', 'cst', 'cste', 'but', 'bv', 'cost.', 'cost',
                    # Common federal laws
                    'stgb', 'zgb', 'or', 'svg', 'schkg', 'zpo', 'stpo',
                    'uvg', 'ahvg', 'kvg', 'bvg', 'avig', 'urg', 'dsg',
                    'usg', 'mwstg', 'dbg', 'vesr', 'fiskalg'
                ]

                # Only flag as short_fragment if < 15 chars AND no known abbreviations
                if len(normalized.strip()) < 15:
                    has_known_abbrev = any(abbrev in normalized.lower() for abbrev in known_abbrevs)
                    # Check for abbreviation patterns:
                    # - All caps: StGB, ZGB, OR, BV (2+ consecutive uppercase)
                    # - Mixed case: VwVG, SchKG (word with 2+ uppercase letters)
                    has_uppercase_abbrev = bool(re.search(r'\b[A-ZÄÖÜ]{2,}\b', normalized))  # All caps
                    has_mixed_case_abbrev = bool(re.search(r'\b[A-ZÄÖÜ][a-zäöü]*[A-ZÄÖÜ]', normalized))  # Mixed case

                    if not has_known_abbrev and not has_uppercase_abbrev and not has_mixed_case_abbrev:
                        failures.append({
                            'uuid': row['uuid'],
                            'part_number': row.get('part_number', '0'),
                            'original': citation,
                            'normalized': normalized,
                            'reason': 'short_fragment'
                        })

            # Apply context enrichment: associate article-only citations with law references
            enriched_articles, enriched_count = enrich_article_only_citations(normalized_articles)
            stats['citations_enriched'] += enriched_count

            # Keep examples of enrichments and log all transformations
            if enriched_count > 0:
                for orig, enriched in zip(normalized_articles, enriched_articles):
                    if orig != enriched:
                        # Log the enrichment transformation with part_content for debugging
                        transformations_file.write(f"{orig} | {enriched} | {part_content}\n")
                        # Keep examples for display
                        if len(enriched_examples) < 20:
                            enriched_examples.append(f"{orig} → {enriched}")
                            if len(enriched_examples) >= 20:
                                break

            # Update the analysis with normalized and enriched citations
            analysis['articles de loi'] = enriched_articles
            row['analysis'] = json.dumps(analysis, ensure_ascii=False)

            writer.writerow(row)

            if stats['total_rows'] % 1000 == 0:
                print(f"  Processed {stats['total_rows']} rows...")

    # Close transformations file
    transformations_file.close()

    # Write failures
    print(f"\nWriting failures to {failures_file}...")
    with open(failures_file, 'w', encoding='utf-8') as f:
        for failure in failures:
            f.write(json.dumps(failure, ensure_ascii=False) + '\n')

    # Print statistics
    print("\n" + "="*80)
    print("📊 PREPROCESSING COMPLETE")
    print("="*80)

    print("\n┌─ INPUT PROCESSING ────────────────────────────────────────────────────────┐")
    print(f"│ Total rows processed:              {stats['total_rows']:>8,}                       │")
    print(f"│   • Rows with 'articles de loi':   {stats['articles_de_loi_rows']:>8,}                       │")
    print(f"│   • Rows with jurisprudence:       {stats['jurisprudence_kept']:>8,}                       │")
    print(f"│   • Rows with doctrine:            {stats['doctrine_kept']:>8,}                       │")
    print("└───────────────────────────────────────────────────────────────────────────┘")

    print("\n┌─ CITATION NORMALIZATION ──────────────────────────────────────────────────┐")
    print(f"│ Total citations processed:          {stats['citations_processed']:>8,}                       │")
    print(f"│   • Citations kept (valid):         {stats['citations_kept']:>8,}  ({stats['citations_kept']/max(stats['citations_processed'],1)*100:>5.1f}%)          │")
    print(f"│   • Citations removed (digit-only):  {stats['citations_removed_digit_only']:>7,}  ({stats['citations_removed_digit_only']/max(stats['citations_processed'],1)*100:>5.1f}%)          │")
    print(f"│   • Citations removed (garbage):     {stats['citations_removed_garbage']:>7,}  ({stats['citations_removed_garbage']/max(stats['citations_processed'],1)*100:>5.1f}%)          │")
    print(f"│   • Citations changed (normalized):  {stats['citations_changed']:>7,}  ({stats['citations_changed']/max(stats['citations_processed'],1)*100:>5.1f}%)          │")
    print(f"│   • Citations enriched (context):    {stats['citations_enriched']:>7,}  ({stats['citations_enriched']/max(stats['citations_processed'],1)*100:>5.1f}%)          │")
    print("└───────────────────────────────────────────────────────────────────────────┘")

    print("\n┌─ NORMALIZATION PATTERNS APPLIED ──────────────────────────────────────────┐")
    print("│ ✓ Fixed 'aCP' → 'a CP' patterns (missing space after 'a')                │")
    print("│ ✓ Fixed 'aBauR' → 'a BauR' patterns (mixed-case abbreviations)           │")
    print("│ ✓ Fixed 'Art.6VwVG' → 'Art. 6 VwVG' (missing spaces)                     │")
    print("│ ✓ Stripped trailing footnote digits (e.g., 'administration3')            │")
    print("│ ✓ Removed digit-only citations (e.g., '125', '3.14')                     │")
    print("│ ✓ Removed garbage citations (dates, fragments, proposals, etc.)          │")
    print("│ ✓ Enriched article-only with context (e.g., 'art. 128' + law from text)  │")
    print("└───────────────────────────────────────────────────────────────────────────┘")

    if enriched_examples:
        print("\n┌─ EXAMPLES OF ENRICHED CITATIONS (context-based) ─────────────────────────┐")
        for i, example in enumerate(enriched_examples[:10], 1):
            print(f"│ {i:2d}. {example:<71} │")
        if len(enriched_examples) > 10:
            print(f"│     ... and {len(enriched_examples) - 10} more                                                      │")
        print("└───────────────────────────────────────────────────────────────────────────┘")

    if removed_examples:
        print("\n┌─ EXAMPLES OF REMOVED CITATIONS (digit-only) ─────────────────────────────┐")
        for i, example in enumerate(removed_examples[:10], 1):
            print(f"│ {i:2d}. {example:<71} │")
        if len(removed_examples) > 10:
            print(f"│     ... and {len(removed_examples) - 10} more                                                      │")
        print("└───────────────────────────────────────────────────────────────────────────┘")

    if garbage_examples:
        print("\n┌─ EXAMPLES OF REMOVED CITATIONS (garbage) ────────────────────────────────┐")
        for i, example in enumerate(garbage_examples[:10], 1):
            # Truncate long examples to fit
            display_example = example if len(example) <= 71 else example[:68] + "..."
            print(f"│ {i:2d}. {display_example:<71} │")
        if len(garbage_examples) > 10:
            print(f"│     ... and {len(garbage_examples) - 10} more                                                      │")
        print("└───────────────────────────────────────────────────────────────────────────┘")

    total_transformations = stats['citations_changed'] + stats['citations_enriched']
    print("\n┌─ OUTPUT FILES ────────────────────────────────────────────────────────────┐")
    print(f"│ ✓ {output_file:<73} │")
    print(f"│ ✓ {failures_file:<73} │")
    print(f"│   - Short fragments logged: {len(failures):>6,}                                      │")
    print(f"│ ✓ logs/citation_transformations.txt                                        │")
    print(f"│   - Transformations logged: {total_transformations:>6,} (normalized + enriched)            │")
    print("└───────────────────────────────────────────────────────────────────────────┘")

    print("\n" + "="*80)

if __name__ == "__main__":
    process_csv(
        input_file="CSVs/data_filtered.csv",
        output_file="CSVs/data_filtered_citations_changed.csv",
        failures_file="logs/preprocessing_failures.jsonl"
    )
