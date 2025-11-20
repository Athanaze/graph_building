#!/usr/bin/env python3
"""
Analyze unparseable citations to identify patterns that should be filtered.
"""
import json
import csv
from collections import defaultdict, Counter

def load_unparseable():
    """Load unparseable citations."""
    citations = []
    with open('logs/unparseable_citations_preprocessed.jsonl', 'r', encoding='utf-8') as f:
        for line in f:
            citations.append(json.loads(line))
    return citations

def categorize_citation(citation):
    """Categorize an unparseable citation to determine if it's garbage or rescuable."""
    citation_lower = citation.lower()

    # Category 1: Repealed/abrogated articles (not real citations)
    if 'abrogé' in citation_lower or 'aufgehoben' in citation_lower or 'abrogato' in citation_lower:
        return 'GARBAGE_REPEALED'

    # Category 2: Just paragraph/letter markers without article
    if citation.startswith(('Abs. ', 'Al. ', 'al. ', 'lit. ', 'let. ', 'Lit. ', 'Let. ')):
        return 'GARBAGE_FRAGMENT'

    # Category 3: Dates (not citations)
    if any(month in citation for month in ['Januar', 'Februar', 'März', 'April', 'Mai', 'Juni',
                                            'Juli', 'August', 'September', 'Oktober', 'November', 'Dezember',
                                            'janvier', 'février', 'mars', 'avril', 'mai', 'juin',
                                            'juillet', 'août', 'septembre', 'octobre', 'novembre', 'décembre']):
        return 'GARBAGE_DATE'

    # Category 4: Page references (207ff., 218f., etc.)
    if citation.endswith(('ff.', 'f.', 'ff', 'f')) and any(c.isdigit() for c in citation):
        return 'GARBAGE_PAGE_REF'

    # Category 5: Incomplete sentences/fragments (ends with "de la", "du", "des", etc.)
    if citation.endswith(('de la', 'du', 'des', 'de l\'', 'della', 'del', 'ist (', 'sind (')):
        return 'GARBAGE_INCOMPLETE'

    # Category 6: Proposal/motion markers (not actual law)
    if any(word in citation_lower for word in ['proposition', 'motion', 'postulat', 'initiative']):
        return 'GARBAGE_PROPOSAL'

    # Category 7: Article-only citations (potentially rescuable with context enrichment)
    if citation.startswith(('Art. ', 'art. ', 'Art ', 'art ')):
        # Check if it's really article-only (no clear law reference)
        if len(citation) < 30:  # Short citations are likely article-only
            return 'RESCUABLE_ARTICLE_ONLY'

    # Category 8: References to non-Swiss laws (cantonal regulations, foreign laws)
    if any(word in citation_lower for word in ['reglement', 'règlement', 'statut', 'ordonnance communale',
                                                  'feuerwehrreglement', 'öffentlichkeitsgesetz']):
        return 'NON_SWISS_LAW'

    # Category 9: Everything else - needs manual review
    return 'UNKNOWN'

def main():
    citations = load_unparseable()
    print(f"Total unparseable citations: {len(citations)}")

    # Categorize all citations
    categories = Counter()
    by_category = defaultdict(list)

    for item in citations:
        citation = item['citation']
        category = categorize_citation(citation)
        categories[category] += 1
        if len(by_category[category]) < 10:  # Keep first 10 examples
            by_category[category].append(citation)

    # Print statistics
    print("\n" + "="*80)
    print("CATEGORIZATION RESULTS")
    print("="*80)

    total_garbage = sum(count for cat, count in categories.items() if cat.startswith('GARBAGE_'))
    total_rescuable = sum(count for cat, count in categories.items() if cat.startswith('RESCUABLE_'))

    print(f"\nTotal GARBAGE (should be filtered): {total_garbage} ({total_garbage/len(citations)*100:.1f}%)")
    print(f"Total RESCUABLE: {total_rescuable} ({total_rescuable/len(citations)*100:.1f}%)")
    print(f"NON_SWISS_LAW: {categories['NON_SWISS_LAW']} ({categories['NON_SWISS_LAW']/len(citations)*100:.1f}%)")
    print(f"UNKNOWN (needs review): {categories['UNKNOWN']} ({categories['UNKNOWN']/len(citations)*100:.1f}%)")

    print("\n" + "-"*80)
    print("BREAKDOWN BY CATEGORY")
    print("-"*80)

    for category, count in sorted(categories.items(), key=lambda x: -x[1]):
        pct = count / len(citations) * 100
        print(f"\n{category}: {count} ({pct:.1f}%)")
        print("Examples:")
        for i, ex in enumerate(by_category[category][:5], 1):
            print(f"  {i}. {ex}")

    # Identify citations to filter
    print("\n" + "="*80)
    print("FILTERING RECOMMENDATION")
    print("="*80)

    garbage_categories = [cat for cat in categories.keys() if cat.startswith('GARBAGE_')]
    print(f"\nShould filter these categories: {', '.join(garbage_categories)}")
    print(f"Total citations to filter: {total_garbage}")

    # Write filtered citations to file
    filtered_citations = []
    for item in citations:
        citation = item['citation']
        category = categorize_citation(citation)
        if category.startswith('GARBAGE_'):
            filtered_citations.append({
                'citation': citation,
                'category': category,
                'element_id': item['element_id']
            })

    with open('logs/garbage_citations_to_filter.jsonl', 'w', encoding='utf-8') as f:
        for item in filtered_citations:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')

    print(f"\n✓ Wrote {len(filtered_citations)} garbage citations to logs/garbage_citations_to_filter.jsonl")

if __name__ == "__main__":
    main()
