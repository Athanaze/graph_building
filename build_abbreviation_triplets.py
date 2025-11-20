#!/usr/bin/env python3
"""
Build a mapping of RS numbers to abbreviations in all three languages (FR, DE, IT).
Uses the fedlex dataset from HuggingFace.
"""
import json
from datasets import load_dataset
from collections import defaultdict

def build_abbreviation_triplets():
    """
    Load fedlex dataset and create a mapping of RS numbers to language-specific abbreviations.
    """
    print("Loading fedlex dataset from HuggingFace...")
    dataset = load_dataset("liechticonsulting/fedlex", split="train")

    print(f"Loaded {len(dataset)} rows")
    print("\nBuilding abbreviation triplets...")

    # Structure: {rs_number: {language: abbreviation}}
    rs_to_abbrevs = defaultdict(dict)

    for i, row in enumerate(dataset):
        rs_number = row.get('RS_number')
        abbreviation = row.get('Abbreviation')
        language = row.get('Language')

        # Skip if any required field is missing
        if not rs_number or not language:
            continue

        # Skip RS numbers starting with 0 (as done in the original script)
        if rs_number.startswith('0'):
            continue

        # Skip RS numbers with pattern: number.number.number (3 parts)
        parts = rs_number.split('.')
        if len(parts) == 3 and all(part.isdigit() for part in parts):
            continue

        # Only store if abbreviation exists and is not null
        if abbreviation and abbreviation.strip():
            # Store the abbreviation for this language
            # If multiple abbreviations exist for same RS+language, keep the first one
            if language not in rs_to_abbrevs[rs_number]:
                rs_to_abbrevs[rs_number][language] = abbreviation.strip()

        if (i + 1) % 10000 == 0:
            print(f"  Processed {i + 1}/{len(dataset)} rows")

    # Convert to regular dict for JSON serialization
    result = {rs: dict(langs) for rs, langs in rs_to_abbrevs.items()}

    # Statistics
    print(f"\n{'='*60}")
    print(f"Statistics:")
    print(f"  Total unique RS numbers: {len(result):,}")

    # Count how many have all 3 languages
    complete_triplets = sum(1 for langs in result.values()
                           if 'FR' in langs and 'DE' in langs and 'IT' in langs)
    print(f"  RS numbers with all 3 languages (FR, DE, IT): {complete_triplets:,}")

    # Count by language coverage
    lang_counts = defaultdict(int)
    for langs in result.values():
        lang_counts[len(langs)] += 1

    print(f"\n  Coverage by number of languages:")
    for num_langs in sorted(lang_counts.keys()):
        print(f"    {num_langs} language(s): {lang_counts[num_langs]:,} RS numbers")

    # Show which languages appear
    all_languages = set()
    for langs in result.values():
        all_languages.update(langs.keys())
    print(f"\n  Languages found: {sorted(all_languages)}")

    return result

def save_abbreviation_triplets(data, output_file):
    """Save the abbreviation triplets to a JSON file."""
    print(f"\nSaving to {output_file}...")

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"✓ Saved {len(data):,} RS numbers to {output_file}")

def show_examples(data, num_examples=10):
    """Show some examples of the data."""
    print(f"\nExample entries (first {num_examples}):")
    print("="*60)

    for i, (rs_number, langs) in enumerate(list(data.items())[:num_examples]):
        print(f"\nRS {rs_number}:")
        for lang, abbrev in sorted(langs.items()):
            print(f"  {lang}: {abbrev}")

if __name__ == "__main__":
    # Build the mapping
    abbreviation_data = build_abbreviation_triplets()

    # Save to file
    output_file = "abbreviation_triplets.json"
    save_abbreviation_triplets(abbreviation_data, output_file)

    # Show examples
    show_examples(abbreviation_data, num_examples=10)

    print(f"\n{'='*60}")
    print("✓ Done!")
