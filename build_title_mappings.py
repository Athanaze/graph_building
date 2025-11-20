#!/usr/bin/env python3
"""
Build mappings from law titles to RS numbers.
This allows matching citations that use full law names instead of abbreviations.
"""
import json
from datasets import load_dataset
from collections import defaultdict

print("Loading fedlex dataset...")
dataset = load_dataset("liechticonsulting/fedlex", split="train")

print(f"Loaded {len(dataset)} rows")
print("\nBuilding title mappings...")

# Structure: {rs_number: {language: title}}
titles_by_rs = defaultdict(dict)

# Also create reverse mapping: title -> RS number
# We'll store normalized titles (lowercase, strip extra spaces)
title_to_rs = {}

for i, row in enumerate(dataset):
    rs_number = row.get('RS_number')
    title = row.get('Title')
    language = row.get('Language')

    if not rs_number or not title or not language:
        continue

    # Skip RS numbers starting with 0 (as done before)
    if rs_number.startswith('0'):
        continue

    # Skip RS numbers with 3 parts (e.g., 123.456.789)
    parts = rs_number.split('.')
    if len(parts) == 3 and all(part.isdigit() for part in parts):
        continue

    # Store title by RS and language
    if language not in titles_by_rs[rs_number]:
        titles_by_rs[rs_number][language] = title

    # Create normalized version for reverse lookup
    # Normalize: lowercase, remove extra spaces, remove punctuation at end
    normalized_title = ' '.join(title.lower().strip().split())
    normalized_title = normalized_title.rstrip('.,;:')

    # Store reverse mapping
    if normalized_title not in title_to_rs:
        title_to_rs[normalized_title] = rs_number

    if (i + 1) % 1000 == 0:
        print(f"  Processed {i + 1}/{len(dataset)} rows")

# Convert to regular dicts
titles_mapping = {
    "titles_by_rs": dict(titles_by_rs),
    "title_to_rs": title_to_rs
}

# Statistics
print(f"\nStatistics:")
print(f"  RS numbers with titles: {len(titles_by_rs)}")
print(f"  Total normalized titles: {len(title_to_rs)}")

# Count by language
lang_counts = defaultdict(int)
for rs_titles in titles_by_rs.values():
    for lang in rs_titles.keys():
        lang_counts[lang] += 1

print(f"\n  Titles by language:")
for lang in sorted(lang_counts.keys()):
    print(f"    {lang}: {lang_counts[lang]}")

# Save to JSON
output_file = "titles_mapping.json"
print(f"\nSaving to {output_file}...")

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(titles_mapping, f, ensure_ascii=False, indent=2)

print(f"✓ Saved to {output_file}")

# Show some examples
print(f"\nExample titles (first 5):")
for i, (rs, titles) in enumerate(list(titles_by_rs.items())[:5], 1):
    print(f"\n{i}. RS {rs}:")
    for lang, title in titles.items():
        print(f"   {lang}: {title[:80]}{'...' if len(title) > 80 else ''}")
