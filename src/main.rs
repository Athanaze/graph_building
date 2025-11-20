use ahash::{AHashMap, AHashSet};
use once_cell::sync::Lazy;
use rayon::prelude::*;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

mod context_lookup;

// ============================================================================
// TYPES
// ============================================================================

#[derive(Debug, Deserialize)]
struct DatasetRow {
    uuid: String,
    part_number: Option<String>,
    analysis: Option<String>,
    part_content: Option<String>,
}

#[derive(Debug, Clone)]
struct Element {
    id: String,
    articles_de_loi: Vec<String>,
    part_content: String,
}

#[derive(Debug, Deserialize)]
struct Analysis {
    #[serde(rename = "articles de loi")]
    articles_de_loi: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
struct CitationInfo {
    element_id: String,
    citation: String,
    law: String,
    articles: AHashSet<u32>,
}

#[derive(Debug, Serialize)]
struct UnparseableCitation {
    element_id: String,
    citation: String,
    extracted_abbrev: Option<String>,
    reason: String,
}

#[derive(Debug, Serialize)]
struct CitationAnalysis {
    citation1: String,
    citation2: String,
    same_law: bool,
    same_article: bool,
    law1: Option<String>,
    law2: Option<String>,
    articles1: Vec<u32>,
    articles2: Vec<u32>,
    overlapping_articles: Vec<u32>,
}

#[derive(Debug, Serialize)]
struct OutputRecord {
    element1: String,
    element2: String,
    analysis: CitationAnalysis,
}

type AbbrevTriplets = HashMap<String, HashMap<String, String>>;
type AbbrevToRs = AHashMap<String, String>;

// ============================================================================
// REGEX PATTERNS (compiled once)
// ============================================================================

static RS_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bRS\s*(\d+(?:\.\d+)*)\b").unwrap()
});

static SR_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\bSR\s*(\d+(?:\.\d+)*)\b").unwrap()
});

static ABBREV_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\b([A-ZÄÖÜ][A-ZÄÖÜa-zäöü]{1,15})\b").unwrap()
});

static PAREN_ABBREV_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"\(([A-ZÄÖÜ][A-ZÄÖÜa-zäöü-]{1,15})\)").unwrap()
});

static ART_SS_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)[Aa]rt\.?\s*(\d+)\s*(?:ss|ff|sqq?)").unwrap()
});

static ART_RANGE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)[Aa]rt\.?\s*(\d+)\s*(?:-|à|bis)\s*(\d+)").unwrap()
});

static ART_SIMPLE_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)[Aa]rt\.?\s*(\d+)").unwrap()
});

static COMMON_WORDS: Lazy<AHashSet<&'static str>> = Lazy::new(|| {
    [
        // Article markers
        "art", "artikel", "article",
        // Structural markers
        "abs", "al", "lit", "let", "ch", "bst", "ziff", "satz", "anhang",
        // Common words
        "du", "de", "vom", "der", "des", "und", "et", "bzw", "recte", "ff", "ss",
        // Generic terms
        "antrag", "verordnung", "gesetzes", "loi", "constitution", "convention",
        "conseil", "proposition", "tribunal", "gegen", "für", "über",
        // Months (often picked up)
        "januar", "februar", "märz", "april", "mai", "juni", "juli",
        "august", "september", "oktober", "november", "dezember",
        "janvier", "février", "mars", "avril", "mai", "juin", "juillet",
        "août", "septembre", "octobre", "novembre", "décembre",
        // Other
        "le", "la", "les", "planungs", "baureglements",
    ]
        .iter()
        .copied()
        .collect()
});

// ============================================================================
// ABBREVIATION HANDLING
// ============================================================================

fn normalize_abbreviation(abbrev: &str) -> String {
    abbrev.to_lowercase().replace('.', "").trim().to_string()
}

fn load_abbreviation_triplets(path: &str) -> Result<(AbbrevTriplets, AbbrevToRs), Box<dyn std::error::Error>> {
    println!("Loading abbreviation triplets from {}...", path);
    let file = File::open(path)?;
    let triplets: AbbrevTriplets = serde_json::from_reader(file)?;

    let mut abbrev_to_rs = AHashMap::new();
    for (rs_number, langs) in &triplets {
        for (_lang, abbrev) in langs {
            let normalized = normalize_abbreviation(abbrev);
            abbrev_to_rs.entry(normalized).or_insert_with(|| rs_number.clone());
        }
    }

    println!("  ✓ Loaded {} RS numbers", triplets.len());
    println!("  ✓ Mapped {} abbreviations", abbrev_to_rs.len());
    Ok((triplets, abbrev_to_rs))
}

// ============================================================================
// CITATION PARSING
// ============================================================================

fn extract_law_abbreviation(citation: &str) -> Option<String> {
    // First try RS/SR patterns
    if let Some(caps) = RS_PATTERN.captures(citation) {
        return Some(caps[1].to_string());
    }
    if let Some(caps) = SR_PATTERN.captures(citation) {
        return Some(caps[1].to_string());
    }

    // Check for Constitution references (in 3 languages + abbreviations)
    let citation_lower = citation.to_lowercase();
    if citation_lower.contains("constitution") ||
       citation_lower.contains("verfassung") ||
       citation_lower.contains("costituzione") {
        return Some("Cst.".to_string());
    }

    // Check for Constitution abbreviations: Cst./Cste. (FR), BV (DE), Cost. (IT), But (variant)
    for caps in PAREN_ABBREV_PATTERN.captures_iter(citation) {
        let abbrev = caps[1].to_string();
        let abbrev_lower = abbrev.to_lowercase();
        if abbrev_lower == "cst" || abbrev_lower == "cste" ||
           abbrev_lower == "bv" || abbrev_lower == "cost" ||
           abbrev_lower == "but" {
            return Some("Cst.".to_string());
        }
    }

    // Also check without parentheses
    for caps in ABBREV_PATTERN.captures_iter(citation) {
        let abbrev = caps[1].to_string();
        let abbrev_lower = abbrev.to_lowercase();
        if abbrev_lower == "cst" || abbrev_lower == "cste" ||
           abbrev_lower == "bv" || abbrev_lower == "cost" ||
           abbrev_lower == "but" {
            return Some("Cst.".to_string());
        }
    }

    // Try to find abbreviation in parentheses (high priority)
    // e.g., "(BGG)", "(StPO)", "(WUB, BS 6 173)"
    for caps in PAREN_ABBREV_PATTERN.captures_iter(citation) {
        let abbrev = caps[1].to_string();
        if !COMMON_WORDS.contains(abbrev.to_lowercase().as_str()) {
            return Some(abbrev);
        }
    }

    // Fall back to general pattern (look for capitalized words)
    for caps in ABBREV_PATTERN.captures_iter(citation) {
        let abbrev = caps[1].to_string();
        if !COMMON_WORDS.contains(abbrev.to_lowercase().as_str()) {
            return Some(abbrev);
        }
    }

    None
}

fn normalize_to_rs_number(abbrev: &str, abbrev_to_rs: &AbbrevToRs) -> Option<String> {
    // If it's already an RS number, return it
    if abbrev.chars().all(|c| c.is_numeric() || c == '.') {
        return Some(abbrev.to_string());
    }

    // Otherwise look it up
    let normalized = normalize_abbreviation(abbrev);
    abbrev_to_rs.get(&normalized).cloned()
}

fn extract_article_numbers(citation: &str) -> AHashSet<u32> {
    let mut articles = AHashSet::new();

    for caps in ART_SS_PATTERN.captures_iter(citation) {
        if let Ok(start) = caps[1].parse::<u32>() {
            for num in start..=(start + 10) {
                articles.insert(num);
            }
        }
    }

    for caps in ART_RANGE_PATTERN.captures_iter(citation) {
        if let (Ok(start), Ok(end)) = (caps[1].parse::<u32>(), caps[2].parse::<u32>()) {
            if start <= end {
                for num in start..=end {
                    articles.insert(num);
                }
            }
        }
    }

    for caps in ART_SIMPLE_PATTERN.captures_iter(citation) {
        if let Ok(num) = caps[1].parse::<u32>() {
            articles.insert(num);
        }
    }

    articles
}

// ============================================================================
// DATASET LOADING
// ============================================================================

fn load_and_filter_dataset(path: &str) -> Result<Vec<Element>, Box<dyn std::error::Error>> {
    println!("\n📂 Loading dataset from {}...", path);

    // Determine file type by extension
    if path.ends_with(".csv") {
        load_from_csv(path)
    } else {
        load_from_jsonl(path)
    }
}

fn load_from_csv(path: &str) -> Result<Vec<Element>, Box<dyn std::error::Error>> {
    let mut rdr = csv::Reader::from_path(path)?;

    let mut elements = Vec::new();
    let mut total = 0;

    for result in rdr.records() {
        total += 1;
        let record = result?;

        // CSV columns:
        // Original: uuid, part_number, part_content, n_char, arbitrary_chunked, analysis (6 cols)
        // Preprocessed: uuid, part_number, n_char, arbitrary_chunked, analysis (5 cols)
        let uuid = record.get(0).unwrap_or("");
        let part_number = record.get(1).unwrap_or("0");
        let id = format!("{}_{}", uuid, part_number);

        // Detect CSV format by column count and get analysis column
        let analysis_str = if record.len() >= 6 {
            // Original format: analysis at column 5
            record.get(5).unwrap_or("{}")
        } else {
            // Preprocessed format: analysis at column 4
            record.get(4).unwrap_or("{}")
        };

        // part_content not needed for analysis (set to empty)
        let part_content = String::new();

        if let Ok(analysis) = serde_json::from_str::<serde_json::Value>(&analysis_str) {
            if let Some(articles) = analysis.get("articles de loi").and_then(|v| v.as_array()) {
                let articles: Vec<String> = articles
                    .iter()
                    .filter_map(|v| v.as_str())
                    .filter(|s| !s.trim().is_empty())
                    .map(|s| s.to_string())
                    .collect();

                if !articles.is_empty() {
                    elements.push(Element {
                        id,
                        articles_de_loi: articles,
                        part_content,
                    });
                }
            }
        }

        if total % 5000 == 0 {
            print!("\r  Scanned {} rows, found {} with articles...", total, elements.len());
            std::io::stdout().flush().ok();
        }
    }

    println!("\r  ✓ Total rows: {}", total);
    println!("  ✓ Elements with 'articles de loi': {}", elements.len());
    Ok(elements)
}

fn load_from_jsonl(path: &str) -> Result<Vec<Element>, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let reader = std::io::BufReader::new(file);

    let mut elements = Vec::new();
    let mut total = 0;

    for line in std::io::BufRead::lines(reader) {
        total += 1;
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }

        let row: DatasetRow = match serde_json::from_str(&line) {
            Ok(r) => r,
            Err(_) => continue,
        };

        if let Some(analysis_str) = row.analysis {
            if let Ok(analysis) = serde_json::from_str::<Analysis>(&analysis_str) {
                if let Some(articles) = analysis.articles_de_loi {
                    let articles: Vec<_> = articles
                        .into_iter()
                        .filter(|a| !a.trim().is_empty())
                        .collect();

                    if !articles.is_empty() {
                        let part = row.part_number.unwrap_or_else(|| "0".to_string());
                        let id = format!("{}_{}", row.uuid, part);
                        let part_content = row.part_content.unwrap_or_default();
                        elements.push(Element {
                            id,
                            articles_de_loi: articles,
                            part_content,
                        });
                    }
                }
            }
        }

        if total % 5000 == 0 {
            print!("\r  Scanned {} rows, found {} with articles...", total, elements.len());
            std::io::stdout().flush().ok();
        }
    }

    println!("\r  ✓ Total rows: {}", total);
    println!("  ✓ Elements with 'articles de loi': {}", elements.len());
    Ok(elements)
}

// ============================================================================
// PHASE 1: GROUP BY LAW
// ============================================================================

fn load_titles_mapping() -> Option<HashMap<String, String>> {
    if let Ok(file) = std::fs::File::open("titles_mapping.json") {
        if let Ok(data) = serde_json::from_reader::<_, serde_json::Value>(file) {
            // The JSON has a "title_to_rs" key with the mapping
            if let Some(title_to_rs_obj) = data.get("title_to_rs").and_then(|v| v.as_object()) {
                let mut title_to_rs = HashMap::new();
                for (title, rs_val) in title_to_rs_obj {
                    if let Some(rs_number) = rs_val.as_str() {
                        title_to_rs.insert(title.clone(), rs_number.to_string());
                    }
                }
                return Some(title_to_rs);
            }
        }
    }
    None
}

fn normalize_title_for_matching(text: &str) -> String {
    // Remove parenthetical expressions like (PA), (SR 123.45)
    let re = regex::Regex::new(r"\([^)]*\)").unwrap();
    let without_parens = re.replace_all(text, "");

    without_parens
        .to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(&['.', ',', ';', ':'][..])
        .to_string()
}

fn find_law_by_title_direct(citation: &str, title_to_rs: &HashMap<String, String>) -> Option<String> {
    let normalized_citation = normalize_title_for_matching(citation);

    // Try exact match first
    if let Some(rs) = title_to_rs.get(&normalized_citation) {
        return Some(rs.clone());
    }

    // Extract key words from citation (words longer than 4 chars, excluding common ones)
    let common_words = [
        "loi", "ordonnance", "décret", "arrêté", "règlement", "gesetz",
        "verordnung", "beschluss", "bundesgesetz", "legge", "ordinanza",
        "decreto", "fédérale", "federale", "suisse", "svizzera", "schweiz",
        "concernant", "betreffend", "concerning", "über", "sulla", "sur",
        "pour", "dans", "avec", "même", "ainsi", "aussi", "dans", "fédéral",
        "federal", "vom", "della", "del", "sulla", "relative", "relatif",
        "relative", "relativa", "relativi"
    ];

    let citation_words: Vec<&str> = normalized_citation
        .split_whitespace()
        .filter(|w| w.len() > 4 && !common_words.contains(w))
        .collect();

    // Need at least 2 distinctive words for matching
    if citation_words.len() < 2 {
        return None;
    }

    // Try fuzzy matching
    let mut best_match: Option<(String, usize)> = None;

    for (title, rs) in title_to_rs.iter() {
        // Skip very short titles
        if title.len() < 20 {
            continue;
        }

        let matching_words = citation_words.iter()
            .filter(|&&word| title.contains(word))
            .count();

        // For short citations (2-3 words), require all words to match
        // For longer citations, require at least 70% match
        let required_matches = if citation_words.len() <= 3 {
            citation_words.len()
        } else {
            ((citation_words.len() as f64 * 0.7).ceil() as usize).max(2)
        };

        if matching_words >= required_matches {
            if let Some((_, prev_count)) = best_match {
                if matching_words > prev_count {
                    best_match = Some((rs.clone(), matching_words));
                }
            } else {
                best_match = Some((rs.clone(), matching_words));
            }
        }
    }

    best_match.map(|(rs, _)| rs)
}

fn group_citations_by_law(
    elements: &[Element],
    abbrev_to_rs: &AbbrevToRs,
) -> (AHashMap<String, Vec<CitationInfo>>, Vec<UnparseableCitation>) {
    println!("\n🗂️  Phase 1: Grouping citations by law...");

    // Load title mappings for direct title matching
    let title_to_rs = load_titles_mapping();
    if let Some(ref mapping) = title_to_rs {
        println!("  ✓ Loaded {} law titles for direct matching", mapping.len());
    }

    let mut law_groups: AHashMap<String, Vec<CitationInfo>> = AHashMap::new();
    let mut unparseable_list = Vec::new();
    let mut total_citations = 0;
    let mut unparseable = 0;
    let mut matched_by_title = 0;

    for element in elements {
        for citation in &element.articles_de_loi {
            total_citations += 1;

            let law_abbrev_opt = extract_law_abbreviation(citation);

            if let Some(law_abbrev) = law_abbrev_opt.as_ref() {
                if let Some(rs_number) = normalize_to_rs_number(law_abbrev, abbrev_to_rs) {
                    // Federal law - use RS number
                    let articles = extract_article_numbers(citation);

                    law_groups.entry(rs_number.clone()).or_insert_with(Vec::new).push(CitationInfo {
                        element_id: element.id.clone(),
                        citation: citation.clone(),
                        law: rs_number,
                        articles,
                    });
                } else {
                    // Not in RS mapping - treat as cantonal/regional law
                    // Use "CANTONAL_" prefix to distinguish from federal laws
                    let cantonal_key = format!("CANTONAL_{}", law_abbrev.to_uppercase());
                    let articles = extract_article_numbers(citation);

                    law_groups.entry(cantonal_key.clone()).or_insert_with(Vec::new).push(CitationInfo {
                        element_id: element.id.clone(),
                        citation: citation.clone(),
                        law: cantonal_key,
                        articles,
                    });
                }
            } else {
                // No abbreviation found - try title matching
                if let Some(ref mapping) = title_to_rs {
                    if let Some(rs_number) = find_law_by_title_direct(citation, mapping) {
                        matched_by_title += 1;
                        let articles = extract_article_numbers(citation);

                        law_groups.entry(rs_number.clone()).or_insert_with(Vec::new).push(CitationInfo {
                            element_id: element.id.clone(),
                            citation: citation.clone(),
                            law: rs_number,
                            articles,
                        });
                        continue;
                    }
                }

                // Couldn't extract any abbreviation or match by title
                unparseable += 1;
                unparseable_list.push(UnparseableCitation {
                    element_id: element.id.clone(),
                    citation: citation.clone(),
                    extracted_abbrev: None,
                    reason: "no_abbreviation_found".to_string(),
                });
            }
        }
    }

    // Count federal vs cantonal laws
    let federal_laws = law_groups.keys().filter(|k| !k.starts_with("CANTONAL_")).count();
    let cantonal_laws = law_groups.keys().filter(|k| k.starts_with("CANTONAL_")).count();
    let parseable_citations = total_citations - unparseable;

    println!("  ✓ Total citations: {}", total_citations);
    println!("  ✓ Successfully parsed: {} ({:.1}%)", parseable_citations, 100.0 * parseable_citations as f64 / total_citations as f64);
    if matched_by_title > 0 {
        println!("    - Matched by title: {}", matched_by_title);
    }
    println!("  ✓ Unique laws cited: {}", law_groups.len());
    println!("    - Federal laws (RS): {}", federal_laws);
    println!("    - Cantonal/regional laws: {}", cantonal_laws);
    println!("  ✓ Unparseable citations: {} ({:.1}%)", unparseable, 100.0 * unparseable as f64 / total_citations as f64);

    // Print distribution statistics
    let mut group_sizes: Vec<usize> = law_groups.values().map(|v| v.len()).collect();
    group_sizes.sort_unstable();
    group_sizes.reverse();

    if !group_sizes.is_empty() {
        println!("\n📊 Distribution:");
        println!("  ✓ Largest group: {} citations", group_sizes[0]);
        println!("  ✓ Median group: {} citations", group_sizes[group_sizes.len() / 2]);
        println!("  ✓ Top 5 groups: {:?}", &group_sizes[..5.min(group_sizes.len())]);

        // Calculate expected comparisons
        let total_comparisons: usize = group_sizes.iter().map(|&n| n * (n - 1) / 2).sum();
        println!("  ✓ Expected pairwise comparisons: {}", format_number(total_comparisons));
    }

    (law_groups, unparseable_list)
}

// ============================================================================
// PHASE 2: WITHIN-GROUP COMPARISON
// ============================================================================

fn compare_within_groups(
    law_groups: AHashMap<String, Vec<CitationInfo>>,
    output_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n⚡ Phase 2: Comparing citations within each law group...");
    println!("  Using {} CPU cores\n", rayon::current_num_threads());

    // Calculate total comparisons for progress tracking
    let total_comparisons: usize = law_groups.values()
        .map(|citations| citations.len() * (citations.len() - 1) / 2)
        .sum();

    println!("  Total comparisons to perform: {}\n", format_number(total_comparisons));

    let completed = Arc::new(AtomicUsize::new(0));
    let same_article_count = Arc::new(AtomicUsize::new(0));
    let file = File::create(output_path)?;
    let writer = Arc::new(Mutex::new(BufWriter::new(file)));

    let start_time = Instant::now();
    let last_print = Arc::new(Mutex::new(Instant::now()));

    // Convert to vec for parallel iteration
    let groups: Vec<_> = law_groups.into_iter().collect();

    groups.par_iter().for_each(|(law, citations)| {
        let n = citations.len();

        for i in 0..n {
            for j in (i + 1)..n {
                let c1 = &citations[i];
                let c2 = &citations[j];

                // Skip if same element (element comparing with itself)
                if c1.element_id == c2.element_id {
                    continue;
                }

                // Check article overlap
                let overlap: AHashSet<_> = c1.articles.intersection(&c2.articles).copied().collect();
                let has_overlap = !overlap.is_empty();

                if has_overlap {
                    same_article_count.fetch_add(1, Ordering::Relaxed);
                }

                // Always write if same law (which they are, by construction)
                let mut arts1: Vec<_> = c1.articles.iter().copied().collect();
                let mut arts2: Vec<_> = c2.articles.iter().copied().collect();
                let mut overlap_vec: Vec<_> = overlap.iter().copied().collect();

                arts1.sort_unstable();
                arts2.sort_unstable();
                overlap_vec.sort_unstable();

                let analysis = CitationAnalysis {
                    citation1: c1.citation.clone(),
                    citation2: c2.citation.clone(),
                    same_law: true,
                    same_article: has_overlap,
                    law1: Some(law.clone()),
                    law2: Some(law.clone()),
                    articles1: arts1,
                    articles2: arts2,
                    overlapping_articles: overlap_vec,
                };

                let record = OutputRecord {
                    element1: c1.element_id.clone(),
                    element2: c2.element_id.clone(),
                    analysis,
                };

                // Write to output (thread-safe)
                if let Ok(json) = serde_json::to_string(&record) {
                    if let Ok(mut w) = writer.lock() {
                        let _ = writeln!(w, "{}", json);
                    }
                }

                // Update progress
                let current = completed.fetch_add(1, Ordering::Relaxed) + 1;

                // Print progress every 10 seconds
                if let Ok(mut last) = last_print.try_lock() {
                    let now = Instant::now();
                    if now.duration_since(*last) >= Duration::from_secs(10) {
                        *last = now;
                        let elapsed = start_time.elapsed().as_secs_f64();
                        let progress = 100.0 * current as f64 / total_comparisons as f64;
                        let rate = current as f64 / elapsed;
                        let remaining = (total_comparisons - current) as f64 / rate;
                        let same_art = same_article_count.load(Ordering::Relaxed);

                        println!(
                            "  Progress: {:>5.1}% | Matches: {:>6} ({:.1}%) | Rate: {:>8}/s | ETA: {}",
                            progress,
                            format_number(same_art),
                            100.0 * same_art as f64 / current as f64,
                            format_number(rate as usize),
                            format_duration(remaining as u64)
                        );
                    }
                }
            }
        }
    });

    // Flush writer
    if let Ok(mut w) = writer.lock() {
        w.flush()?;
    }

    let elapsed = start_time.elapsed();
    let total = completed.load(Ordering::Relaxed);
    let same_article = same_article_count.load(Ordering::Relaxed);

    println!("\n{}", "=".repeat(70));
    println!("✅ ANALYSIS COMPLETE!");
    println!("{}", "=".repeat(70));
    println!("  Total comparisons: {}", format_number(total));
    println!("  Same article matches: {} ({:.2}%)",
             format_number(same_article),
             100.0 * same_article as f64 / total.max(1) as f64);
    println!("  Time elapsed: {}", format_duration(elapsed.as_secs()));
    println!("  Average rate: {}/sec", format_number((total as f64 / elapsed.as_secs_f64()) as usize));
    println!("  Output file: {}", output_path);
    println!("{}", "=".repeat(70));

    Ok(())
}

// ============================================================================
// UTILITY FUNCTIONS
// ============================================================================

fn format_number(n: usize) -> String {
    let s = n.to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

fn format_signed(n: i64) -> String {
    let sign = if n >= 0 { "+" } else { "" };
    let s = n.abs().to_string();
    let mut result = String::new();
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    let num = result.chars().rev().collect::<String>();
    if n >= 0 {
        format!("+{}", num)
    } else {
        format!("-{}", num)
    }
}

fn format_duration(secs: u64) -> String {
    if secs < 60 {
        format!("{}s", secs)
    } else if secs < 3600 {
        format!("{}m {:02}s", secs / 60, secs % 60)
    } else {
        format!("{}h {:02}m {:02}s", secs / 3600, (secs % 3600) / 60, secs % 60)
    }
}

// ============================================================================
// ANALYSIS STATISTICS
// ============================================================================

#[derive(Debug)]
struct AnalysisStats {
    file_name: String,
    total_citations: usize,
    parsed_citations: usize,
    unparseable_citations: usize,
    unique_laws: usize,
    federal_laws: usize,
    cantonal_laws: usize,
    total_comparisons: usize,
    same_article_matches: usize,
}

impl AnalysisStats {
    fn parsing_rate(&self) -> f64 {
        100.0 * self.parsed_citations as f64 / self.total_citations.max(1) as f64
    }

    fn unparseable_rate(&self) -> f64 {
        100.0 * self.unparseable_citations as f64 / self.total_citations.max(1) as f64
    }

    fn match_rate(&self) -> f64 {
        100.0 * self.same_article_matches as f64 / self.total_comparisons.max(1) as f64
    }
}

fn run_analysis(
    input_file: &str,
    output_suffix: &str,
    abbrev_to_rs: &AbbrevToRs,
) -> Result<AnalysisStats, Box<dyn std::error::Error>> {
    println!("\n{}", "=".repeat(70));
    println!("📊 ANALYZING: {}", input_file);
    println!("{}", "=".repeat(70));

    // Load dataset
    let elements = load_and_filter_dataset(input_file)?;

    if elements.is_empty() {
        return Err("No elements found with 'articles de loi'".into());
    }

    // Phase 1: Group by law
    let (law_groups, unparseable_citations) = group_citations_by_law(&elements, abbrev_to_rs);
    let initial_unparseable = unparseable_citations.len();

    // Collect statistics
    let federal_laws = law_groups.keys().filter(|k| !k.starts_with("CANTONAL_")).count();
    let cantonal_laws = law_groups.keys().filter(|k| k.starts_with("CANTONAL_")).count();
    let parsed_citations: usize = law_groups.values().map(|v| v.len()).sum();
    let total_citations = parsed_citations + unparseable_citations.len();

    // Create logs directory if it doesn't exist
    std::fs::create_dir_all("logs").ok();

    // Save unparseable citations to file
    let unparseable_file_path = format!("logs/unparseable_citations_{}.jsonl", output_suffix);
    if !unparseable_citations.is_empty() {
        let unparseable_file = File::create(&unparseable_file_path)?;
        let mut writer = BufWriter::new(unparseable_file);

        for citation in &unparseable_citations {
            if let Ok(json) = serde_json::to_string(&citation) {
                writeln!(writer, "{}", json)?;
            }
        }
        writer.flush()?;
    }

    // Phase 2: Compare within groups
    let output_path = format!("logs/law_citation_matches_{}.jsonl", output_suffix);
    let (total_comparisons, same_article_matches) = compare_within_groups_stats(law_groups, &output_path)?;

    Ok(AnalysisStats {
        file_name: input_file.to_string(),
        total_citations,
        parsed_citations,
        unparseable_citations: unparseable_citations.len(),
        unique_laws: federal_laws + cantonal_laws,
        federal_laws,
        cantonal_laws,
        total_comparisons,
        same_article_matches,
    })
}

fn compare_within_groups_stats(
    law_groups: AHashMap<String, Vec<CitationInfo>>,
    output_path: &str,
) -> Result<(usize, usize), Box<dyn std::error::Error>> {
    println!("\n⚡ Phase 2: Comparing citations within each law group...");
    println!("  Using {} CPU cores\n", rayon::current_num_threads());

    let total_comparisons: usize = law_groups.values()
        .map(|citations| citations.len() * (citations.len() - 1) / 2)
        .sum();

    println!("  Total comparisons to perform: {}\n", format_number(total_comparisons));

    let completed = Arc::new(AtomicUsize::new(0));
    let same_article_count = Arc::new(AtomicUsize::new(0));
    let file = File::create(output_path)?;
    let writer = Arc::new(Mutex::new(BufWriter::new(file)));

    let start_time = Instant::now();
    let last_print = Arc::new(Mutex::new(Instant::now()));

    let groups: Vec<_> = law_groups.into_iter().collect();

    groups.par_iter().for_each(|(law, citations)| {
        let n = citations.len();

        for i in 0..n {
            for j in (i + 1)..n {
                let c1 = &citations[i];
                let c2 = &citations[j];

                if c1.element_id == c2.element_id {
                    continue;
                }

                let overlap: AHashSet<_> = c1.articles.intersection(&c2.articles).copied().collect();
                let has_overlap = !overlap.is_empty();

                if has_overlap {
                    same_article_count.fetch_add(1, Ordering::Relaxed);
                }

                let mut arts1: Vec<_> = c1.articles.iter().copied().collect();
                let mut arts2: Vec<_> = c2.articles.iter().copied().collect();
                let mut overlap_vec: Vec<_> = overlap.iter().copied().collect();

                arts1.sort_unstable();
                arts2.sort_unstable();
                overlap_vec.sort_unstable();

                let analysis = CitationAnalysis {
                    citation1: c1.citation.clone(),
                    citation2: c2.citation.clone(),
                    same_law: true,
                    same_article: has_overlap,
                    law1: Some(law.clone()),
                    law2: Some(law.clone()),
                    articles1: arts1,
                    articles2: arts2,
                    overlapping_articles: overlap_vec,
                };

                let record = OutputRecord {
                    element1: c1.element_id.clone(),
                    element2: c2.element_id.clone(),
                    analysis,
                };

                if let Ok(json) = serde_json::to_string(&record) {
                    if let Ok(mut w) = writer.lock() {
                        let _ = writeln!(w, "{}", json);
                    }
                }

                let current = completed.fetch_add(1, Ordering::Relaxed) + 1;

                if let Ok(mut last) = last_print.try_lock() {
                    let now = Instant::now();
                    if now.duration_since(*last) >= Duration::from_secs(10) {
                        *last = now;
                        let elapsed = start_time.elapsed().as_secs_f64();
                        let progress = 100.0 * current as f64 / total_comparisons as f64;
                        let rate = current as f64 / elapsed;
                        let remaining = (total_comparisons - current) as f64 / rate;
                        let same_art = same_article_count.load(Ordering::Relaxed);

                        println!(
                            "  Progress: {:>5.1}% | Matches: {:>6} ({:.1}%) | Rate: {:>8}/s | ETA: {}",
                            progress,
                            format_number(same_art),
                            100.0 * same_art as f64 / current as f64,
                            format_number(rate as usize),
                            format_duration(remaining as u64)
                        );
                    }
                }
            }
        }
    });

    if let Ok(mut w) = writer.lock() {
        w.flush()?;
    }

    let total = completed.load(Ordering::Relaxed);
    let same_article = same_article_count.load(Ordering::Relaxed);

    println!("\n  ✓ Completed {} comparisons", format_number(total));
    println!("  ✓ Found {} same-article matches ({:.2}%)",
             format_number(same_article),
             100.0 * same_article as f64 / total.max(1) as f64);

    Ok((total, same_article))
}

fn print_comparison(original: &AnalysisStats, preprocessed: &AnalysisStats) {
    println!("\n{}", "=".repeat(70));
    println!("📈 PREPROCESSING IMPACT COMPARISON");
    println!("{}", "=".repeat(70));

    println!("\n┌─ PARSING RESULTS ─────────────────────────────────────────────────┐");
    println!("│                          Original    Preprocessed    Improvement   │");
    println!("├───────────────────────────────────────────────────────────────────┤");
    println!("│ Total Citations       {:>10}      {:>10}                 │",
             format_number(original.total_citations),
             format_number(preprocessed.total_citations));
    println!("│ Successfully Parsed   {:>10}      {:>10}      {:>6}     │",
             format_number(original.parsed_citations),
             format_number(preprocessed.parsed_citations),
             format_signed(preprocessed.parsed_citations as i64 - original.parsed_citations as i64));
    println!("│ Parsing Rate          {:>9.1}%      {:>9.1}%      {:>+5.1}%    │",
             original.parsing_rate(),
             preprocessed.parsing_rate(),
             preprocessed.parsing_rate() - original.parsing_rate());
    println!("│ Unparseable           {:>10}      {:>10}      {:>6}     │",
             format_number(original.unparseable_citations),
             format_number(preprocessed.unparseable_citations),
             format_signed(preprocessed.unparseable_citations as i64 - original.unparseable_citations as i64));
    println!("└───────────────────────────────────────────────────────────────────┘");

    println!("\n┌─ LAW COVERAGE ────────────────────────────────────────────────────┐");
    println!("│                          Original    Preprocessed    Change        │");
    println!("├───────────────────────────────────────────────────────────────────┤");
    println!("│ Total Unique Laws     {:>10}      {:>10}      {:>6}     │",
             format_number(original.unique_laws),
             format_number(preprocessed.unique_laws),
             format_signed(preprocessed.unique_laws as i64 - original.unique_laws as i64));
    println!("│ Federal Laws (RS)     {:>10}      {:>10}      {:>6}     │",
             format_number(original.federal_laws),
             format_number(preprocessed.federal_laws),
             format_signed(preprocessed.federal_laws as i64 - original.federal_laws as i64));
    println!("│ Cantonal Laws         {:>10}      {:>10}      {:>6}     │",
             format_number(original.cantonal_laws),
             format_number(preprocessed.cantonal_laws),
             format_signed(preprocessed.cantonal_laws as i64 - original.cantonal_laws as i64));
    println!("└───────────────────────────────────────────────────────────────────┘");

    println!("\n┌─ COMPARISON ANALYSIS ─────────────────────────────────────────────┐");
    println!("│                          Original    Preprocessed    Change        │");
    println!("├───────────────────────────────────────────────────────────────────┤");
    println!("│ Total Comparisons     {:>10}      {:>10}      {:>6}     │",
             format_number(original.total_comparisons),
             format_number(preprocessed.total_comparisons),
             format_signed(preprocessed.total_comparisons as i64 - original.total_comparisons as i64));
    println!("│ Same-Article Matches  {:>10}      {:>10}      {:>6}     │",
             format_number(original.same_article_matches),
             format_number(preprocessed.same_article_matches),
             format_signed(preprocessed.same_article_matches as i64 - original.same_article_matches as i64));
    println!("│ Match Rate            {:>9.2}%      {:>9.2}%      {:>+5.2}%    │",
             original.match_rate(),
             preprocessed.match_rate(),
             preprocessed.match_rate() - original.match_rate());
    println!("└───────────────────────────────────────────────────────────────────┘");

    // Summary
    let rescued = (preprocessed.parsed_citations as i64 - original.parsed_citations as i64) as usize;
    let failure_reduction = if original.unparseable_citations > 0 {
        100.0 * rescued as f64 / original.unparseable_citations as f64
    } else {
        0.0
    };

    println!("\n🎯 KEY INSIGHTS:");
    println!("  • Citations rescued by preprocessing: {}", format_number(rescued));
    println!("  • Parsing failure reduction: {:.1}%", failure_reduction);
    println!("  • Additional comparisons enabled: {}", format_signed(preprocessed.total_comparisons as i64 - original.total_comparisons as i64));
    println!("  • Additional matches discovered: {}", format_signed(preprocessed.same_article_matches as i64 - original.same_article_matches as i64));

    println!("\n{}", "=".repeat(70));
}

// ============================================================================
// MAIN
// ============================================================================

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n{}", "=".repeat(70));
    println!("🚀 CARTESIAN LAW CITATION ANALYSIS - COMPARISON MODE");
    println!("{}", "=".repeat(70));

    // Load abbreviation triplets (shared for both analyses)
    let (_triplets, abbrev_to_rs) = load_abbreviation_triplets("abbreviation_triplets.json")?;

    // Run analysis on original CSV
    let original_stats = run_analysis(
        "CSVs/data_filtered.csv",
        "original",
        &abbrev_to_rs
    )?;

    // Run analysis on preprocessed CSV
    let preprocessed_stats = run_analysis(
        "CSVs/data_filtered_citations_changed.csv",
        "preprocessed",
        &abbrev_to_rs
    )?;

    // Print comparison
    print_comparison(&original_stats, &preprocessed_stats);

    println!("\n✅ All done!\n");
    Ok(())
}
