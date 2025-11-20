// Helper functions for context-aware citation parsing
use crate::{
    extract_law_abbreviation, extract_article_numbers, normalize_to_rs_number,
    AHashMap, AHashSet, AbbrevToRs, CitationInfo, Element, UnparseableCitation,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use regex::Regex;

#[derive(Debug, Deserialize)]
struct TitlesMapping {
    title_to_rs: HashMap<String, String>,
}

fn load_titles_mapping() -> Option<HashMap<String, String>> {
    if let Ok(file) = std::fs::File::open("titles_mapping.json") {
        if let Ok(mapping) = serde_json::from_reader::<_, TitlesMapping>(file) {
            return Some(mapping.title_to_rs);
        }
    }
    None
}

fn normalize_text(text: &str) -> String {
    text.to_lowercase()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .trim_end_matches(&['.', ',', ';', ':'][..])
        .to_string()
}

/// Normalize citation text by fixing common patterns
fn normalize_citation(citation: &str) -> String {
    let mut result = citation.to_string();

    // Fix pattern 1: "43 aCP" -> "43 a CP" (missing space after digit+a)
    // Allow mixed case abbreviations like "BauR"
    let re1 = Regex::new(r"(\d+)\s+a([A-ZÄÖÜ][A-ZÄÖÜa-zäöüß]{1,})\b").unwrap();
    result = re1.replace_all(&result, "$1 a $2").to_string();

    // Fix pattern 2: " aBauR" -> " a BauR" (missing space in general)
    // Matches whitespace + lowercase "a" + uppercase letter + any letters (mixed case allowed)
    let re2 = Regex::new(r"(\s)a([A-ZÄÖÜ][A-ZÄÖÜa-zäöüß]{1,})\b").unwrap();
    result = re2.replace_all(&result, "$1a $2").to_string();

    result
}

/// Clean law title by stripping trailing footnote numbers
fn clean_law_title(title: &str) -> String {
    // Strip trailing digits (footnote references like "administration3")
    let re = Regex::new(r"([a-zàâäéèêëïîôùûüÿœæç])\d+$").unwrap();
    re.replace_all(title, "$1").to_string()
}

fn find_law_by_title(text: &str, title_to_rs: &HashMap<String, String>) -> Option<String> {
    // Clean the text (strip footnote numbers, etc.)
    let cleaned = clean_law_title(text);
    let normalized = normalize_text(&cleaned);

    // Try exact match first
    if let Some(rs) = title_to_rs.get(&normalized) {
        return Some(rs.clone());
    }

    // Extract key words from text (words longer than 3 chars, excluding common ones)
    let common_words = ["loi", "ordonnance", "décret", "arrêté", "règlement", "gesetz",
                        "verordnung", "beschluss", "bundesgesetz", "legge", "ordinanza",
                        "decreto", "fédérale", "federale", "suisse", "svizzera", "schweiz",
                        "concernant", "betreffend", "concerning", "über", "sulla", "sur",
                        "pour", "dans", "avec", "même", "ainsi", "aussi"];

    let text_words: Vec<&str> = normalized
        .split_whitespace()
        .filter(|w| w.len() > 3 && !common_words.contains(w))
        .collect();

    if text_words.len() < 2 {  // Reduced from 3 to allow shorter titles
        return None; // Too few distinctive words to match reliably
    }

    // Try fuzzy matching: if most key words from text appear in title
    let mut best_match: Option<(String, usize)> = None;

    for (title, rs) in title_to_rs.iter() {
        if title.len() < 20 {  // Reduced from 30 to catch more titles
            continue;
        }

        // Count how many text words appear in this title
        let matching_words = text_words.iter()
            .filter(|&&word| title.contains(word))
            .count();

        // Adaptive threshold: longer titles can match with fewer words
        // 2-3 words: 50%, 4-6 words: 45%, 7+ words: 40%
        let threshold = if text_words.len() <= 3 {
            0.5
        } else if text_words.len() <= 6 {
            0.45
        } else {
            0.4
        };

        if matching_words as f64 / text_words.len() as f64 >= threshold {
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

/// Find citation in content and extract surrounding context
/// Returns tuple of (complete_citation, context) where complete_citation has balanced parentheses
pub fn extract_context_around_citation(citation: &str, content: &str, context_size: usize) -> Option<(String, String)> {
    // Try to find the citation in the content (case-insensitive)
    let citation_lower = citation.to_lowercase();
    let content_lower = content.to_lowercase();

    if let Some(byte_pos) = content_lower.find(&citation_lower) {
        // Find safe character boundaries for slicing
        let char_indices: Vec<_> = content.char_indices().collect();

        // Find the character index corresponding to byte_pos
        let char_idx = char_indices.iter()
            .position(|(idx, _)| *idx >= byte_pos)
            .unwrap_or(0);

        // Extend to the right to close any open parentheses
        let citation_end_char = char_idx + citation.chars().count();
        let mut extended_end_char = citation_end_char;

        // Count open parentheses in the citation
        let open_parens = citation.chars().filter(|&c| c == '(').count();
        let close_parens = citation.chars().filter(|&c| c == ')').count();

        if open_parens > close_parens {
            // We have unclosed parentheses - extend to the right
            let mut balance = open_parens - close_parens;
            for i in citation_end_char..char_indices.len() {
                if let Some((_, ch)) = char_indices.get(i) {
                    if *ch == '(' {
                        balance += 1;
                    } else if *ch == ')' {
                        balance -= 1;
                        if balance == 0 {
                            extended_end_char = i + 1; // Include the closing paren
                            break;
                        }
                    }
                }
            }
        } else {
            extended_end_char = citation_end_char;
        }

        // Extract the complete citation with balanced parentheses
        let citation_start_byte = char_indices.get(char_idx).map(|(idx, _)| *idx).unwrap_or(0);
        let citation_end_byte = char_indices.get(extended_end_char).map(|(idx, _)| *idx).unwrap_or(content.len());
        let complete_citation = content[citation_start_byte..citation_end_byte].to_string();

        // Calculate start and end in character indices for context window
        let start_char = char_idx.saturating_sub(context_size);
        let end_char = (extended_end_char + context_size).min(char_indices.len());

        // Convert back to byte indices
        let start_byte = char_indices.get(start_char).map(|(idx, _)| *idx).unwrap_or(0);
        let end_byte = char_indices.get(end_char).map(|(idx, _)| *idx).unwrap_or(content.len());
        let context = content[start_byte..end_byte].to_string();

        return Some((complete_citation, context));
    }

    // Try normalized version (remove extra spaces, etc.)
    {
        // Try to find a normalized version (remove extra spaces, etc.)
        let citation_normalized: String = citation_lower.split_whitespace().collect::<Vec<_>>().join(" ");
        let content_normalized: String = content_lower.split_whitespace().collect::<Vec<_>>().join(" ");

        if let Some(pos) = content_normalized.find(&citation_normalized) {
            // Approximate position in original content
            let words_before = content_normalized[..pos].split_whitespace().count();
            let words_in_citation = citation_normalized.split_whitespace().count();

            let content_words: Vec<&str> = content.split_whitespace().collect();

            // Extract complete citation with balanced parens
            let citation_start = words_before;
            let mut citation_end = words_before + words_in_citation;

            // Check if we need to extend for closing parens
            let citation_text = content_words[citation_start..citation_end].join(" ");
            let open_parens = citation_text.chars().filter(|&c| c == '(').count();
            let close_parens = citation_text.chars().filter(|&c| c == ')').count();

            if open_parens > close_parens {
                let mut balance = open_parens - close_parens;
                for i in citation_end..content_words.len() {
                    citation_end = i + 1;
                    let word = content_words[i];
                    for ch in word.chars() {
                        if ch == '(' {
                            balance += 1;
                        } else if ch == ')' {
                            balance -= 1;
                            if balance == 0 {
                                break;
                            }
                        }
                    }
                    if balance == 0 {
                        break;
                    }
                }
            }

            let complete_citation = content_words[citation_start..citation_end].join(" ");
            let start_word = words_before.saturating_sub(20);
            let end_word = (citation_end + 20).min(content_words.len());

            if start_word < content_words.len() && end_word <= content_words.len() {
                let context = content_words[start_word..end_word].join(" ");
                return Some((complete_citation, context));
            }
        }
        None
    }
}

/// Process unparseable citations by looking up context
pub fn enrich_with_context(
    elements: &[Element],
    mut unparseable_list: Vec<UnparseableCitation>,
    law_groups: &mut AHashMap<String, Vec<CitationInfo>>,
    abbrev_to_rs: &AbbrevToRs,
) -> Vec<UnparseableCitation> {
    println!("\n🔍 Phase 1.5: Enriching fragments with context from part_content...");

    // Ensure logs directory exists
    std::fs::create_dir_all("logs").ok();

    // Open rescued citations file
    let rescued_file = File::create("logs/rescued_citations.txt")
        .expect("Failed to create logs/rescued_citations.txt");
    let mut rescued_writer = BufWriter::new(rescued_file);

    // Load title mappings
    let title_to_rs = load_titles_mapping();
    if let Some(ref mapping) = title_to_rs {
        println!("  ✓ Loaded {} law titles for matching", mapping.len());
    } else {
        println!("  ⚠ Could not load titles_mapping.json, skipping title matching");
    }

    // Create a map of element_id -> part_content for quick lookup
    let element_content_map: AHashMap<String, String> = elements
        .iter()
        .map(|e| (e.id.clone(), e.part_content.clone()))
        .collect();

    let mut rescued = 0;
    let mut rescued_by_title = 0;
    let mut still_unparseable = Vec::new();
    let mut contexts_found = 0;
    let mut contexts_not_found = 0;

    for (idx, unparseable) in unparseable_list.into_iter().enumerate() {
        if unparseable.reason != "no_abbreviation_found" {
            still_unparseable.push(unparseable);
            continue;
        }

        // Look up the element's content
        if let Some(content) = element_content_map.get(&unparseable.element_id) {
            // Extract context around the citation (wider window to capture full law names)
            if let Some((complete_citation, context)) = extract_context_around_citation(&unparseable.citation, content, 300) {
                contexts_found += 1;
                let mut law_key_opt = None;

                // Normalize the complete citation (fix "43 aCP" -> "43 a CP", etc.)
                let normalized_citation = normalize_citation(&complete_citation);

                // Try to extract law abbreviation from the normalized citation first
                if let Some(law_abbrev) = extract_law_abbreviation(&normalized_citation) {
                    // ONLY accept if it's in the abbreviation triplets (known federal law)
                    if let Some(rs_number) = normalize_to_rs_number(&law_abbrev, abbrev_to_rs) {
                        law_key_opt = Some(rs_number);
                    }
                }

                // If not found in citation, try the context
                if law_key_opt.is_none() {
                    if let Some(law_abbrev) = extract_law_abbreviation(&context) {
                        if let Some(rs_number) = normalize_to_rs_number(&law_abbrev, abbrev_to_rs) {
                            law_key_opt = Some(rs_number);
                        }
                    }
                }

                // If abbreviation didn't work, try title matching
                if law_key_opt.is_none() {
                    if let Some(ref mapping) = title_to_rs {
                        // Try title matching on the normalized citation first
                        if let Some(rs_from_title) = find_law_by_title(&normalized_citation, mapping) {
                            law_key_opt = Some(rs_from_title);
                            rescued_by_title += 1;
                        } else if let Some(rs_from_title) = find_law_by_title(&context, mapping) {
                            // If not found in citation, try the wider context
                            law_key_opt = Some(rs_from_title);
                            rescued_by_title += 1;
                        }
                    }
                }

                // If we found a law (either by abbreviation or title), add it
                if let Some(law_key) = law_key_opt {
                    // Extract articles from the normalized citation
                    let articles = extract_article_numbers(&normalized_citation);

                    // Write rescued citation to file
                    let fixed_citation = format!("{} {}", law_key, normalized_citation);
                    writeln!(rescued_writer, "{} | {}", unparseable.citation, fixed_citation)
                        .ok();

                    // Add to law groups
                    law_groups.entry(law_key.clone()).or_insert_with(Vec::new).push(CitationInfo {
                        element_id: unparseable.element_id.clone(),
                        citation: normalized_citation,
                        law: law_key,
                        articles,
                    });

                    rescued += 1;
                    continue;
                }
            } else {
                contexts_not_found += 1;
            }
        } else {
            contexts_not_found += 1;
        }

        // Still couldn't parse
        still_unparseable.push(unparseable);
    }

    println!("  ✓ Contexts found: {}, not found: {}", contexts_found, contexts_not_found);
    println!("  ✓ Rescued {} fragments using context", rescued);
    if rescued_by_title > 0 {
        println!("    - {} rescued by title matching", rescued_by_title);
        println!("    - {} rescued by abbreviation extraction", rescued - rescued_by_title);
    }
    println!("  ✓ Still unparseable: {}", still_unparseable.len());

    // Flush and close rescued citations file
    rescued_writer.flush().ok();
    if rescued > 0 {
        println!("  ✓ Saved {} rescued citations to logs/rescued_citations.txt", rescued);
    }

    still_unparseable
}
