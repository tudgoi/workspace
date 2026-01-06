use regex::Regex;

use crate::dto;

pub fn derive_id(entity_type: &dto::EntityType, name: &str) -> String {
    match entity_type {
        dto::EntityType::Person => derive_person_id(name),
        dto::EntityType::Office => derive_office_id(name),
    }
}

pub fn derive_person_id(name: &str) -> String {
    let parts: Vec<&str> = name.split_whitespace().collect();
    if parts.is_empty() {
        return String::new();
    }

    let first_name = parts[0];
    let initials: String = parts
        .iter()
        .skip(1)
        .filter_map(|p| p.chars().next())
        .collect();

    let max_len = 8;
    let initials_len = initials.len();

    if first_name.len() + initials_len <= max_len {
        format!("{}{}", first_name, initials).to_lowercase()
    } else {
        let first_name_len = max_len.saturating_sub(initials_len);
        let truncated_first_name = first_name.chars().take(first_name_len).collect::<String>();
        format!("{}{}", truncated_first_name, initials).to_lowercase()
    }
}

/// Generate an office ID from the office name.
pub fn derive_office_id(name: &str) -> String {
    let name = name.trim().to_lowercase();

    // State abbreviations (expandable)
    let states = [
        ("andhra pradesh", "ap"),
        ("arunachal pradesh", "ar"),
        ("assam", "as"),
        ("bihar", "br"),
        ("chhattisgarh", "ct"),
        ("goa", "ga"),
        ("gujarat", "gj"),
        ("haryana", "hr"),
        ("himachal pradesh", "hp"),
        ("jharkhand", "jh"),
        ("karnataka", "ka"),
        ("kerala", "kl"),
        ("madhya pradesh", "mp"),
        ("maharashtra", "mh"),
        ("manipur", "mn"),
        ("meghalaya", "ml"),
        ("mizoram", "mz"),
        ("nagaland", "nl"),
        ("odisha", "od"),
        ("punjab", "pb"),
        ("rajasthan", "rj"),
        ("sikkim", "sk"),
        ("tamil nadu", "tn"),
        ("telangana", "ts"),
        ("tripura", "tr"),
        ("uttar pradesh", "up"),
        ("uttarakhand", "uk"),
        ("west bengal", "wb"),
        ("delhi", "dl"),
        ("jammu and kashmir", "jk"),
        ("ladakh", "la"),
        ("puducherry", "py"),
    ];

    // Regex: split on anything that's not a-z0-9
    let splitter = Regex::new(r"[^a-z0-9]+").unwrap();

    // MLA constituencies
    if name.contains("mla") {
        let re = Regex::new(r"mla\s*\((?P<constituency>.+)\)").unwrap();
        if let Some(cap) = re.captures(&name) {
            let constituency = cap["constituency"].to_lowercase();
            let parts: Vec<&str> = splitter
                .split(&constituency)
                .filter(|s| !s.is_empty())
                .collect();
            return format!("mla-{}", parts.join("-"));
        }
    }

    // Rajya Sabha MPs
    if name.contains("member of parliament, rajya sabha") {
        return "mp-rs".into();
    }

    // Lok Sabha MPs (with constituency)
    if name.contains("member of parliament, lok sabha") {
        let re = Regex::new(r"lok sabha\s*\((?P<constituency>.+)\)").unwrap();
        if let Some(cap) = re.captures(&name) {
            let constituency = cap["constituency"].to_lowercase();
            let parts: Vec<&str> = splitter
                .split(&constituency)
                .filter(|s| !s.is_empty())
                .collect();
            return format!("mp-ls-{}", parts.join("-"));
        }
        return "mp-ls".into();
    }

    // Union Ministers
    if name.contains("union minister of") {
        let re = Regex::new(r"union minister of\s+(?P<portfolio>.+)").unwrap();
        if let Some(cap) = re.captures(&name) {
            let portfolio: String = splitter
                .split(&cap["portfolio"])
                .filter(|s| !s.is_empty())
                .map(|w| {
                    if w.chars().all(|c| c.is_ascii_digit()) {
                        w.to_string()
                    } else {
                        w.chars().next().unwrap().to_string()
                    }
                })
                .collect();
            return format!("um{}", portfolio);
        }
    }

    // Governors
    if name.starts_with("governor of") {
        let state_name = name.strip_prefix("governor of").unwrap().trim();
        for (full, abbr) in states {
            if state_name == full {
                return format!("go{}", abbr);
            }
        }
    }

    // Default rule:
    // - Collapse the "prefix" of initials
    // - Keep special keywords and numbers as full tokens
    let tokens: Vec<&str> = splitter.split(&name).filter(|s| !s.is_empty()).collect();

    let special_keywords = ["ward", "zone", "district", "block"];

    let mut parts: Vec<String> = Vec::new();
    let mut prefix = String::new();

    for token in tokens {
        if token.chars().all(|c| c.is_ascii_digit()) || special_keywords.contains(&token) {
            // flush prefix if we have it
            if !prefix.is_empty() {
                parts.push(prefix.clone());
                prefix.clear();
            }
            // keep keyword/number as-is
            parts.push(token.to_string());
        } else {
            // take initial into prefix
            prefix.push(token.chars().next().unwrap());
        }
    }

    if !prefix.is_empty() {
        parts.push(prefix);
    }

    parts.join("-")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_derive_office_id() {
        assert_eq!(derive_office_id("Governor of Meghalaya"), "goml");
        assert_eq!(derive_office_id("MLA (Ariyalur)"), "mla-ariyalur");
        assert_eq!(derive_office_id("MLA (Cheyyur (SC))"), "mla-cheyyur-sc");
        assert_eq!(
            derive_office_id("Member of Parliament, Rajya Sabha"),
            "mp-rs"
        );
        assert_eq!(derive_office_id("Union Minister of Tourism"), "umt");
        assert_eq!(
            derive_office_id("Member of Parliament, Lok Sabha (Chennai South)"),
            "mp-ls-chennai-south"
        );
        assert_eq!(
            derive_office_id("Greater Chennai Corporation Councillor Ward 180"),
            "gccc-ward-180"
        );
        assert_eq!(
            derive_office_id("Panchayat Union Councillor Zone 5"),
            "puc-zone-5"
        );
    }
}
