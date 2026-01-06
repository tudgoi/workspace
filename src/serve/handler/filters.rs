pub fn thumbnail<T: std::fmt::Display>(s: T, _: &dyn askama::Values) -> askama::Result<String> {
    const WIKIMEDIA_PREFIX: &str = "https://upload.wikimedia.org/wikipedia/commons/";
    let s = s.to_string();
    let thumbnail =
        if s.starts_with(WIKIMEDIA_PREFIX) && !s.contains("/thumb/") && !s.ends_with(".svg") {
            let prefix = s.replace("/commons/", "/commons/thumb/");
            let filename: String = s
                .trim_start_matches(WIKIMEDIA_PREFIX)
                .chars()
                .skip(5)
                .collect();

            format!("{}/320px-{}", prefix, filename)
        } else {
            s
        };

    Ok(thumbnail)
}
