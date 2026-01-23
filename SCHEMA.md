# Database Key-Value Schema

This document describes the key-value schema managed by the `tudgoi` CLI. Data is stored in an SQLite-backed Merkle Search Tree (MST) and can be manipulated using `get`, `set`, `list`, and `delete` commands.

## Key Path Patterns

Keys follow a hierarchical path structure: `{entity_type}/{entity_id}/{field_path}`.
entity_id for a real person needs to be maximum of 8 characters. The truncated first name
forms the first part of the ID. The initials of the other parts of the name form the last part of the ID.

### 1. Entity Name
Sets the display name for a person or office.
- **Path:** `person/{id}/name` or `office/{id}/name`
- **Value Type:** `String` (JSON string)
- **Example:** `cargo run -- set db.db person/narendra-modi/name '"Narendra Modi"'`

### 2. Entity Photo
Sets the photo URL and attribution.
- **Path:** `person/{id}/photo` or `office/{id}/photo`
- **Value Type:** `Photo` object
- **Schema:**
  ```json
  {
    "url": "string",
    "attribution": "string | null"
  }
  ```
- **Example:** `cargo run -- set db.db person/narendra-modi/photo '{"url": "...", "attribution": "..."}'`

### 3. Entity Contact
Sets a contact detail for a person or office.
- **Path:** `person/{id}/contact/{type}` or `office/{id}/contact/{type}`
- **Value Type:** `String` (JSON string)
- **Valid Types:** `address`, `phone`, `email`, `website`, `wikipedia`, `x`, `youtube`, `facebook`, `instagram`, `wikidata`
- **Example:** `cargo run -- set db.db person/narendra-modi/contact/x '"narendramodi"'`

### 4. Office Supervisor
Sets a supervising relation for an office.
- **Path:** `office/{id}/supervisor/{relation}`
- **Value Type:** `String` (JSON string) - ID of the supervising entity.
- **Valid Relations:** `head`, `adviser`, `during_the_pleasure_of`, `responsible_to`, `member_of`, `minister`
- **Example:** `cargo run -- set db.db office/pmo/supervisor/head '"narendra-modi"'`

### 5. Person Tenure
Sets the end date for a person's tenure in a specific office.
- **Path:** `person/{id}/tenure/{office_id}/{start_date}`
- **Value Type:** `String | null` (JSON string) - The end date in `YYYY-MM-DD` format, or `null` if current.
- **Note:** The `{start_date}` in the path must be in `YYYY-MM-DD` format (or empty if unknown).
- **Example:** `cargo run -- set db.db person/narendra-modi/tenure/prime-minister/2014-05-26 'null'`

---

## Command Examples

### Listing all data for an entity
```bash
cargo run -- list db.db person/narendra-modi/
```

### Getting a specific value
```bash
cargo run -- get db.db person/narendra-modi/name
```

### Deleting a value
```bash
cargo run -- delete db.db person/narendra-modi/contact/facebook
```

## Data Types Reference

### Date Format
All dates must be in ISO 8601 format: `YYYY-MM-DD`.

### Enums

#### Contact Types
- `address`, `phone`, `email`, `website`, `wikipedia`, `x`, `youtube`, `facebook`, `instagram`, `wikidata`

#### Supervising Relations
- `head`, `adviser`, `during_the_pleasure_of`, `responsible_to`, `member_of`, `minister`