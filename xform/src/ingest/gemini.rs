use std::{collections::HashMap, env};
use gemini_rust::Gemini;
use anyhow::{Context, Result};
use serde_json::{json, Value};

use crate::{data, ingest::Ingestor};

pub struct GeminiIngestor {
    client: Gemini,
}

impl GeminiIngestor {
    pub fn new() -> Result<Self> {
        let api_key = env::var("GEMINI_API_KEY")
            .with_context(|| format!("GEMINI_API_KEY environment variable not set"))?;
        let client = Gemini::new(api_key);

        Ok(GeminiIngestor { client })
    }
}

impl Ingestor for GeminiIngestor {
    async fn query(&self, query: &str) -> Result<HashMap<String, data::Person>> {
        let schema = persons_json_schema();

        let response = self.client
            .generate_content()
            .with_system_prompt(
                "You provide information about Indian Government officers and politicians in office in JSON format.",
            )
            .with_user_message(query)
            .with_response_mime_type("application/json")
            .with_response_schema(schema.into())
            .execute()
            .await
            .with_context(|| format!("error calling Gemini API"))?;
        
        let texts = response.all_text();
        for (text, status) in texts {
            println!("{} {}", status, text);
        }

        //let persons: Vec<data::Person> = serde_json::from_str(&json_text)?;
        
        Ok(HashMap::new())        
    }
}

fn person_json_schema() -> Value {
    json!({
      "type": "object",
      "properties": {
        "name": {
          "type": "string"
        },
        "photo": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string"
                },
                "attribution": {
                    "type": "string"
                }
            },
            "required": ["url"]
        },
        "contacts": {
          "type": "object",
          "properties": {
            "wikidata": {
                "type": "string"
            }
          }
        },
        "tenures": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "office": {
                    "type": "string"
                },
                "start": {
                    "type": "string"
                },
                "end": {
                    "type": "string"
                }
              },
              "required": ["office"]
          }
        }
      },
      "required": [
        "name"
      ],
    })
}

fn persons_json_schema() -> Value {
    json!({
        "type": "array",
        "items": person_json_schema(),
    })
}