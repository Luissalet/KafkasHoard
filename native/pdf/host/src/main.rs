//! Kafka's owned PDF worker. The normal API and MCP share this JSON batch contract.
use kafka_pdf_automation::{Automation, Content};
use serde_json::{Value, json};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

fn catalog() -> Value {
    let tools: Vec<Value> = kafka_pdf_automation::tools().iter().map(|tool| json!({
        "name": tool.name, "description": tool.description, "read_only": tool.read_only,
        "command": tool.command, "input_schema": tool.input_schema,
    })).collect();
    json!({"format": "kafka-pdf-catalog/v1", "engine": "Kafka PDF", "tools": tools})
}

fn execute(request: &Path) -> Result<Value, String> {
    let bytes = std::fs::read(request).map_err(|error| error.to_string())?;
    let input: Value = serde_json::from_slice(&bytes).map_err(|error| error.to_string())?;
    if input["format"] != "kafka-pdf-batch/v1" {
        return Err("Expected kafka-pdf-batch/v1".into());
    }
    let workspace = PathBuf::from(input["workspace"].as_str().ok_or("Missing workspace")?);
    let operations = input["operations"].as_array().ok_or("Missing operations array")?;
    let mut session = Automation::new().with_root(&workspace).map_err(|error| error.to_string())?;
    let mut results = Vec::new();
    for (index, operation) in operations.iter().enumerate() {
        let name = operation["name"].as_str().ok_or("Missing operation name")?;
        let content = session.call(name, &operation["arguments"])
            .map_err(|error| format!("Operation {} ({name}): {error}", index + 1))?;
        for item in content {
            match item {
                Content::Json(value) => results.push(value),
                Content::Png { data, width, height } => {
                    let output = workspace.join(format!("page-render-{}.png", index + 1));
                    std::fs::write(&output, &data).map_err(|error| error.to_string())?;
                    results.push(json!({"image": output, "width": width, "height": height, "bytes": data.len()}));
                }
            }
        }
    }
    Ok(json!({"format": "kafka-pdf-result/v1", "engine": "Kafka PDF", "results": results}))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let result = match args.first().map(String::as_str) {
        Some("catalog") => Ok(catalog()),
        Some("execute") if args.len() == 2 => execute(Path::new(&args[1])),
        Some("--version") => Ok(json!({"engine": "Kafka PDF", "version": env!("CARGO_PKG_VERSION")})),
        _ => Err("Usage: kafka-pdf-worker catalog | execute REQUEST.json | --version".into()),
    };
    match result {
        Ok(value) => { println!("{value}"); ExitCode::SUCCESS }
        Err(error) => { eprintln!("Kafka PDF: {error}"); ExitCode::FAILURE }
    }
}
