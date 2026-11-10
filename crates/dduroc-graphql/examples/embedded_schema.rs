//! Execute the transport-neutral schema and a finite follow stream.
//! `cargo run -p dduroc-graphql --example embedded_schema -- --sdl` exports SDL.
use dduroc::prelude::*;
use dduroc_graphql::{SchemaLimits, Service, ServiceConfig, build_schema};
use futures_util::StreamExt;
use std::sync::Arc;

dduroc::schema! {
    name: demo, version: 1, languages: [en],
    events { Tick = 1 { level: Info, en: "tick {sequence}", sequence: u32 } }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let store = Store::open(StoreConfig::new(directory.path()))?;
    let namespace = store.namespace("device", demo::SCHEMA)?;
    let service = Service::new(Arc::new(store.reader()), ServiceConfig::default())?;
    let schema = build_schema(service, SchemaLimits::default())?;
    if std::env::args().any(|argument| argument == "--sdl") {
        print!("{}", schema.sdl());
        return Ok(());
    }
    namespace.try_log(demo::events::Tick { sequence: 1 })?;
    namespace.sync()?;
    let response = schema
        .execute(
            r#"{
        apiVersion
        read(query:{order:OLDEST},page:{maxRecords:"2"}) {
            entries { namespace at { boot micros } message { rendered payload { base64 bytes } } }
            damaged { message offset } unanchored exhausted truncated
        }
    }"#,
        )
        .await;
    if !response.errors.is_empty() {
        return Err(format!("read failed: {:?}", response.errors).into());
    }
    println!("{}", response.data);
    // A finite query limit lets this example consume the complete follow
    // stream, including its ended notice. No HTTP or WebSocket is involved.
    let mut stream = schema.execute_stream(
        r#"subscription {
        follow(query:{order:OLDEST,limit:"1"}) {
            entries { namespace message { rendered } }
            seeds { namespace } damaged { message } unanchored ended
        }
    }"#,
    );
    while let Some(response) = stream.next().await {
        if !response.errors.is_empty() {
            return Err(format!("follow failed: {:?}", response.errors).into());
        }
        println!("{}", response.data);
    }
    Ok(())
}
