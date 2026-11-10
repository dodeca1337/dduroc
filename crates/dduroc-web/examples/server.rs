//! Local demonstration only. The embedding host owns authentication and TLS.
use async_graphql_axum::{GraphQLProtocol, GraphQLRequest, GraphQLResponse, GraphQLWebSocket};
use axum::{
    Router,
    extract::{DefaultBodyLimit, State, WebSocketUpgrade},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use dduroc::prelude::*;
use dduroc_graphql::{LogSchema, SchemaLimits, Service, ServiceConfig, build_schema};
use std::sync::Arc;

dduroc::schema! {
    name: web_demo, version: 1, languages: [en, ru],
    events {
        Tick = 1 { level: Info, en: "tick {sequence}", ru: "такт {sequence}", sequence: u64, samples: Vec<u16> },
        Fault = 2 { level: Error, en: "fault {code}", ru: "ошибка {code}", code: u8 },
    }
    metrics {
        Temperature = 1 { type: f32, unit: "°C" },
        Counter = 2 { type: u64 },
        Link = 3 { states: [warn Searching = 0, Ready = 1] },
    }
    spans { Calibration = 1 }
}
async fn graphql(State(schema): State<LogSchema>, request: GraphQLRequest) -> GraphQLResponse {
    schema.execute(request.into_inner()).await.into()
}
async fn ws(
    State(schema): State<LogSchema>,
    headers: HeaderMap,
    protocol: GraphQLProtocol,
    upgrade: WebSocketUpgrade,
) -> Response {
    // This example accepts only the modern protocol. Authentication and origin
    // checks are the responsibility of the embedding host.
    if headers
        .get(header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok())
        != Some("graphql-transport-ws")
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    upgrade
        .protocols(["graphql-transport-ws"])
        .on_upgrade(move |socket| GraphQLWebSocket::new(socket, schema, protocol).serve())
        .into_response()
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let port: u16 = args.next().unwrap_or_else(|| "8080".into()).parse()?;
    let history_records: u32 = match args.next().as_deref() {
        None => 0,
        Some("--history-records") => args.next().ok_or("missing history count")?.parse()?,
        Some(_) => return Err("expected --history-records after the port".into()),
    };
    if history_records > 100_000 || args.next().is_some() {
        return Err("history count must be at most 100000 with no extra arguments".into());
    }
    let directory = tempfile::tempdir()?;
    let store = Store::open(StoreConfig::new(directory.path()))?;
    // The demo host's system clock is the explicitly chosen UTC source.
    store.record_sync(dduroc::chrono::Utc::now(), dduroc::SyncSource::User)?;
    if history_records != 0 {
        let history = store.namespace("history-0", web_demo::SCHEMA)?;
        for sequence in 0..history_records {
            history.try_log(web_demo::events::Tick {
                sequence: sequence.into(),
                samples: Vec::new(),
            })?;
            if sequence % 64 == 63 {
                store.sync()?;
            }
        }
        store.sync()?;
    }
    let namespace = store.namespace("radio-0", web_demo::SCHEMA)?;
    let other = store.namespace("radio-1", web_demo::SCHEMA)?;
    for sequence in 0..20 {
        namespace.try_log(web_demo::events::Tick {
            sequence,
            samples: vec![1, 2, 3],
        })?;
    }
    other.try_log(web_demo::events::Tick {
        sequence: u64::MAX,
        samples: vec![8, 13],
    })?;
    other.try_log(web_demo::events::Fault { code: 7 })?;
    other.try_log_text(Level::Warn, "demo", "Example diagnostic", None)?;
    namespace
        .series(web_demo::metrics::Temperature)?
        .sample(f32::NAN);
    namespace
        .series(web_demo::metrics::Counter)?
        .sample(u64::MAX);
    namespace
        .series(web_demo::metrics::Link)?
        .sample(web_demo::metrics::Link::Searching);
    namespace
        .series(web_demo::metrics::Link)?
        .sample(web_demo::metrics::Link::Ready);
    {
        let span = namespace.span(web_demo::spans::Calibration);
        span.log(web_demo::events::Fault { code: 3 });
    }
    store.sync()?;
    let service = Service::new(Arc::new(store.reader()), ServiceConfig::default())?;
    let schema = build_schema(service, SchemaLimits::default())?;
    let api = Router::new()
        .route("/graphql", post(graphql))
        .route("/graphql/ws", get(ws))
        .layer(DefaultBodyLimit::max(1024 * 1024))
        .with_state(schema);
    let ui = dduroc_web::mount(
        "/logs",
        dduroc_web::UiConfig {
            base_path: "/logs/".into(),
            graphql_endpoint: "/graphql".into(),
            subscription_endpoint: Some("/graphql/ws".into()),
            ..Default::default()
        },
    )?;
    let app = api.merge(ui);
    let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).await?;
    eprintln!("Local demo: http://{}/logs/", listener.local_addr()?);
    axum::serve(listener, app).await?;
    Ok(())
}
