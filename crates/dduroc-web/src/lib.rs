//! An embeddable, UI-only Axum router for a separately hosted dduroc GraphQL
//! endpoint. The host owns listeners, authentication, TLS and endpoint routing.
#![forbid(unsafe_code)]

use axum::{
    Router,
    extract::{OriginalUri, Path, State},
    http::{HeaderMap, HeaderValue, StatusCode, Uri, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Serialize;
use std::sync::Arc;

include!(concat!(env!("OUT_DIR"), "/assets.rs"));
const BASE_TOKEN: &str = "__DDUROC_BASE_HREF__";

/// Browser configuration. `base_path` is the external mount path, including
/// any reverse-proxy prefix, and must end with `/`.
#[derive(Debug, Clone)]
pub struct UiConfig {
    pub base_path: String,
    pub graphql_endpoint: String,
    pub subscription_endpoint: Option<String>,
    pub title: Option<String>,
    pub max_rows: Option<usize>,
}
impl Default for UiConfig {
    fn default() -> Self {
        Self {
            base_path: "/".into(),
            graphql_endpoint: "/graphql".into(),
            subscription_endpoint: None,
            title: None,
            max_rows: None,
        }
    }
}
#[derive(Debug, thiserror::Error)]
#[error("invalid UI configuration: {0}")]
pub struct ConfigError(&'static str);
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BrowserConfig<'a> {
    graphql_endpoint: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    subscription_endpoint: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    title: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_rows: Option<usize>,
}
impl UiConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.base_path.starts_with('/')
            || !self.base_path.ends_with('/')
            || self.base_path.len() > 1024
            || self.base_path.contains("//")
            || self.base_path.bytes().any(|b| {
                !(b.is_ascii_alphanumeric() || matches!(b, b'/' | b'-' | b'_' | b'.' | b'~'))
            })
            || self
                .base_path
                .split('/')
                .any(|part| part == "." || part == "..")
        {
            return Err(ConfigError(
                "base_path must be a safe absolute path ending in slash",
            ));
        }
        endpoint(&self.graphql_endpoint, false)?;
        if let Some(value) = &self.subscription_endpoint {
            endpoint(value, true)?;
        }
        if self.title.as_ref().is_some_and(|title| title.len() > 4096) {
            return Err(ConfigError("title is too long"));
        }
        if self.max_rows.is_some_and(|rows| rows == 0 || rows > 10_000) {
            return Err(ConfigError("max_rows is outside the supported range"));
        }
        Ok(())
    }
}
fn endpoint(value: &str, websocket: bool) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > 4096
        || value
            .bytes()
            .any(|b| b <= 0x20 || b == 0x7f || b == b'\\' || b == b'#')
        || value.starts_with("//")
    {
        return Err(ConfigError("invalid endpoint"));
    }
    // HTTP URI parsing treats a bare relative path as an authority; parse it
    // as a path instead, while still rejecting scheme-like relative values.
    let relative;
    let checked = if !value.starts_with('/') && !value.contains("://") {
        if value.contains(':') {
            return Err(ConfigError("unsupported endpoint URI"));
        }
        relative = format!("/{value}");
        relative.as_str()
    } else {
        value
    };
    let uri: Uri = checked
        .parse()
        .map_err(|_| ConfigError("invalid endpoint URI"))?;
    if let Some(scheme) = uri.scheme_str() {
        let allowed = if websocket {
            matches!(scheme, "http" | "https" | "ws" | "wss")
        } else {
            matches!(scheme, "http" | "https")
        };
        if !allowed || uri.authority().is_none_or(|a| a.as_str().contains('@')) {
            return Err(ConfigError("unsupported endpoint URI"));
        }
    } else if uri.authority().is_some() {
        return Err(ConfigError("invalid relative endpoint URI"));
    }
    Ok(())
}

#[derive(Clone)]
struct UiState {
    config: Arc<UiConfig>,
    index: Arc<str>,
    browser_json: Arc<str>,
}
/// Create relative UI routes. For a complete prefix with a slash redirect,
/// use [`mount`]. When nesting manually, use a trailing slash in the nesting
/// prefix (for example `.nest("/logs/", router(config)?)`). Apply host
/// authentication to UI, GraphQL and WebSocket endpoints alike.
pub fn router(config: UiConfig) -> Result<Router, ConfigError> {
    config.validate()?;
    let index_asset = ASSETS
        .iter()
        .find(|asset| asset.path == "index.html")
        .ok_or(ConfigError("embedded index is missing"))?;
    let index = std::str::from_utf8(index_asset.bytes)
        .map_err(|_| ConfigError("embedded index is not UTF-8"))?;
    if index.matches(BASE_TOKEN).count() != 1 {
        return Err(ConfigError("embedded base placeholder is invalid"));
    }
    let index: Arc<str> = index.replace(BASE_TOKEN, &config.base_path).into();
    let data = BrowserConfig {
        graphql_endpoint: &config.graphql_endpoint,
        subscription_endpoint: config.subscription_endpoint.as_deref(),
        title: config.title.as_deref(),
        max_rows: config.max_rows,
    };
    let browser_json: Arc<str> = serde_json::to_string(&data)
        .map_err(|_| ConfigError("cannot serialize browser configuration"))?
        .into();
    let state = UiState {
        config: Arc::new(config),
        index,
        browser_json,
    };
    Ok(Router::new()
        .route("/", get(index_handler))
        .route("/dduroc-config.json", get(config_handler))
        .route("/{*path}", get(asset_handler))
        .with_state(state))
}
/// Mount at the host's internal `prefix`, including a slash redirect. The
/// configured `base_path` remains the browser-visible external path, which may
/// include a reverse-proxy prefix. Merge the result into the host router.
pub fn mount(prefix: &str, config: UiConfig) -> Result<Router, ConfigError> {
    config.validate()?;
    if !prefix.starts_with('/') || prefix.contains("//") {
        return Err(ConfigError("mount prefix must be an absolute path"));
    }
    let base = if prefix == "/" {
        "/".to_owned()
    } else {
        format!("{}/", prefix.trim_end_matches('/'))
    };
    let mut validate_prefix = config.clone();
    validate_prefix.base_path = base.clone();
    validate_prefix.validate()?;
    let target = config.base_path.clone();
    let inner = router(config)?;
    if base == "/" {
        return Ok(inner);
    }
    let short = base.trim_end_matches('/').to_owned();
    Ok(Router::new()
        .route(
            &short,
            get(move |OriginalUri(uri): OriginalUri| async move {
                let mut location = target;
                if let Some(query) = uri.query() {
                    location.push('?');
                    location.push_str(query);
                }
                match HeaderValue::from_str(&location) {
                    Ok(value) => (
                        StatusCode::PERMANENT_REDIRECT,
                        [
                            (header::LOCATION, value),
                            (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
                        ],
                    )
                        .into_response(),
                    Err(_) => StatusCode::BAD_REQUEST.into_response(),
                }
            }),
        )
        .nest(&base, inner))
}
async fn index_handler(State(state): State<UiState>, OriginalUri(uri): OriginalUri) -> Response {
    if state.config.base_path != "/" && uri.path() == state.config.base_path.trim_end_matches('/') {
        let mut target = state.config.base_path.clone();
        if let Some(query) = uri.query() {
            target.push('?');
            target.push_str(query);
        }
        return match HeaderValue::from_str(&target) {
            Ok(value) => (
                StatusCode::PERMANENT_REDIRECT,
                [
                    (header::LOCATION, value),
                    (header::CACHE_CONTROL, HeaderValue::from_static("no-store")),
                ],
            )
                .into_response(),
            Err(_) => StatusCode::BAD_REQUEST.into_response(),
        };
    }
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        state.index.to_string(),
    )
        .into_response()
}
async fn config_handler(State(state): State<UiState>) -> Response {
    (
        [
            (header::CONTENT_TYPE, "application/json; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        state.browser_json.to_string(),
    )
        .into_response()
}
fn valid_asset(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 2048
        && path.split('/').all(|part| {
            !part.is_empty()
                && part != "."
                && part != ".."
                && part
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~'))
        })
}
async fn asset_handler(
    State(state): State<UiState>,
    Path(path): Path<String>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response {
    if !valid_asset(&path) {
        return StatusCode::NOT_FOUND.into_response();
    }
    if let Some(asset) = ASSETS
        .iter()
        .find(|asset| asset.path == path && asset.path != "index.html")
    {
        return (
            [
                (header::CONTENT_TYPE, asset.content_type),
                (
                    header::CACHE_CONTROL,
                    if asset.hashed {
                        "public, max-age=31536000, immutable"
                    } else {
                        "no-cache"
                    },
                ),
                (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
            ],
            asset.bytes,
        )
            .into_response();
    }
    let accepts_html = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value
                .split(',')
                .any(|part| part.trim_start().starts_with("text/html"))
        });
    if accepts_html
        && !path.starts_with("assets/")
        && !path.rsplit('/').next().unwrap_or_default().contains('.')
    {
        return index_handler(State(state), OriginalUri(uri)).await;
    }
    StatusCode::NOT_FOUND.into_response()
}
