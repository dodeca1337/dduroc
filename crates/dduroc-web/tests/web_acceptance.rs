//! Independent acceptance of the actual packaged assets through the public router.
use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use dduroc_web::{UiConfig, mount};
use std::path::PathBuf;
use tower::ServiceExt;

fn mounted() -> Router {
    mount(
        "/logs",
        UiConfig {
            base_path: "/proxy/logs/".into(),
            graphql_endpoint: "https://reader.example/graphql".into(),
            subscription_endpoint: Some("wss://reader.example/graphql/ws".into()),
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test]
async fn every_real_packaged_asset_is_served_unchanged_with_mime_and_cache_policy() {
    let dist = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("ui/dist");
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dist.join("build-manifest.json")).unwrap()).unwrap();
    let app = mounted();
    let mut scripts = 0;
    let mut styles = 0;
    let mut fonts = 0;
    for path in manifest["assets"].as_object().unwrap().keys() {
        if path == "index.html" {
            continue;
        }
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(format!("/logs/{path}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(
            response.headers()[header::X_CONTENT_TYPE_OPTIONS],
            "nosniff"
        );
        let content_type = response.headers()[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .to_owned();
        if path.starts_with("assets/") {
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "public, max-age=31536000, immutable",
                "{path}"
            );
        }
        let expected = std::fs::read(dist.join(path)).unwrap();
        let bytes = to_bytes(response.into_body(), expected.len() + 1)
            .await
            .unwrap();
        assert_eq!(bytes.as_ref(), expected, "{path}");
        if path.ends_with(".js") {
            scripts += 1;
            assert!(content_type.starts_with("text/javascript"));
        } else if path.ends_with(".css") {
            styles += 1;
            assert!(content_type.starts_with("text/css"));
        } else if path.ends_with(".woff2") {
            fonts += 1;
            assert_eq!(content_type, "font/woff2");
        }
    }
    assert!(scripts > 0, "a real compiled UI script must be packaged");
    assert!(styles > 0, "the real UI stylesheet must be packaged");
    assert!(fonts > 0, "IBM Plex fonts must be self-hosted");
}

#[tokio::test]
async fn mounted_router_does_not_leak_files_or_turn_unknown_assets_into_html() {
    let app = mounted();
    for path in [
        "/logs/../Cargo.toml",
        "/logs/%2e%2e/Cargo.toml",
        "/logs/assets/%2e%2e/%2e%2e/Cargo.toml",
        "/logs/%252e%252e/Cargo.toml",
        "/logs/assets/missing.js",
        "/logs/assets/missing",
        "/logs/build-manifest.json",
        "/logs/src/main.ts",
        "/logs/package-lock.json",
        "/outside/",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header(header::ACCEPT, "text/html")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
}

#[test]
fn configured_remote_endpoints_are_allowed_but_mount_syntax_cannot_inject_routes() {
    for prefix in [
        "/logs/{name}",
        "/logs/{*path}",
        "/logs/../other",
        "//host/logs",
        "logs",
    ] {
        assert!(mount(prefix, UiConfig::default()).is_err(), "{prefix}");
    }
    for max_rows in [0, 10_001, usize::MAX] {
        assert!(
            UiConfig {
                max_rows: Some(max_rows),
                ..Default::default()
            }
            .validate()
            .is_err()
        );
    }
    assert!(
        UiConfig {
            graphql_endpoint: "https://reader.example/graphql".into(),
            subscription_endpoint: Some("wss://reader.example/graphql/ws".into()),
            max_rows: Some(10_000),
            ..Default::default()
        }
        .validate()
        .is_ok()
    );
}
