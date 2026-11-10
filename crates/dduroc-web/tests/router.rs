use axum::{
    Router,
    body::{Body, to_bytes},
    http::{Request, StatusCode, header},
};
use dduroc_web::{UiConfig, router};
use tower::ServiceExt;

#[tokio::test]
async fn nested_index_config_and_missing_assets() {
    let app = Router::new().nest(
        "/logs/",
        router(UiConfig {
            base_path: "/logs/".into(),
            graphql_endpoint: "graphql".into(),
            title: Some("</script>\u{2028}demo".into()),
            ..Default::default()
        })
        .unwrap(),
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/logs/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let bytes = to_bytes(response.into_body(), 2_000_000).await.unwrap();
    let html = std::str::from_utf8(&bytes).unwrap();
    assert!(html.contains("<base href=\"/logs/\">"));
    assert!(!html.contains("</script>\u{2028}demo"));
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/logs/dduroc-config.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    let data = to_bytes(response.into_body(), 4096).await.unwrap();
    let config: serde_json::Value = serde_json::from_slice(&data).unwrap();
    assert_eq!(config["graphqlEndpoint"], "graphql");
    assert_eq!(config["title"], "</script>\u{2028}demo");
    let response = app
        .oneshot(
            Request::builder()
                .uri("/logs/assets/unknown")
                .header(header::ACCEPT, "text/html")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}
#[test]
fn rejects_unsafe_mount_paths_and_endpoint_schemes() {
    for base in ["logs/", "/logs", "/a/../", "/a/%2e/", "/a/\"/"] {
        let config = UiConfig {
            base_path: base.into(),
            ..Default::default()
        };
        assert!(config.validate().is_err(), "{base}");
    }
    for endpoint in [
        "javascript:alert(1)",
        "//other.invalid/x",
        "https://user@host.invalid/x",
    ] {
        let config = UiConfig {
            graphql_endpoint: endpoint.into(),
            ..Default::default()
        };
        assert!(config.validate().is_err(), "{endpoint}");
    }
}

#[tokio::test]
async fn mount_redirects_and_preserves_external_proxy_path() {
    let app = dduroc_web::mount(
        "/logs",
        UiConfig {
            base_path: "/proxy/logs/".into(),
            ..Default::default()
        },
    )
    .unwrap();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/logs?x=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PERMANENT_REDIRECT);
    assert_eq!(response.headers()[header::LOCATION], "/proxy/logs/?x=1");
    let nested = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/logs/history/detail")
                .header(header::ACCEPT, "text/html")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(nested.status(), StatusCode::OK);
    let response = app
        .oneshot(
            Request::builder()
                .uri("/logs/")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
