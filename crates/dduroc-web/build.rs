use sha2::{Digest, Sha256};
use std::{
    env, fs,
    path::{Path, PathBuf},
    process::Command,
};

fn files(root: &Path, path: &Path, out: &mut Vec<PathBuf>) {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
        Err(error) => panic!("inspect UI input {}: {error}", path.display()),
    };
    assert!(
        !metadata.file_type().is_symlink(),
        "UI inputs cannot be symlinks: {}",
        path.display()
    );
    if metadata.is_dir() {
        let mut entries: Vec<_> = fs::read_dir(path)
            .expect("read UI directory")
            .map(|x| x.expect("read UI entry").path())
            .collect();
        entries.sort();
        for entry in entries {
            files(root, &entry, out);
        }
    } else {
        assert!(
            path.is_file(),
            "UI inputs must be regular files: {}",
            path.display()
        );
        out.push(path.strip_prefix(root).expect("UI path").to_path_buf());
    }
}
fn sources(root: &Path) -> Vec<PathBuf> {
    let mut paths = Vec::new();
    for name in [
        "src",
        "index.html",
        "run.mjs",
        ".npmrc",
        "LICENSE-THIRD-PARTY.txt",
        "package.json",
        "package-lock.json",
        "vite.config.ts",
        "vite.config.js",
        "tsconfig.json",
        "tsconfig.node.json",
        "svelte.config.js",
        "svelte.config.ts",
    ] {
        files(root, &root.join(name), &mut paths);
    }
    paths.sort();
    paths
}
fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
fn source_hash(root: &Path, paths: &[PathBuf]) -> String {
    let mut digest = Sha256::new();
    for path in paths {
        let name = path.to_str().expect("UTF-8 UI path").replace('\\', "/");
        let data = fs::read(root.join(path)).expect("read UI source");
        digest.update((name.len() as u64).to_le_bytes());
        digest.update(name.as_bytes());
        digest.update((data.len() as u64).to_le_bytes());
        digest.update(data);
    }
    format!("{:x}", digest.finalize())
}
fn manifest(root: &Path, fingerprint: &str) -> serde_json::Value {
    let mut paths = Vec::new();
    files(root, root, &mut paths);
    paths.sort();
    let mut assets = serde_json::Map::new();
    for path in paths {
        let name = path.to_str().expect("UTF-8 asset path").replace('\\', "/");
        if name != "build-manifest.json" {
            assets.insert(
                name,
                hash(&fs::read(root.join(path)).expect("read asset")).into(),
            );
        }
    }
    serde_json::json!({"version":1,"sourceSha256":fingerprint,"assets":assets})
}
fn run(command: &mut Command, what: &str) {
    let status = command.status().unwrap_or_else(|error| {
        panic!("{what} requires Node and npm when feature build-ui is enabled: {error}")
    });
    assert!(status.success(), "{what} failed: {status}");
}
struct ScratchCleanup(Vec<PathBuf>);
impl Drop for ScratchCleanup {
    fn drop(&mut self) {
        for path in &self.0 {
            if path.exists() {
                let _ = fs::remove_dir_all(path);
            }
        }
    }
}
fn main() {
    println!("cargo:rerun-if-env-changed=DDUROC_VERIFY_PREBUILT");
    println!("cargo:rerun-if-env-changed=PATH");
    for path in [
        "ui/src",
        "ui/index.html",
        "ui/run.mjs",
        "ui/.npmrc",
        "ui/LICENSE-THIRD-PARTY.txt",
        "ui/package.json",
        "ui/package-lock.json",
        "ui/vite.config.ts",
        "ui/vite.config.js",
        "ui/tsconfig.json",
        "ui/tsconfig.node.json",
        "ui/svelte.config.js",
        "ui/svelte.config.ts",
        "ui/dist",
    ] {
        if Path::new(path).exists() {
            println!("cargo:rerun-if-changed={path}");
        }
    }
    let ui = PathBuf::from("ui");
    let source_files = sources(&ui);
    assert!(
        ui.join("package-lock.json").is_file() && ui.join("package.json").is_file(),
        "packaged UI source and lockfile are required"
    );
    let fingerprint = source_hash(&ui, &source_files);
    let out = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR"));
    let rebuilding = env::var_os("CARGO_FEATURE_BUILD_UI").is_some();
    let dist = if rebuilding {
        let scratch = out.join("ui-source");
        if scratch.exists() {
            fs::remove_dir_all(&scratch).expect("clear UI scratch");
        }
        fs::create_dir_all(&scratch).expect("create UI scratch");
        for path in &source_files {
            let target = scratch.join(path);
            fs::create_dir_all(target.parent().unwrap()).expect("create source directory");
            fs::copy(ui.join(path), target).expect("copy UI source");
        }
        let cache = out.join("npm-cache");
        let _cleanup = ScratchCleanup(vec![scratch.clone(), cache.clone()]);
        let destination = out.join("dduroc-ui-dist");
        if destination.exists() {
            fs::remove_dir_all(&destination).expect("clear UI output");
        }
        let envs = [
            ("CARBON_TELEMETRY_DISABLED", "1"),
            ("IBM_TELEMETRY_DISABLED", "true"),
            ("npm_config_ignore_scripts", "true"),
            ("NODE_ENV", "production"),
        ];
        run(
            Command::new("npm")
                .current_dir(&scratch)
                .envs(envs)
                .args([
                    "ci",
                    "--ignore-scripts",
                    "--no-audit",
                    "--no-fund",
                    "--include=dev",
                    "--cache",
                ])
                .arg(&cache),
            "npm ci",
        );
        run(
            Command::new("npm")
                .current_dir(&scratch)
                .envs(envs)
                .args(["--ignore-scripts", "run", "build", "--", "--outDir"])
                .arg(out.join("dduroc-ui-dist")),
            "npm run build",
        );
        let dist = out.join("dduroc-ui-dist");
        let value = manifest(&dist, &fingerprint);
        fs::write(
            dist.join("build-manifest.json"),
            serde_json::to_vec_pretty(&value).unwrap(),
        )
        .expect("write UI manifest");
        if env::var_os("DDUROC_VERIFY_PREBUILT").is_some() {
            let existing: serde_json::Value = serde_json::from_slice(
                &fs::read(ui.join("dist/build-manifest.json"))
                    .expect("prebuilt UI manifest missing"),
            )
            .expect("invalid prebuilt UI manifest");
            assert_eq!(existing, value, "rebuilt UI differs from prebuilt assets");
        }
        dist
    } else {
        let dist = ui.join("dist");
        let actual = manifest(&dist, &fingerprint);
        let expected: serde_json::Value = serde_json::from_slice(
            &fs::read(dist.join("build-manifest.json"))
                .expect("prebuilt UI missing; enable build-ui to rebuild"),
        )
        .expect("invalid prebuilt UI manifest");
        assert_eq!(
            actual, expected,
            "prebuilt UI does not match packaged sources or manifest"
        );
        dist
    };
    let mut paths = Vec::new();
    files(&dist, &dist, &mut paths);
    paths.sort();
    let mut generated = String::from(
        "struct Asset { path: &'static str, bytes: &'static [u8], content_type: &'static str, hashed: bool }\nstatic ASSETS: &[Asset] = &[\n",
    );
    for path in paths {
        let name = path.to_str().expect("UTF-8 asset path").replace('\\', "/");
        if name == "build-manifest.json" {
            continue;
        }
        let mime = match path.extension().and_then(|s| s.to_str()) {
            Some("html") => "text/html; charset=utf-8",
            Some("js") => "text/javascript; charset=utf-8",
            Some("css") => "text/css; charset=utf-8",
            Some("svg") => "image/svg+xml",
            Some("txt") => "text/plain; charset=utf-8",
            Some("woff2") => "font/woff2",
            Some("woff") => "font/woff",
            Some("ttf") => "font/ttf",
            Some("png") => "image/png",
            Some("ico") => "image/x-icon",
            _ => "application/octet-stream",
        };
        let file = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
        let bytes = file.as_bytes();
        let hashed = name.starts_with("assets/")
            && bytes.len() > 9
            && bytes[bytes.len() - 9] == b'-'
            && bytes[bytes.len() - 8..]
                .iter()
                .all(|b| b.is_ascii_alphanumeric() || *b == b'_' || *b == b'-');
        let absolute = fs::canonicalize(dist.join(&path)).expect("asset path");
        generated.push_str(&format!("Asset {{ path: {name:?}, bytes: include_bytes!({:?}), content_type: {mime:?}, hashed: {hashed} }},\n", absolute.to_str().expect("UTF-8 path")));
    }
    generated.push_str("];\n");
    fs::write(out.join("assets.rs"), generated).expect("write asset index");
}
