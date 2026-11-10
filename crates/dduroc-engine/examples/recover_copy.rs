//! Offline tool; neither the original nor evidence copy is opened by raft.
//! `cargo run -p dduroc-engine --features recovery --example recover_copy --
//!   SOURCE NEW_DEST [--critical-root ROOT] [--default-root ROOT]
//!   [--telemetry-root ROOT] [--max-bytes N] [--max-entries N]
//!   [--deadline-seconds N] [--repair-copy-allow-data-loss]`
use dduroc_engine::recovery::{RecoveryMode, RecoveryOptions, recover_copy};
use dduroc_engine::{ChannelConfig, StorageClass, StoreConfig};
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let source = PathBuf::from(
        args.next()
            .ok_or("expected SOURCE NEW_DEST and optional flags")?,
    );
    let destination = PathBuf::from(args.next().ok_or("expected NEW_DEST")?);
    let mut config = StoreConfig::new(&source);
    let mut options = RecoveryOptions::default();
    while let Some(flag) = args.next() {
        let flag = flag.to_str().ok_or("non-UTF8 flag")?;
        match flag {
            "--repair-copy-allow-data-loss" => options.mode = RecoveryMode::RepairCopyAllowDataLoss,
            "--critical-root" | "--default-root" | "--telemetry-root" => {
                let root = PathBuf::from(args.next().ok_or("missing custom root")?);
                let class = match flag {
                    "--critical-root" => StorageClass::Critical,
                    "--telemetry-root" => StorageClass::Telemetry,
                    _ => StorageClass::Default,
                };
                config = config.channel(
                    class,
                    ChannelConfig {
                        custom_root: Some(root),
                        ..if class == StorageClass::Critical {
                            ChannelConfig::critical(16 << 20)
                        } else {
                            ChannelConfig::new(16 << 20)
                        }
                    },
                );
            }
            "--max-bytes" | "--max-entries" | "--deadline-seconds" => {
                let value = args.next().ok_or("missing limit value")?;
                let value = value.to_str().ok_or("non-UTF8 limit")?;
                match flag {
                    "--max-bytes" => options.max_source_bytes = value.parse()?,
                    "--max-entries" => options.max_entries = value.parse()?,
                    _ => {
                        options.deadline = Some(
                            Instant::now()
                                .checked_add(Duration::from_secs(value.parse()?))
                                .ok_or("deadline overflow")?,
                        )
                    }
                }
            }
            _ => return Err(format!("unknown flag {flag}").into()),
        }
    }
    eprintln!(
        "Offline only: supply every original custom root. Source: {}. New copy: {}.",
        source.display(),
        destination.display()
    );
    eprintln!(
        "Requires two full copies plus recovery overhead; cooperative deadline cannot interrupt Engine::open or blocked I/O."
    );
    if options.mode == RecoveryMode::RepairCopyAllowDataLoss {
        eprintln!(
            "LOSS POSSIBLE: tolerant repair can discard ACKed records. Only candidate is repaired; never activate automatically."
        );
    }
    let report = recover_copy(&config, &destination, &options, &AtomicBool::new(false))?;
    println!("{}", serde_json::to_string_pretty(&report)?);
    if !report.strict_inspection_succeeded {
        std::process::exit(2);
    }
    Ok(())
}
