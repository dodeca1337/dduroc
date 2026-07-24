//! Файловые примитивы с явной семантикой долговечности.
//!
//! Правила, которым подчиняется весь движок:
//!
//! - **`fdatasync`, а не `fsync`.** Сегменты преаллоцированы (`fallocate`),
//!   поэтому append не меняет размер файла и синхронизировать метаданные
//!   инода незачем. Исключение — файлы, которые растут (метаданные): там
//!   `fdatasync` тоже достаточен, он синхронизирует метаданные, необходимые
//!   для чтения данных (в том числе размер).
//! - **Имя файла долговечно только после `fsync` каталога.** Без этого
//!   после обрыва питания файл может существовать, но не иметь имени.
//! - **Замена файла — только через `rename`.** Перезапись на месте оставляет
//!   окно, в котором файл наполовину старый, наполовину новый.

use crate::error::{IoContext, Result};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

/// Синхронизировать каталог: делает долговечными операции над **именами**
/// (создание, переименование, удаление файлов внутри него).
pub fn sync_dir(dir: &Path) -> Result<()> {
    let f = File::open(dir).ctx_path("открытие каталога", dir)?;
    // Часть ФС не поддерживает fsync на каталоге (например, некоторые
    // сетевые); EINVAL там означает «нечего синхронизировать», а не сбой.
    match rustix::fs::fsync(&f) {
        Ok(()) | Err(rustix::io::Errno::INVAL) => Ok(()),
        Err(e) => Err(e).ctx_path("fsync каталога", dir),
    }
}

/// `fdatasync` файла.
pub fn sync_data(file: &File, path: &Path) -> Result<()> {
    rustix::fs::fdatasync(file).ctx_path("fdatasync", path)
}

/// Создать каталог со всеми родителями и сделать имена долговечными.
pub fn create_dir_all_synced(dir: &Path) -> Result<()> {
    if dir.is_dir() {
        return Ok(());
    }
    std::fs::create_dir_all(dir).ctx_path("создание каталога", dir)?;
    // Долговечно должно стать имя каждого созданного звена, поэтому
    // синхронизируем цепочку родителей снизу вверх.
    let mut cur = Some(dir);
    while let Some(d) = cur {
        if let Some(parent) = d.parent() {
            if parent.as_os_str().is_empty() {
                break;
            }
            sync_dir(parent)?;
            cur = Some(parent);
        } else {
            break;
        }
    }
    Ok(())
}

/// Атомарно заменить содержимое файла.
///
/// Порядок: запись во временный файл → `fdatasync` → `rename` →
/// `fsync` каталога. Обрыв питания в любой точке оставляет на диске либо
/// целиком старое содержимое, либо целиком новое.
pub fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let dir = path.parent().unwrap_or(Path::new("."));
    let tmp = tmp_path(path);

    {
        let mut f = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&tmp)
            .ctx_path("создание временного файла", &tmp)?;
        f.write_all(bytes).ctx_path("запись", &tmp)?;
        sync_data(&f, &tmp)?;
    }

    // Ошибку rename нельзя проглатывать: временный файл остался бы мусором,
    // а вызывающий считал бы данные записанными.
    if let Err(e) = std::fs::rename(&tmp, path) {
        let _ = std::fs::remove_file(&tmp);
        return Err(e).ctx_path("переименование в", path);
    }
    sync_dir(dir)
}

/// Путь временного файла рядом с целевым: `rename` работает только в
/// пределах одной ФС, поэтому /tmp не подходит.
fn tmp_path(path: &Path) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(".tmp");
    path.with_file_name(name)
}

/// Прочитать файл целиком; `Ok(None)`, если его нет.
pub fn read_optional(path: &Path) -> Result<Option<Vec<u8>>> {
    match std::fs::read(path) {
        Ok(v) => Ok(Some(v)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e).ctx_path("чтение", path),
    }
}

/// Удалить файл, если он есть, и сделать удаление долговечным.
pub fn remove_synced(path: &Path) -> Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e).ctx_path("удаление", path),
    }
    sync_dir(path.parent().unwrap_or(Path::new(".")))
}

/// Прибрать оставшиеся от прерванных операций `*.tmp` в каталоге.
///
/// Такой файл — след обрыва питания между созданием временного файла и
/// `rename`: его содержимое заведомо неполное и никем не адресуется.
pub fn sweep_tmp(dir: &Path) -> Result<usize> {
    let mut removed = 0;
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(e) => return Err(e).ctx_path("чтение каталога", dir),
    };
    for entry in entries {
        let entry = entry.ctx_path("обход каталога", dir)?;
        let path = entry.path();
        if path.extension().is_some_and(|e| e == "tmp") && path.is_file() {
            std::fs::remove_file(&path).ctx_path("удаление временного файла", &path)?;
            removed += 1;
        }
    }
    if removed > 0 {
        sync_dir(dir)?;
    }
    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn atomic_write_replaces_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta");

        assert_eq!(read_optional(&path).unwrap(), None);
        write_atomic(&path, b"first").unwrap();
        assert_eq!(
            read_optional(&path).unwrap().as_deref(),
            Some(&b"first"[..])
        );
        write_atomic(&path, b"second-longer").unwrap();
        assert_eq!(
            read_optional(&path).unwrap().as_deref(),
            Some(&b"second-longer"[..])
        );

        // Временный файл не остаётся мусором.
        let leftovers: Vec<_> = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "tmp"))
            .collect();
        assert!(leftovers.is_empty(), "остался временный файл");
    }

    #[test]
    fn tmp_is_a_sibling_for_same_filesystem_rename() {
        let p = Path::new("/data/logs/ns/epochs.bin");
        assert_eq!(tmp_path(p), Path::new("/data/logs/ns/epochs.bin.tmp"));
    }

    #[test]
    fn sweep_removes_only_tmp_files() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("a.seg"), b"keep").unwrap();
        std::fs::write(dir.path().join("a.seg.tmp"), b"drop").unwrap();
        std::fs::write(dir.path().join("meta.tmp"), b"drop").unwrap();

        assert_eq!(sweep_tmp(dir.path()).unwrap(), 2);
        assert!(dir.path().join("a.seg").exists());
        assert!(!dir.path().join("a.seg.tmp").exists());
        assert!(!dir.path().join("meta.tmp").exists());

        // Идемпотентность: второй проход ничего не находит.
        assert_eq!(sweep_tmp(dir.path()).unwrap(), 0);
        // Отсутствующий каталог — не ошибка.
        assert_eq!(sweep_tmp(&dir.path().join("nope")).unwrap(), 0);
    }

    #[test]
    fn create_dir_all_is_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let nested = dir.path().join("a/b/c");
        create_dir_all_synced(&nested).unwrap();
        assert!(nested.is_dir());
        create_dir_all_synced(&nested).unwrap();
    }

    #[test]
    fn remove_missing_is_ok() {
        let dir = tempfile::tempdir().unwrap();
        remove_synced(&dir.path().join("absent")).unwrap();
    }
}
