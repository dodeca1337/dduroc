//! Инвентарь сегментов канала и ротация по бюджету.
//!
//! Ротация — это `unlink` самого старого сегмента, когда суммарный размер
//! превышает бюджет. Никакой перезаписи: удаление целого файла на eMMC/SD
//! отдаёт FTL сразу целые erase-блоки, а не размазывает работу по всей
//! трансляции адресов.
//!
//! Считается **выделенный** размер файлов, а не объём полезных данных:
//! флеш занят преаллокацией, и бюджет должен отражать реальную занятость.

use crate::error::{IoContext, Result};
use crate::fsutil;
use dduroc_format::segment::SegmentName;
use std::collections::VecDeque;
use std::path::{Path, PathBuf};

/// Сегмент в инвентаре.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentEntry {
    pub name: SegmentName,
    /// Размер файла на диске (с учётом преаллокации).
    pub size: u64,
}

impl SegmentEntry {
    pub fn path(&self, dir: &Path) -> PathBuf {
        dir.join(self.name.to_string())
    }
}

/// Упорядоченный по времени список сегментов канала.
#[derive(Debug, Default)]
pub struct Inventory {
    /// Отсортирован по возрастанию `(boot, base)` — порядок ротации.
    segments: VecDeque<SegmentEntry>,
    total: u64,
}

impl Inventory {
    /// Прочитать каталог канала.
    ///
    /// Посторонние файлы игнорируются: каталог может содержать что угодно,
    /// и падать из-за чужого `README` движок не должен.
    pub fn scan(dir: &Path) -> Result<Self> {
        let mut segments = Vec::new();
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self::default());
            }
            Err(e) => return Err(e).ctx_path("чтение каталога канала", dir),
        };

        for entry in entries {
            let entry = entry.ctx_path("обход каталога", dir)?;
            let file_name = entry.file_name();
            let Some(name) = file_name.to_str().and_then(SegmentName::parse) else {
                continue;
            };
            // Симлинк на чужой файл не должен попасть под ротацию: удаление
            // пошло бы по ссылке, а учёт размера был бы неверным.
            let meta = entry.metadata().ctx_path("stat", &entry.path())?;
            if !meta.is_file() {
                continue;
            }
            segments.push(SegmentEntry {
                name,
                size: meta.len(),
            });
        }

        segments.sort_by_key(|s| s.name);
        let total = segments.iter().map(|s| s.size).sum();
        Ok(Self {
            segments: segments.into(),
            total,
        })
    }

    /// Только имена сегментов, в порядке времени.
    ///
    /// Отдельно от [`Inventory::scan`], потому что размеры стоят `stat` на
    /// каждый файл, а читателю они не нужны вовсе: отбор сегментов по окну
    /// идёт по именам. При сотнях сегментов в канале и тысячах каналов это
    /// разница между одним `readdir` и сотнями тысяч системных вызовов на
    /// запрос.
    pub fn scan_names(dir: &Path) -> Result<Vec<SegmentName>> {
        let entries = match std::fs::read_dir(dir) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e).ctx_path("чтение каталога канала", dir),
        };
        let mut names: Vec<SegmentName> = entries
            .flatten()
            .filter_map(|e| e.file_name().to_str().and_then(SegmentName::parse))
            .collect();
        names.sort_unstable();
        Ok(names)
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn len(&self) -> usize {
        self.segments.len()
    }

    /// Суммарный размер сегментов.
    pub fn total_bytes(&self) -> u64 {
        self.total
    }

    pub fn iter(&self) -> impl Iterator<Item = &SegmentEntry> {
        self.segments.iter()
    }

    pub fn oldest(&self) -> Option<&SegmentEntry> {
        self.segments.front()
    }

    pub fn newest(&self) -> Option<&SegmentEntry> {
        self.segments.back()
    }

    /// Добавить только что созданный сегмент (он самый новый).
    pub fn push_newest(&mut self, entry: SegmentEntry) {
        self.total += entry.size;
        self.segments.push_back(entry);
    }

    /// Уточнить размер сегмента (после запечатывания файл обрезается).
    pub fn update_size(&mut self, name: SegmentName, size: u64) {
        if let Some(e) = self.segments.iter_mut().find(|e| e.name == name) {
            self.total = self.total - e.size + size;
            e.size = size;
        }
    }

    /// Удалять самые старые сегменты, пока сумма превышает бюджет.
    ///
    /// `protect` — сегмент, который удалять нельзя (активный: в него пишут).
    /// Возвращает число удалённых.
    pub fn enforce_budget(
        &mut self,
        dir: &Path,
        budget: u64,
        protect: Option<SegmentName>,
    ) -> Result<usize> {
        let mut removed = 0;
        while self.total > budget {
            let Some(front) = self.segments.front() else {
                break;
            };
            // Единственный оставшийся сегмент — активный: удалять нечего,
            // иначе запись потеряла бы файл под собой.
            if Some(front.name) == protect {
                break;
            }
            let path = front.path(dir);
            match std::fs::remove_file(&path) {
                Ok(()) => {}
                // Файл уже исчез (ручная уборка, гонка) — просто забываем его.
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).ctx_path("удаление сегмента", &path),
            }
            let entry = self.segments.pop_front().expect("front проверен выше");
            self.total = self.total.saturating_sub(entry.size);
            removed += 1;
        }
        if removed > 0 {
            fsutil::sync_dir(dir)?;
        }
        Ok(removed)
    }

    /// Забыть сегмент (после удаления миграцией или вручную).
    pub fn remove(&mut self, name: SegmentName) {
        if let Some(pos) = self.segments.iter().position(|e| e.name == name) {
            let entry = self.segments.remove(pos).expect("позиция найдена");
            self.total = self.total.saturating_sub(entry.size);
        }
    }

    /// Минимальный `boot_counter` среди сегментов — граница для уборки
    /// записей об эпохах.
    pub fn min_boot(&self) -> Option<u32> {
        self.segments.front().map(|s| s.name.boot.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dduroc_format::{BootCounter, Micros};

    fn make(dir: &Path, boot: u32, base: u64, size: usize) {
        let name = SegmentName::new(BootCounter(boot), Micros(base));
        std::fs::write(dir.join(name.to_string()), vec![0u8; size]).unwrap();
    }

    #[test]
    fn scan_orders_by_time_and_ignores_foreign_files() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 2, 100, 10);
        make(dir.path(), 1, 500, 20);
        make(dir.path(), 1, 100, 30);
        std::fs::write(dir.path().join("README"), "не сегмент".as_bytes()).unwrap();
        std::fs::write(dir.path().join("junk.seg"), "неверное имя".as_bytes()).unwrap();
        std::fs::create_dir(dir.path().join("00000009-000000000000000f.seg")).unwrap();

        let inv = Inventory::scan(dir.path()).unwrap();
        assert_eq!(inv.len(), 3, "посторонние файлы и каталоги пропущены");
        assert_eq!(inv.total_bytes(), 60);

        let order: Vec<_> = inv.iter().map(|e| (e.name.boot.0, e.name.base.0)).collect();
        assert_eq!(
            order,
            vec![(1, 100), (1, 500), (2, 100)],
            "порядок по (boot, время)"
        );
        assert_eq!(inv.oldest().unwrap().name.base, Micros(100));
        assert_eq!(inv.newest().unwrap().name.boot, BootCounter(2));
        assert_eq!(inv.min_boot(), Some(1));
    }

    #[test]
    fn missing_dir_is_empty_not_error() {
        let dir = tempfile::tempdir().unwrap();
        let inv = Inventory::scan(&dir.path().join("absent")).unwrap();
        assert!(inv.is_empty());
        assert_eq!(inv.total_bytes(), 0);
    }

    #[test]
    fn budget_removes_oldest_first() {
        let dir = tempfile::tempdir().unwrap();
        for i in 0..5u64 {
            make(dir.path(), 1, i * 100, 100);
        }
        let mut inv = Inventory::scan(dir.path()).unwrap();
        assert_eq!(inv.total_bytes(), 500);

        let removed = inv.enforce_budget(dir.path(), 250, None).unwrap();
        assert_eq!(removed, 3, "удалено ровно столько, чтобы влезть в бюджет");
        assert_eq!(inv.total_bytes(), 200);
        assert_eq!(inv.oldest().unwrap().name.base, Micros(300));
        assert!(
            !dir.path()
                .join(SegmentName::new(BootCounter(1), Micros(0)).to_string())
                .exists(),
            "старейший файл действительно удалён"
        );
    }

    #[test]
    fn active_segment_is_never_removed() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 1, 0, 1000);
        let mut inv = Inventory::scan(dir.path()).unwrap();
        let active = inv.newest().unwrap().name;

        // Бюджет заведомо превышен, но удалять нечего кроме активного.
        let removed = inv.enforce_budget(dir.path(), 10, Some(active)).unwrap();
        assert_eq!(removed, 0);
        assert_eq!(inv.len(), 1, "активный сегмент уцелел");

        // Со вторым сегментом старый удаляется, активный остаётся.
        make(dir.path(), 1, 500, 1000);
        let mut inv = Inventory::scan(dir.path()).unwrap();
        let active = inv.newest().unwrap().name;
        assert_eq!(inv.enforce_budget(dir.path(), 10, Some(active)).unwrap(), 1);
        assert_eq!(inv.len(), 1);
        assert_eq!(inv.newest().unwrap().name, active);
    }

    #[test]
    fn budget_within_limit_removes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 1, 0, 100);
        let mut inv = Inventory::scan(dir.path()).unwrap();
        assert_eq!(inv.enforce_budget(dir.path(), 1000, None).unwrap(), 0);
        assert_eq!(inv.len(), 1);
    }

    #[test]
    fn size_updates_after_seal_are_accounted() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 1, 0, 1000);
        let mut inv = Inventory::scan(dir.path()).unwrap();
        assert_eq!(inv.total_bytes(), 1000);

        // Запечатывание обрезает хвост преаллокации.
        let name = inv.newest().unwrap().name;
        inv.update_size(name, 200);
        assert_eq!(inv.total_bytes(), 200, "бюджет учитывает реальный размер");

        inv.remove(name);
        assert_eq!(inv.total_bytes(), 0);
        assert!(inv.is_empty());
    }

    #[test]
    fn vanished_file_does_not_break_rotation() {
        let dir = tempfile::tempdir().unwrap();
        make(dir.path(), 1, 0, 100);
        make(dir.path(), 1, 100, 100);
        let mut inv = Inventory::scan(dir.path()).unwrap();

        // Кто-то удалил файл мимо движка.
        std::fs::remove_file(
            dir.path()
                .join(SegmentName::new(BootCounter(1), Micros(0)).to_string()),
        )
        .unwrap();

        let removed = inv.enforce_budget(dir.path(), 50, None).unwrap();
        assert_eq!(removed, 2, "исчезнувший файл просто забыт");
        assert!(inv.is_empty());
    }
}
