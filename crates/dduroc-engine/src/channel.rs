//! Канал — класс хранения внутри неймспейса.
//!
//! Физически это поддиректория с сегментами, логически — набор политик:
//! бюджет ротации, размер сегмента и блока, сжатие и, главное, долговечность.
//! Формат записей во всех каналах одинаков — различаются только политики,
//! поэтому «критическое» и «некритическое» живёт в одном движке.

use crate::error::{Error, Result};
use dduroc_format::Compression;
use std::time::Duration;

/// Политика синхронизации с диском.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// `fdatasync` при первой же возможности после записи.
    ///
    /// Это **group commit**, а не sync на событие: writer забирает из очереди
    /// всё накопившееся, пишет одним блоком и синхронизирует один раз. Всплеск
    /// из сотни критических событий стоит одного `fdatasync` (~1–10 мс на
    /// eMMC), а не сотни.
    Immediate,
    /// Не чаще указанного интервала. Окно потери при обрыве питания равно
    /// интервалу.
    Interval(Duration),
    /// Только при запечатывании сегмента и завершении работы.
    Relaxed,
}

impl Durability {
    /// Минимальный интервал между `fdatasync`.
    pub fn min_interval(self) -> Option<Duration> {
        match self {
            Durability::Immediate => Some(Duration::ZERO),
            Durability::Interval(d) => Some(d),
            Durability::Relaxed => None,
        }
    }
}

/// Настройки канала.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelConfig {
    /// Имя канала = имя поддиректории.
    pub name: String,
    /// Бюджет ротации: суммарный размер сегментов канала.
    pub budget_bytes: u64,
    /// Размер одного сегмента (он же — шаг преаллокации).
    pub segment_bytes: u64,
    /// Порог закрытия блока.
    pub block_max_bytes: usize,
    /// Максимальная задержка выталкивания неполного блока.
    pub flush_interval: Duration,
    pub durability: Durability,
    pub compression: Compression,
}

impl ChannelConfig {
    /// Разумные умолчания под указанный бюджет.
    pub fn new(name: impl Into<String>, budget_bytes: u64) -> Self {
        Self {
            name: name.into(),
            budget_bytes,
            segment_bytes: Self::segment_size_for(budget_bytes),
            block_max_bytes: 64 * 1024,
            flush_interval: Duration::from_secs(1),
            durability: Durability::Interval(Duration::from_secs(10)),
            compression: Compression::Lz4,
        }
    }

    /// Канал критических данных: синхронизация сразу, сжатие выключено.
    ///
    /// Сжатие на критическом канале вредно: оно заставляет копить данные ради
    /// эффективности, а копить — ровно то, чего критический канал избегает.
    pub fn critical(name: impl Into<String>, budget_bytes: u64) -> Self {
        Self {
            durability: Durability::Immediate,
            compression: Compression::None,
            block_max_bytes: 8 * 1024,
            flush_interval: Duration::from_millis(50),
            ..Self::new(name, budget_bytes)
        }
    }

    /// Размер сегмента из бюджета: примерно 1/256 бюджета, но в разумных
    /// пределах.
    ///
    /// Слишком крупные сегменты делают ротацию грубой (за раз теряется
    /// заметная доля истории) и удорожают преаллокацию; слишком мелкие плодят
    /// файлы и открытые дескрипторы. При бюджете 20 ГБ получается 80 МБ и
    /// ~256 файлов на канал.
    pub fn segment_size_for(budget_bytes: u64) -> u64 {
        const MIN: u64 = 4 * 1024 * 1024;
        const MAX: u64 = 256 * 1024 * 1024;
        (budget_bytes / 256).clamp(MIN, MAX)
    }

    /// Проверить и нормализовать конфигурацию.
    pub fn validate(&self) -> Result<()> {
        validate_component(&self.name).map_err(|reason| Error::BadChannel {
            name: self.name.clone(),
            reason,
        })?;

        // Бюджет меньше двух сегментов означает, что при запечатывании
        // единственного сегмента ротация немедленно удалила бы его — канал
        // не хранил бы ничего.
        if self.budget_bytes < self.segment_bytes * 2 {
            return Err(Error::BadChannel {
                name: self.name.clone(),
                reason: "бюджет меньше двух сегментов — ротация съедала бы данные сразу",
            });
        }
        if self.block_max_bytes < 512 {
            return Err(Error::BadChannel {
                name: self.name.clone(),
                reason: "слишком маленький блок: накладные расходы съедят экономию",
            });
        }
        if (self.block_max_bytes as u64) * 2 > self.segment_bytes {
            return Err(Error::BadChannel {
                name: self.name.clone(),
                reason: "блок сопоставим с сегментом — сегмент не вместит и пары блоков",
            });
        }
        Ok(())
    }

    /// Сколько сегментов помещается в бюджет.
    pub fn max_segments(&self) -> u64 {
        (self.budget_bytes / self.segment_bytes).max(1)
    }
}

/// Проверка компонента пути (имя неймспейса или канала).
///
/// Имена приходят из конфигурации и кода приложения, но подставляются в путь
/// файловой системы, поэтому проверяются строго: `..`, разделители пути и
/// управляющие символы способны увести запись за пределы хранилища.
pub(crate) fn validate_component(name: &str) -> std::result::Result<(), &'static str> {
    if name.is_empty() {
        return Err("пустое имя");
    }
    if name.len() > 64 {
        return Err("длиннее 64 байт");
    }
    if name == "." || name == ".." {
        return Err("зарезервированное имя");
    }
    if name.starts_with('.') {
        return Err("имя не может начинаться с точки");
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    {
        return Err("допустимы только ASCII-буквы, цифры, '-', '_' и '.'");
    }
    // Расширение .tmp зарезервировано за незавершёнными атомарными записями,
    // .seg — за сегментами: каталог с таким именем сбил бы уборку и скан.
    if name.ends_with(".tmp") || name.ends_with(".seg") || name.ends_with(".corrupt") {
        return Err("зарезервированное расширение");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_scale_with_budget() {
        // 20 ГБ → 80 МБ сегменты, 256 файлов.
        let c = ChannelConfig::new("default", 20 * 1024 * 1024 * 1024);
        assert_eq!(c.segment_bytes, 80 * 1024 * 1024);
        assert_eq!(c.max_segments(), 256);
        c.validate().unwrap();

        // Маленький бюджет упирается в нижнюю границу сегмента.
        let c = ChannelConfig::new("small", 64 * 1024 * 1024);
        assert_eq!(c.segment_bytes, 4 * 1024 * 1024);
        assert_eq!(c.max_segments(), 16);
        c.validate().unwrap();

        // Огромный — в верхнюю.
        let c = ChannelConfig::new("big", 500 * 1024 * 1024 * 1024);
        assert_eq!(c.segment_bytes, 256 * 1024 * 1024);
        c.validate().unwrap();
    }

    #[test]
    fn critical_channel_avoids_buffering() {
        let c = ChannelConfig::critical("critical", 256 * 1024 * 1024);
        assert_eq!(c.durability, Durability::Immediate);
        assert_eq!(c.compression, Compression::None);
        assert!(c.flush_interval < Duration::from_secs(1));
        c.validate().unwrap();
    }

    #[test]
    fn degenerate_configs_rejected() {
        let mut c = ChannelConfig::new("x", 64 * 1024 * 1024);
        c.budget_bytes = c.segment_bytes; // ровно один сегмент
        assert!(c.validate().is_err(), "бюджет в один сегмент бессмыслен");

        let mut c = ChannelConfig::new("x", 64 * 1024 * 1024);
        c.block_max_bytes = 16;
        assert!(c.validate().is_err());

        let mut c = ChannelConfig::new("x", 64 * 1024 * 1024);
        c.block_max_bytes = c.segment_bytes as usize;
        assert!(c.validate().is_err());
    }

    #[test]
    fn path_components_are_validated_strictly() {
        for good in ["default", "orc-radio-0", "apt_x", "a.b", "A1"] {
            assert!(validate_component(good).is_ok(), "{good} должно проходить");
        }
        for bad in [
            "",
            "..",
            ".",
            ".hidden",
            "a/b",
            "a\\b",
            "../etc",
            "a b",
            "имя",
            "a\0b",
            "x.tmp",
            "x.seg",
            "x.corrupt",
            "\n",
        ] {
            assert!(
                validate_component(bad).is_err(),
                "{bad:?} обязано отвергаться"
            );
        }
        assert!(validate_component(&"x".repeat(64)).is_ok());
        assert!(validate_component(&"x".repeat(65)).is_err());
    }

    #[test]
    fn durability_intervals() {
        assert_eq!(Durability::Immediate.min_interval(), Some(Duration::ZERO));
        assert_eq!(
            Durability::Interval(Duration::from_secs(5)).min_interval(),
            Some(Duration::from_secs(5))
        );
        assert_eq!(Durability::Relaxed.min_interval(), None);
    }
}
