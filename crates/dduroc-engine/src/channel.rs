//! Канал — класс хранения внутри неймспейса.
//!
//! Физически это поддиректория с сегментами, логически — набор политик:
//! размер сегмента и блока, сжатие и, главное, долговечность. Формат записей
//! во всех каналах одинаков — различаются только политики, поэтому
//! «критическое» и «некритическое» живёт в одном движке.
//!
//! Бюджет объявляется на **класс целиком**: «вся телеметрия — столько-то,
//! все логи — столько-то». Каналы всех неймспейсов одного класса черпают из
//! общего бюджета, и вытесняется старейший сегмент класса, в чьём бы
//! неймспейсе он ни лежал.

use crate::error::{Error, Result};
use dduroc_format::Compression;
use std::path::PathBuf;
use std::time::Duration;

/// Настройки класса хранения — политика всех его каналов.
///
/// Имени здесь нет намеренно: канал живёт в каталоге своего класса хранения
/// ([`crate::schema::StorageClass::as_str`]), и второй источник того же имени
/// в конфигурации мог бы только разойтись с первым.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChannelConfig {
    /// Бюджет **класса на всё хранилище**: суммарный размер сегментов этого
    /// класса во всех неймспейсах. При превышении вытесняется старейший
    /// сегмент класса — чей бы он ни был: молчащий сервис не держит место,
    /// которого не хватает шумному. Квота отдельного неймспейса — отдельная
    /// необязательная настройка ([`crate::store::NsQuota`]).
    pub budget_bytes: u64,
    /// Предел роста одного сегмента — граница ротации.
    ///
    /// Это **не** размер файла при создании: место резервируется окном,
    /// начиная с одного блока, и растёт восьмыми долями предела по мере
    /// записи (см. модуль `segment`). Канал, написавший сто байт, занимает
    /// на носителе один экстент, а не целый сегмент, и в бюджете класса
    /// числится тем же. Поэтому предел одновременно пишущих каналов задаёт
    /// не `budget_bytes / segment_bytes`, а то, сколько они реально
    /// записали.
    ///
    /// Фиксированный, а не производный от бюджета: бюджет общий на класс, и
    /// сегмент, выросший вместе с ним, дал бы одному шумному каналу право
    /// занять его целиком до первой ротации.
    pub segment_bytes: u64,
    /// Порог закрытия блока.
    pub block_max_bytes: usize,
    /// Максимальная задержка выталкивания неполного блока.
    pub flush_interval: Duration,
    /// Не чаще этого интервала — `fdatasync`. Окно потери при обрыве питания
    /// равно интервалу; синхронизация при запечатывании сегмента и на
    /// завершении происходит в любом случае.
    ///
    /// `ZERO` — синхронизировать сразу после каждой групповой фиксации.
    /// Это group commit, а не sync на запись: writer забирает из очереди всё
    /// накопившееся, пишет одним блоком и синхронизирует один раз — всплеск
    /// из сотни событий стоит одного `fdatasync` (~1–10 мс на eMMC). Так
    /// работает критический канал, и для него это не настройка, а
    /// определение: ненулевой интервал у критического класса отвергается при
    /// открытии хранилища.
    pub sync_interval: Duration,
    pub compression: Compression,
    /// Собственный корень класса. `None` — общий корень хранилища.
    ///
    /// Нужен, когда классу положен другой носитель: критические данные — на
    /// защищённый раздел (jffs2), тяжёлая телеметрия — на большой. Раскладка
    /// внутри корня та же: `<корень>/<неймспейс>/<класс>/`. Живой читатель
    /// (`store.reader()`) знает все корни от самого хранилища; дампу их
    /// называют разом (`Reader::open_dump`). Смена корня не переносит
    /// историю: новые сегменты пишутся в новое место, старое остаётся
    /// читаемым как ещё один корень дампа, но в бюджете класса больше не
    /// учитывается.
    pub custom_root: Option<PathBuf>,
}

impl ChannelConfig {
    /// Разумные умолчания под указанный бюджет класса.
    pub fn new(budget_bytes: u64) -> Self {
        Self {
            budget_bytes,
            segment_bytes: 8 * 1024 * 1024,
            block_max_bytes: 64 * 1024,
            flush_interval: Duration::from_secs(1),
            sync_interval: Duration::from_secs(10),
            compression: Compression::Lz4,
            custom_root: None,
        }
    }

    /// Класс критических данных: синхронизация сразу, сжатие выключено.
    ///
    /// Сжатие на критическом канале вредно: оно заставляет копить данные ради
    /// эффективности, а копить — ровно то, чего критический канал избегает.
    /// Сегмент вдвое меньше обычного: критические разделы малы.
    pub fn critical(budget_bytes: u64) -> Self {
        Self {
            sync_interval: Duration::ZERO,
            compression: Compression::None,
            block_max_bytes: 8 * 1024,
            flush_interval: Duration::from_millis(50),
            segment_bytes: 4 * 1024 * 1024,
            ..Self::new(budget_bytes)
        }
    }

    /// Проверить конфигурацию; `class` — чей это канал в сообщении об ошибке.
    pub fn validate(&self, class: crate::schema::StorageClass) -> Result<()> {
        self.check().map_err(|reason| Error::BadChannel {
            class,
            namespace: None,
            reason,
        })
    }

    /// Та же проверка, но одной причиной без адреса.
    ///
    /// Адресов у одной и той же негодной настройки два: класс хранилища и
    /// класс группы неймспейсов ([`crate::store::GroupPolicy`]). Причина у них
    /// общая, и второй её копии быть не должно — разъехавшись, копии объявляли
    /// бы негодным разное.
    pub(crate) fn check(&self) -> std::result::Result<(), &'static str> {
        // Бюджет меньше двух сегментов означает, что при запечатывании
        // единственного сегмента ротация немедленно удалила бы его — канал
        // не хранил бы ничего.
        //
        // Умножение насыщающее: `segment_bytes` приходит из конфигурации
        // приложения, и обычное переполнило бы u64 паникой в debug там, где
        // ответ очевиден — такой бюджет заведомо мал.
        if self.budget_bytes < self.segment_bytes.saturating_mul(2) {
            return Err("бюджет меньше двух сегментов — ротация съедала бы данные сразу");
        }
        if self.block_max_bytes < 512 {
            return Err("слишком маленький блок: накладные расходы съедят экономию");
        }
        if (self.block_max_bytes as u64) * 2 > self.segment_bytes {
            return Err("блок сопоставим с сегментом — сегмент не вместит и пары блоков");
        }
        Ok(())
    }

    /// Сколько сегментов помещается в бюджет.
    pub fn max_segments(&self) -> u64 {
        (self.budget_bytes / self.segment_bytes).max(1)
    }
}

/// Чем группа неймспейсов может отличаться от общих настроек класса.
///
/// Здесь нет ни `budget_bytes`, ни `custom_root`, и это не упущение. Бюджет и
/// носитель — свойства **класса**, общие на всё хранилище: «вся телеметрия —
/// столько-то, критика — на защищённом разделе». Дать их группе значило бы
/// либо поднять потолок занятости выше объявленного (лишний бюджет сверх
/// класса), либо развести один класс по двум носителям, где его общий бюджет
/// перестал бы что-либо означать. Личный предел у группы всё же есть — это
/// квота ([`GroupPolicy::limit_bytes`]), предел ВНУТРИ бюджета класса.
///
/// Остальное — свойства каждого пишущего канала по отдельности, и группе они
/// принадлежат по праву: тяжёлой телеметрии оркестраторов уместен свой размер
/// сегмента, редкому диагностическому сервису — своя задержка выталкивания.
///
/// Незаданное наследуется от класса; строится цепочкой, как и всё остальное.
///
/// [`GroupPolicy::limit_bytes`]: crate::store::GroupPolicy::limit_bytes
#[derive(Debug, Clone, Default, PartialEq, Eq)]
#[must_use]
pub struct ChannelOverride {
    pub segment_bytes: Option<u64>,
    pub block_max_bytes: Option<usize>,
    pub flush_interval: Option<Duration>,
    pub sync_interval: Option<Duration>,
    pub compression: Option<Compression>,
}

impl ChannelOverride {
    pub fn new() -> Self {
        Self::default()
    }

    /// Свой размер сегмента (он же — шаг преаллокации).
    pub fn segment_bytes(mut self, bytes: u64) -> Self {
        self.segment_bytes = Some(bytes);
        self
    }

    /// Свой порог закрытия блока.
    pub fn block_max_bytes(mut self, bytes: usize) -> Self {
        self.block_max_bytes = Some(bytes);
        self
    }

    /// Своя максимальная задержка выталкивания неполного блока.
    pub fn flush_interval(mut self, every: Duration) -> Self {
        self.flush_interval = Some(every);
        self
    }

    /// Свой интервал синхронизации. У критического класса допустим только
    /// нулевой: немедленность — его определение, а не настройка.
    pub fn sync_interval(mut self, every: Duration) -> Self {
        self.sync_interval = Some(every);
        self
    }

    /// Своё сжатие.
    pub fn compression(mut self, compression: Compression) -> Self {
        self.compression = Some(compression);
        self
    }

    /// Наложить на настройки класса.
    pub(crate) fn apply_to(&self, config: &mut ChannelConfig) {
        if let Some(v) = self.segment_bytes {
            config.segment_bytes = v;
        }
        if let Some(v) = self.block_max_bytes {
            config.block_max_bytes = v;
        }
        if let Some(v) = self.flush_interval {
            config.flush_interval = v;
        }
        if let Some(v) = self.sync_interval {
            config.sync_interval = v;
        }
        if let Some(v) = self.compression {
            config.compression = v;
        }
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
    fn segment_size_does_not_scale_with_the_class_budget() {
        // Бюджет общий на класс, а преаллокацию держит каждый пишущий
        // неймспейс: сегмент, выросший вместе с бюджетом, съел бы бюджет
        // одними активными сегментами. 20 ГиБ телеметрии по формуле давали
        // бы 80-МиБ преаллокацию каждому писателю.
        let c = ChannelConfig::new(20 * 1024 * 1024 * 1024);
        assert_eq!(c.segment_bytes, 8 * 1024 * 1024);
        c.validate(crate::schema::StorageClass::Default).unwrap();

        let c = ChannelConfig::new(64 * 1024 * 1024);
        assert_eq!(c.segment_bytes, 8 * 1024 * 1024, "и от малого не зависит");
        assert_eq!(c.max_segments(), 8);
        c.validate(crate::schema::StorageClass::Default).unwrap();
    }

    #[test]
    fn critical_channel_avoids_buffering() {
        let c = ChannelConfig::critical(256 * 1024 * 1024);
        assert_eq!(
            c.sync_interval,
            Duration::ZERO,
            "немедленность — определение критического канала"
        );
        assert_eq!(c.compression, Compression::None);
        assert!(c.flush_interval < Duration::from_secs(1));
        c.validate(crate::schema::StorageClass::Default).unwrap();
    }

    #[test]
    fn degenerate_configs_rejected() {
        let mut c = ChannelConfig::new(64 * 1024 * 1024);
        c.budget_bytes = c.segment_bytes; // ровно один сегмент
        assert!(
            c.validate(crate::schema::StorageClass::Default).is_err(),
            "бюджет в один сегмент бессмыслен"
        );

        let mut c = ChannelConfig::new(64 * 1024 * 1024);
        c.block_max_bytes = 16;
        assert!(c.validate(crate::schema::StorageClass::Default).is_err());

        let mut c = ChannelConfig::new(64 * 1024 * 1024);
        c.block_max_bytes = c.segment_bytes as usize;
        assert!(c.validate(crate::schema::StorageClass::Default).is_err());
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
}
