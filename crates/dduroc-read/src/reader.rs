//! Читатель: слияние потоков и резолв схемы.

use crate::cursor::{ChannelCursor, Damage, OwnedRecord, OwnedSampleValue};
use crate::error::{ReadError, Result};
use crate::query::{KindFilter, Order, Query};
use dduroc_engine::epochs::{EpochStore, Epochs};
use dduroc_engine::namespace::{NS_META, NsMeta};
use dduroc_engine::schema::{EventDesc, MetricDesc, Schema, SpanDesc};
use dduroc_format::{EventId, Level, Micros, SeriesLocal, SpanId};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

/// Разновидность записи в ответе.
#[derive(Debug, Clone, PartialEq)]
pub enum EntryKind {
    /// Схемное сообщение.
    Message {
        event: EventId,
        /// Имя типа из схемы; `None` — схема неизвестна этому билду.
        name: Option<&'static str>,
        level: Option<Level>,
        tags: &'static [&'static str],
        payload: Vec<u8>,
    },
    /// Свободный текст без схемы (мост из tracing, panic-handler).
    Text {
        level: Level,
        target: String,
        text: String,
    },
    SpanStart {
        span: SpanId,
        kind_name: Option<&'static str>,
        parent: Option<SpanId>,
    },
    SpanEnd {
        span: SpanId,
    },
    /// Отсчёт телеметрии с восстановленной идентичностью серии.
    Sample {
        metric_name: Option<&'static str>,
        unit: Option<&'static str>,
        tags: Vec<(String, String)>,
        value: OwnedSampleValue,
    },
    /// Нераспознанное расширение формата.
    Ext {
        bytes: Vec<u8>,
    },
}

/// Запись ответа.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub namespace: String,
    pub channel: String,
    /// Относительное время от старта запуска.
    pub at: Micros,
    pub boot: u32,
    /// Абсолютное время, если для этой загрузки железа есть якорь.
    pub utc_ms: Option<i64>,
    /// Спан, к которому привязана запись.
    pub span: Option<SpanId>,
    pub kind: EntryKind,
}

impl Entry {
    /// Уровень записи: у сообщений — из схемы, у текста — из самой записи.
    pub fn level(&self) -> Option<Level> {
        match &self.kind {
            EntryKind::Message { level, .. } => *level,
            EntryKind::Text { level, .. } => Some(*level),
            _ => None,
        }
    }
}

/// Ответ на запрос.
#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub entries: Vec<Entry>,
    /// Фрагменты, которые не удалось прочитать. Пустой список — ответ полон.
    pub damaged: Vec<Damage>,
    /// Ответ обрезан по `limit`.
    pub truncated: bool,
}

impl QueryResult {
    /// Полон ли ответ: ничего не пропущено из-за повреждений.
    pub fn is_complete(&self) -> bool {
        self.damaged.is_empty()
    }
}

/// Сведения о неймспейсе хранилища.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceInfo {
    pub name: String,
    pub schema_name: String,
    pub protocol_version: u16,
    pub channels: Vec<String>,
    /// Суммарный размер сегментов, байт.
    pub bytes: u64,
}

/// Читатель хранилища.
#[derive(Debug)]
pub struct Reader {
    root: PathBuf,
    /// Схемы приложения по имени: без них записи остаются идентификаторами.
    schemas: HashMap<String, Schema>,
    epochs: Epochs,
    /// Идентичность хранилища; `None` — не проверять (чтение чужого дампа
    /// разрешено явно).
    store_id: Option<u64>,
}

impl Reader {
    /// Открыть хранилище на чтение.
    ///
    /// Только чтение: ни восстановления, ни уборки временных файлов. Вьюер
    /// не имеет права изменять дамп, который ему дали посмотреть.
    pub fn open(root: impl Into<PathBuf>, schemas: &[Schema]) -> Result<Self> {
        let root = root.into();
        let epochs = EpochStore::open_read_only(&root).unwrap_or_default();
        let store_id = read_store_id(&root);
        Ok(Self {
            root,
            schemas: schemas.iter().map(|s| (s.name.to_owned(), *s)).collect(),
            epochs,
            store_id,
        })
    }

    /// Не проверять принадлежность сегментов этому хранилищу.
    ///
    /// Нужно для разбора чужого дампа, собранного из нескольких приборов;
    /// абсолютное время таких записей доверять нельзя.
    pub fn allow_foreign_segments(mut self) -> Self {
        self.store_id = None;
        self
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn epochs(&self) -> &Epochs {
        &self.epochs
    }

    /// Перечислить неймспейсы.
    pub fn namespaces(&self) -> Result<Vec<NamespaceInfo>> {
        let mut out = Vec::new();
        let entries = match std::fs::read_dir(&self.root) {
            Ok(e) => e,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(source) => {
                return Err(ReadError::Io {
                    context: format!("чтение {}", self.root.display()),
                    source,
                });
            }
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            let Some(meta) = read_ns_meta(&path) else {
                continue;
            };

            let mut channels = Vec::new();
            let mut bytes = 0;
            if let Ok(dir) = std::fs::read_dir(&path) {
                for ch in dir.flatten() {
                    let ch_path = ch.path();
                    if !ch_path.is_dir() {
                        continue;
                    }
                    if let Some(ch_name) = ch_path.file_name().and_then(|n| n.to_str()) {
                        if let Ok(inv) = dduroc_engine::rotation::Inventory::scan(&ch_path) {
                            bytes += inv.total_bytes();
                        }
                        channels.push(ch_name.to_owned());
                    }
                }
            }
            channels.sort();

            out.push(NamespaceInfo {
                name: name.to_owned(),
                schema_name: meta.schema_name,
                protocol_version: meta.protocol_version,
                channels,
                bytes,
            });
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    /// Выполнить запрос.
    pub fn query(&self, q: &Query) -> Result<QueryResult> {
        let mut cursors = self.open_cursors(q)?;
        let mut result = QueryResult::default();
        // Определения серий: сегментно-локальные номера действительны в
        // пределах своего сегмента, поэтому таблица ведётся на курсор.
        let mut series: Vec<HashMap<SeriesLocal, SeriesInfo>> =
            (0..cursors.len()).map(|_| HashMap::new()).collect();
        let limit = q.limit.unwrap_or(usize::MAX);

        loop {
            // Выбираем курсор с самой ранней (или поздней) записью.
            let mut best: Option<(usize, Micros, u32)> = None;
            for (i, c) in cursors.iter_mut().enumerate() {
                let Some(head) = c.peek() else { continue };
                let key = (head.boot, head.at.0);
                let better = match best {
                    None => true,
                    Some((_, at, boot)) => match q.order {
                        Order::Oldest => key < (boot, at.0),
                        Order::Newest => key > (boot, at.0),
                    },
                };
                if better {
                    best = Some((i, head.at, head.boot));
                }
            }
            let Some((idx, _, _)) = best else { break };

            let Some(raw) = cursors[idx].next_entry() else {
                continue;
            };

            // Определения серий не выдаются наружу: это служебные записи,
            // задача которых — восстановить идентичность сэмплов.
            if let OwnedRecord::SeriesDef {
                series: local,
                metric,
                value_type: _,
                tags,
            } = &raw.record
            {
                let desc = self.metric_desc(&cursors[idx].namespace, *metric);
                series[idx].insert(
                    *local,
                    SeriesInfo {
                        name: desc.map(|d| d.name),
                        unit: desc.map(|d| d.unit),
                        tags: tags.clone(),
                    },
                );
                continue;
            }

            if !q.in_range(raw.at) {
                // Записи вне диапазона пропускаются, но обход продолжается:
                // сегмент мог начаться раньше `from`.
                if q.order == Order::Oldest && q.to.is_some_and(|to| raw.at > to) {
                    // Дальше по этому курсору будет только позже.
                    continue;
                }
                continue;
            }

            let ns_name = cursors[idx].namespace.clone();
            let ch_name = cursors[idx].channel.clone();
            let Some(entry) = self.build_entry(&ns_name, &ch_name, &raw, &series[idx], q) else {
                continue;
            };

            result.entries.push(entry);
            if result.entries.len() >= limit {
                result.truncated = true;
                break;
            }
        }

        for c in &cursors {
            result.damaged.extend_from_slice(c.damaged());
        }
        Ok(result)
    }

    fn open_cursors(&self, q: &Query) -> Result<Vec<ChannelCursor>> {
        let mut cursors = Vec::new();
        let reverse = q.order == Order::Newest;

        for ns in self.namespaces()? {
            if !q.namespaces.matches(&ns.name) {
                continue;
            }
            for channel in &ns.channels {
                if !q.channels.is_empty() && !q.channels.contains(channel) {
                    continue;
                }
                let dir = self.root.join(&ns.name).join(channel);
                cursors.push(ChannelCursor::open(
                    &dir,
                    ns.name.clone(),
                    channel.clone(),
                    q.from,
                    q.to,
                    q.boot,
                    reverse,
                    self.store_id,
                )?);
            }
        }
        Ok(cursors)
    }

    /// Схема неймспейса — по имени схемы из его метаданных.
    fn schema_of(&self, ns: &str) -> Option<&Schema> {
        let meta = read_ns_meta(&self.root.join(ns))?;
        self.schemas.get(&meta.schema_name)
    }

    fn event_desc(&self, ns: &str, id: EventId) -> Option<&'static EventDesc> {
        self.schema_of(ns)?.event(id)
    }

    fn metric_desc(&self, ns: &str, id: dduroc_format::MetricId) -> Option<&'static MetricDesc> {
        self.schema_of(ns)?.metric(id)
    }

    fn span_desc(&self, ns: &str, id: dduroc_format::SpanKindId) -> Option<&'static SpanDesc> {
        self.schema_of(ns)?.span(id)
    }

    fn build_entry(
        &self,
        ns: &str,
        channel: &str,
        raw: &crate::cursor::RawEntry,
        series: &HashMap<SeriesLocal, SeriesInfo>,
        q: &Query,
    ) -> Option<Entry> {
        let kinds = q.filter.kinds;
        let (kind, span) = match &raw.record {
            OwnedRecord::Message {
                event,
                span,
                payload,
            } => {
                if !kinds.messages {
                    return None;
                }
                let desc = self.event_desc(ns, *event);
                // Уровень и тэги — статические свойства типа, поэтому фильтр
                // применяется здесь, без чтения payload'а.
                if let Some(min) = q.filter.min_level {
                    match desc.map(|d| d.level) {
                        Some(l) if l >= min => {}
                        // Уровень неизвестен — запись не отбрасывается: это
                        // событие удалённого из схемы типа, и молча прятать
                        // его от того, кто ищет проблему, нельзя.
                        None => {}
                        _ => return None,
                    }
                }
                if !q.filter.any_tags.is_empty() {
                    let tags = desc.map(|d| d.tags).unwrap_or(&[]);
                    if !q
                        .filter
                        .any_tags
                        .iter()
                        .any(|want| tags.iter().any(|t| t == want))
                    {
                        return None;
                    }
                }
                if let Some(want) = &q.filter.events
                    && !want.contains(event)
                {
                    return None;
                }
                if !q.filter.event_names.is_empty() {
                    let name = desc.map(|d| d.name).unwrap_or("");
                    if !q.filter.event_names.iter().any(|n| n == name) {
                        return None;
                    }
                }
                (
                    EntryKind::Message {
                        event: *event,
                        name: desc.map(|d| d.name),
                        level: desc.map(|d| d.level),
                        tags: desc.map(|d| d.tags).unwrap_or(&[]),
                        payload: payload.clone(),
                    },
                    *span,
                )
            }
            OwnedRecord::Text {
                level,
                span,
                target,
                text,
            } => {
                if !kinds.text {
                    return None;
                }
                if let Some(min) = q.filter.min_level
                    && *level < min
                {
                    return None;
                }
                (
                    EntryKind::Text {
                        level: *level,
                        target: target.clone(),
                        text: text.clone(),
                    },
                    *span,
                )
            }
            OwnedRecord::SpanStart { span, kind, parent } => {
                if !kinds.spans {
                    return None;
                }
                (
                    EntryKind::SpanStart {
                        span: *span,
                        kind_name: self.span_desc(ns, *kind).map(|d| d.name),
                        parent: *parent,
                    },
                    Some(*span),
                )
            }
            OwnedRecord::SpanEnd { span } => {
                if !kinds.spans {
                    return None;
                }
                (EntryKind::SpanEnd { span: *span }, Some(*span))
            }
            OwnedRecord::Sample {
                series: local,
                value,
            } => {
                if !kinds.samples {
                    return None;
                }
                let info = series.get(local);
                (
                    EntryKind::Sample {
                        metric_name: info.and_then(|i| i.name),
                        unit: info.and_then(|i| i.unit),
                        tags: info.map(|i| i.tags.clone()).unwrap_or_default(),
                        value: value.clone(),
                    },
                    None,
                )
            }
            OwnedRecord::Ext { bytes } => (
                EntryKind::Ext {
                    bytes: bytes.clone(),
                },
                None,
            ),
            OwnedRecord::SeriesDef { .. } => return None,
        };

        if let Some(want) = &q.filter.spans {
            let id = span.map(|s| s.0).unwrap_or(0);
            if !want.contains(&id) {
                return None;
            }
        }

        Some(Entry {
            namespace: ns.to_owned(),
            channel: channel.to_owned(),
            at: raw.at,
            boot: raw.boot,
            utc_ms: self.epochs.to_utc_ms(raw.boot, raw.at.0),
            span,
            kind,
        })
    }
}

/// Восстановленная идентичность серии.
#[derive(Debug, Clone)]
struct SeriesInfo {
    name: Option<&'static str>,
    unit: Option<&'static str>,
    tags: Vec<(String, String)>,
}

fn read_ns_meta(dir: &Path) -> Option<NsMeta> {
    let bytes = std::fs::read(dir.join(NS_META)).ok()?;
    postcard::from_bytes(&bytes).ok()
}

fn read_store_id(root: &Path) -> Option<u64> {
    #[derive(serde::Deserialize)]
    struct Meta {
        #[allow(dead_code)]
        container_version: u8,
        store_id: u64,
    }
    let bytes = std::fs::read(root.join("store-meta")).ok()?;
    postcard::from_bytes::<Meta>(&bytes)
        .ok()
        .map(|m| m.store_id)
}

/// Отрендерить сообщение по шаблону схемы.
///
/// Шаблоны на диске не хранятся: `{поле}` подставляется декодером,
/// сгенерированным макросом схемы. Без декодера возвращается сам шаблон.
pub fn render(schema: &Schema, event: EventId, payload: &[u8], lang: &str) -> Option<String> {
    let desc = schema.event(event)?;
    let lang_index = schema.language_index(lang).unwrap_or(0);
    match desc.decoders {
        Some(d) => (d.render)(payload, lang_index).ok(),
        None => desc.template(lang_index).map(|t| t.to_owned()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dduroc_engine::schema::{Language, StorageClass};
    use dduroc_engine::store::{Store, StoreConfig};
    use dduroc_format::{MetricId, ProtocolVersion, SpanKindId, ValueType};

    static LANGS: &[Language] = &[Language("en"), Language("ru")];
    static EVENTS: &[EventDesc] = &[
        EventDesc {
            id: EventId(1),
            name: "PowerSet",
            level: Level::Info,
            class: StorageClass::DEFAULT,
            tags: &["rf"],
            templates: &["power set", "мощность задана"],
            fields: &[],
            decoders: None,
        },
        EventDesc {
            id: EventId(2),
            name: "Alarm",
            level: Level::Error,
            class: StorageClass::CRITICAL,
            tags: &["fault"],
            templates: &["alarm", "авария"],
            fields: &[],
            decoders: None,
        },
    ];
    static METRICS: &[MetricDesc] = &[MetricDesc {
        id: MetricId(1),
        name: "temp",
        value_type: ValueType::F32,
        class: StorageClass::DEFAULT,
        unit: "°C",
        tag_keys: &["sensor"],
    }];
    static SPANS: &[SpanDesc] = &[SpanDesc {
        id: SpanKindId(1),
        name: "Calibration",
        class: StorageClass::DEFAULT,
    }];

    fn schema() -> Schema {
        Schema {
            name: "radio",
            version: ProtocolVersion(1),
            languages: LANGS,
            events: EVENTS,
            metrics: METRICS,
            spans: SPANS,
            migrations: &[],
        }
    }

    /// Наполнить хранилище и закрыть его.
    fn populate(root: &Path) {
        let cfg = StoreConfig::new(root).with_budget(16 * 1024 * 1024);
        let store = Store::open(cfg.clone()).unwrap();
        for inst in 0..2 {
            let ns = store
                .namespace(&format!("orc-radio-{inst}"), schema(), &cfg)
                .unwrap();
            for i in 0..20u8 {
                ns.log_raw(EventId(1), &[i], None).unwrap();
            }
            ns.log_raw(EventId(2), &[0xFF], None).unwrap();

            let temp = ns.series(MetricId(1), &[("sensor", "pa")]).unwrap();
            for i in 0..10 {
                temp.sample_f32(20.0 + i as f32).unwrap();
            }
            {
                let cal = ns.span(SpanKindId(1)).unwrap();
                cal.log_raw(EventId(1), &[99]).unwrap();
            }
            ns.sync().unwrap();
        }
        let ns = store.namespace("apt-modem-0", schema(), &cfg).unwrap();
        ns.log_raw(EventId(1), &[1], None).unwrap();
        ns.sync().unwrap();
        store.shutdown();
    }

    #[test]
    fn reads_back_everything_written() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let namespaces = reader.namespaces().unwrap();
        assert_eq!(namespaces.len(), 3, "три неймспейса");
        assert_eq!(namespaces[0].name, "apt-modem-0");
        assert_eq!(namespaces[0].schema_name, "radio");
        assert!(namespaces[0].bytes > 0);

        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(result.is_complete(), "повреждений быть не должно");
        assert!(!result.entries.is_empty());

        // Сообщения обеих экземпляров и модема на месте.
        let messages = result
            .entries
            .iter()
            .filter(|e| matches!(e.kind, EntryKind::Message { .. }))
            .count();
        assert_eq!(
            messages,
            2 * 22 + 1,
            "20 + аварийное + внутри спана, ×2, +1"
        );
    }

    #[test]
    fn schema_resolves_names_levels_and_units() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        let result = reader
            .query(&Query::new().order(Order::Oldest).limit(500))
            .unwrap();

        let msg = result
            .entries
            .iter()
            .find(|e| {
                matches!(
                    &e.kind,
                    EntryKind::Message {
                        name: Some("PowerSet"),
                        ..
                    }
                )
            })
            .expect("имя события восстановлено по схеме");
        assert_eq!(msg.level(), Some(Level::Info), "уровень взят из схемы");

        let sample = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Sample { .. }))
            .expect("сэмпл найден");
        match &sample.kind {
            EntryKind::Sample {
                metric_name,
                unit,
                tags,
                value,
            } => {
                assert_eq!(*metric_name, Some("temp"));
                assert_eq!(*unit, Some("°C"));
                assert_eq!(tags, &[("sensor".to_owned(), "pa".to_owned())]);
                assert!(value.as_f64().unwrap() >= 20.0);
            }
            other => panic!("ожидался сэмпл: {other:?}"),
        }

        let span = result.entries.iter().find(|e| {
            matches!(
                &e.kind,
                EntryKind::SpanStart {
                    kind_name: Some("Calibration"),
                    ..
                }
            )
        });
        assert!(span.is_some(), "вид спана восстановлен");
    }

    #[test]
    fn filters_by_group_level_and_kind() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        // Только оркестраторы.
        let orc = reader
            .query(&Query::new().group("orc-").order(Order::Oldest))
            .unwrap();
        assert!(
            orc.entries.iter().all(|e| e.namespace.starts_with("orc-")),
            "группа отобрана по префиксу"
        );

        // Только ошибки: уровень — свойство типа, читать payload не нужно.
        let errors = reader
            .query(
                &Query::new()
                    .min_level(Level::Error)
                    .kinds(KindFilter::LOGS)
                    .order(Order::Oldest),
            )
            .unwrap();
        assert_eq!(errors.entries.len(), 2, "по одной аварии на экземпляр");
        assert!(
            errors
                .entries
                .iter()
                .all(|e| e.level() == Some(Level::Error))
        );

        // Только телеметрия.
        let telemetry = reader
            .query(
                &Query::new()
                    .kinds(KindFilter::TELEMETRY)
                    .order(Order::Oldest),
            )
            .unwrap();
        assert_eq!(telemetry.entries.len(), 20, "10 сэмплов × 2 экземпляра");
        assert!(
            telemetry
                .entries
                .iter()
                .all(|e| matches!(e.kind, EntryKind::Sample { .. }))
        );
    }

    #[test]
    fn newest_order_and_limit() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        let newest = reader
            .query(&Query::new().order(Order::Newest).limit(5))
            .unwrap();
        assert_eq!(newest.entries.len(), 5);
        assert!(newest.truncated, "ответ обрезан по лимиту");

        // Время не возрастает.
        let times: Vec<u64> = newest.entries.iter().map(|e| e.at.0).collect();
        assert!(
            times.windows(2).all(|w| w[0] >= w[1]),
            "порядок от нового к старому: {times:?}"
        );

        let oldest = reader
            .query(&Query::new().order(Order::Oldest).limit(5))
            .unwrap();
        let times: Vec<u64> = oldest.entries.iter().map(|e| e.at.0).collect();
        assert!(times.windows(2).all(|w| w[0] <= w[1]), "{times:?}");
    }

    #[test]
    fn merge_is_ordered_across_namespaces() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let keys: Vec<(u32, u64)> = all.entries.iter().map(|e| (e.boot, e.at.0)).collect();
        assert!(
            keys.windows(2).all(|w| w[0] <= w[1]),
            "слияние обязано давать глобальный порядок по времени"
        );
        // В ответе присутствуют оба неймспейса вперемешку.
        let namespaces: std::collections::HashSet<_> =
            all.entries.iter().map(|e| e.namespace.as_str()).collect();
        assert!(namespaces.len() >= 2);
    }

    #[test]
    fn unknown_schema_leaves_raw_identifiers() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        // Читаем без схем — как чужой билд.
        let reader = Reader::open(dir.path(), &[]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();

        assert!(!result.entries.is_empty(), "записи всё равно читаются");
        let msg = result
            .entries
            .iter()
            .find(|e| matches!(e.kind, EntryKind::Message { .. }))
            .unwrap();
        match &msg.kind {
            EntryKind::Message { name, level, .. } => {
                assert!(name.is_none(), "имя без схемы неизвестно");
                assert!(level.is_none(), "уровень без схемы неизвестен");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn time_range_narrows_result() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());
        let reader = Reader::open(dir.path(), &[schema()]).unwrap();

        let all = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let mid = all.entries[all.entries.len() / 2].at;

        let narrowed = reader
            .query(&Query {
                from: Some(mid),
                order: Order::Oldest,
                ..Query::new()
            })
            .unwrap();
        assert!(narrowed.entries.iter().all(|e| e.at >= mid));
        assert!(narrowed.entries.len() < all.entries.len());
    }

    #[test]
    fn foreign_store_segments_are_reported_not_silently_dropped() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        // Подменяем store-meta: сегменты станут «чужими».
        let meta_path = dir.path().join("store-meta");
        #[derive(serde::Serialize)]
        struct Meta {
            container_version: u8,
            store_id: u64,
        }
        std::fs::write(
            &meta_path,
            postcard::to_allocvec(&Meta {
                container_version: 1,
                store_id: 0xDEAD_BEEF,
            })
            .unwrap(),
        )
        .unwrap();

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(
            !result.is_complete(),
            "чужие сегменты обязаны попасть в список повреждений, а не исчезнуть"
        );
        assert!(result.entries.is_empty());

        // Явное разрешение читает их как есть.
        let reader = Reader::open(dir.path(), &[schema()])
            .unwrap()
            .allow_foreign_segments();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(!result.entries.is_empty());
    }

    #[test]
    fn corrupt_block_does_not_hide_the_rest() {
        let dir = tempfile::tempdir().unwrap();
        populate(dir.path());

        // Портим середину одного сегмента.
        let ch = dir.path().join("orc-radio-0").join("default");
        let seg = std::fs::read_dir(&ch)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .unwrap();
        {
            use std::os::unix::fs::FileExt;
            let f = std::fs::OpenOptions::new().write(true).open(&seg).unwrap();
            f.write_all_at(&[0xFF; 16], 40).unwrap();
        }

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        assert!(
            !result.is_complete(),
            "о повреждении обязано быть сказано явно"
        );
        // Данные других неймспейсов не пострадали.
        assert!(
            result
                .entries
                .iter()
                .any(|e| e.namespace == "orc-radio-1" || e.namespace == "apt-modem-0"),
            "порча одного сегмента не должна прятать остальные"
        );
    }

    #[test]
    fn utc_is_resolved_when_anchor_exists() {
        let dir = tempfile::tempdir().unwrap();
        {
            let cfg = StoreConfig::new(dir.path()).with_budget(8 * 1024 * 1024);
            let store = Store::open(cfg.clone()).unwrap();
            let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();
            ns.log_raw(EventId(1), &[1], None).unwrap();
            ns.sync().unwrap();
            store
                .record_sync(1_700_000_000_000, dduroc_engine::SyncSource::Gps)
                .unwrap();
            store.shutdown();
        }

        let reader = Reader::open(dir.path(), &[schema()]).unwrap();
        let result = reader.query(&Query::new().order(Order::Oldest)).unwrap();
        let entry = &result.entries[0];
        assert!(
            entry.utc_ms.is_some(),
            "якорь ретроактивен: событие записано ДО синхронизации"
        );
        let utc = entry.utc_ms.unwrap();
        assert!(
            (1_699_999_000_000..1_700_001_000_000).contains(&utc),
            "UTC рядом с точкой синхронизации: {utc}"
        );
    }
}
