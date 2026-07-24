//! Неймспейс: рабочая ручка микросервиса.
//!
//! Неймспейс — runtime-сущность: каталог со своими сегментами, привязанный к
//! compile-time схеме. Четыре экземпляра сервиса усилителя поднимают
//! `orc-radio-0`…`orc-radio-3` с одной схемой, и принадлежность записи
//! определяется её местоположением — в самих записях никакого «кто это
//! написал» не хранится.

use crate::error::{Error, Result};
use crate::fsutil;
use crate::schema::{Schema, StorageClass};
use crate::staged::{
    ChannelIdx, DropCounters, NsId, OwnedValue, Payload, SeriesEntry, SeriesId, Staged,
    StagedRecord,
};
use crate::store::next_span_id;
use crate::writer::{SeriesRegistry, Writer};
use crate::{Clock, schema};
use dduroc_format::{EventId, Level, MetricId, Micros, ProtocolVersion, SpanId, SpanKindId};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::AtomicU32;
use std::sync::{Arc, RwLock};

/// Имя файла метаданных неймспейса.
pub const NS_META: &str = "ns-meta";

/// Метаданные неймспейса.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NsMeta {
    /// Имя схемы. Открыть неймспейс чужой схемой нельзя: одни и те же
    /// идентификаторы событий в разных схемах означают разное.
    pub schema_name: String,
    /// Версия протокола, к которой приведены все сегменты каталога.
    pub protocol_version: u16,
}

impl NsMeta {
    /// Прочитать или создать метаданные, проверив совместимость схемы.
    pub fn open(dir: &Path, ns_name: &str, schema: &Schema) -> Result<Self> {
        let path = dir.join(NS_META);
        match fsutil::read_optional(&path)? {
            None => {
                let meta = Self {
                    schema_name: schema.name.to_owned(),
                    protocol_version: schema.version.0,
                };
                fsutil::write_atomic(&path, &postcard::to_allocvec(&meta)?)?;
                Ok(meta)
            }
            Some(bytes) => {
                let meta: Self = postcard::from_bytes(&bytes).map_err(|_| Error::Corrupt {
                    path: path.clone(),
                    reason: "метаданные неймспейса не разбираются".to_owned(),
                })?;
                if meta.schema_name != schema.name {
                    return Err(Error::SchemaMismatch {
                        namespace: ns_name.to_owned(),
                        stored: meta.schema_name,
                        opening: schema.name.to_owned(),
                    });
                }
                // Данные, записанные более новой прошивкой, эта понять не
                // может: у неё нет ни новых типов, ни шагов миграции вперёд.
                if meta.protocol_version > schema.version.0 {
                    return Err(Error::ProtocolFromFuture {
                        namespace: ns_name.to_owned(),
                        stored: meta.protocol_version,
                        current: schema.version.0,
                    });
                }
                // Цепочка шагов проверена при валидации схемы; отсутствующий
                // шаг здесь означал бы, что старые сегменты нечем привести
                // к текущему виду.
                for from in meta.protocol_version..schema.version.0 {
                    if schema.migration(from).is_none() {
                        return Err(Error::MissingMigration {
                            schema: schema.name.to_owned(),
                            from,
                            to: from + 1,
                        });
                    }
                }
                if meta.protocol_version != schema.version.0 {
                    let updated = Self {
                        schema_name: meta.schema_name,
                        protocol_version: schema.version.0,
                    };
                    fsutil::write_atomic(&path, &postcard::to_allocvec(&updated)?)?;
                    return Ok(updated);
                }
                Ok(meta)
            }
        }
    }
}

/// Ручка неймспейса.
///
/// Копируется дёшево (`Clone`) и рассылается по задачам сервиса: записи
/// адресуются явно, без неявного контекста потока.
#[derive(Debug, Clone)]
pub struct Namespace {
    inner: Arc<NamespaceInner>,
}

#[derive(Debug)]
struct NamespaceInner {
    /// Хранилище, которому принадлежит неймспейс.
    ///
    /// Держится живым, пока жива ручка: `Store` при уничтожении
    /// останавливает writer, и переживший его `Namespace` писал бы в
    /// никуда, возвращая `Ok` на каждый вызов.
    _store: Arc<dyn std::any::Any + Send + Sync>,
    id: NsId,
    name: String,
    schema: Schema,
    /// Классы хранения в том же порядке, в каком зарегистрированы каналы.
    classes: Vec<StorageClass>,
    writer: Arc<Writer>,
    clock: Clock,
    series: Arc<RwLock<Vec<SeriesEntry>>>,
    drops: Arc<DropCounters>,
    next_span: Arc<AtomicU32>,
    meta: NsMeta,
}

impl Namespace {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: Arc<dyn std::any::Any + Send + Sync>,
        id: NsId,
        name: String,
        schema: Schema,
        classes: Vec<StorageClass>,
        writer: Arc<Writer>,
        clock: Clock,
        series: Arc<RwLock<Vec<SeriesEntry>>>,
        drops: Arc<DropCounters>,
        next_span: Arc<AtomicU32>,
        meta: NsMeta,
    ) -> Self {
        Self {
            inner: Arc::new(NamespaceInner {
                _store: store,
                id,
                name,
                schema,
                classes,
                writer,
                clock,
                series,
                drops,
                next_span,
                meta,
            }),
        }
    }

    pub fn name(&self) -> &str {
        &self.inner.name
    }

    pub fn id(&self) -> NsId {
        self.inner.id
    }

    pub fn schema(&self) -> &Schema {
        &self.inner.schema
    }

    pub fn meta(&self) -> &NsMeta {
        &self.inner.meta
    }

    pub fn protocol_version(&self) -> ProtocolVersion {
        self.inner.schema.version
    }

    /// Текущее время неймспейса.
    pub fn now(&self) -> Micros {
        self.inner.clock.now()
    }

    /// Канал, соответствующий классу хранения.
    ///
    /// Список каналов построен по самой схеме, поэтому класс обязан
    /// найтись. Тихий возврат нулевого канала при промахе означал бы, что
    /// критическая запись без предупреждения уходит в обычный канал — с
    /// другой политикой долговечности и другим бюджетом.
    fn channel_of(&self, class: StorageClass) -> Result<ChannelIdx> {
        self.inner
            .classes
            .iter()
            .position(|c| *c == class)
            .map(|i| ChannelIdx(i as u16))
            .ok_or(Error::BadNamespace {
                name: self.inner.name.clone(),
                reason: "класс хранения не объявлен ни одним типом схемы",
            })
    }

    /// Записать событие с уже сериализованным payload'ом.
    ///
    /// Ошибка означает именно потерю записи (очередь переполнена, writer
    /// мёртв): у обычного канала она случается при отставании диска, у
    /// критического — только по таймауту ожидания.
    pub fn log_raw(&self, event: EventId, payload: &[u8], span: Option<SpanId>) -> Result<()> {
        self.log_payload(event, Payload::from_slice(payload), span)
    }

    /// То же, но payload уже собран в буфере записи.
    ///
    /// Основной путь для типизированного API: сериализация идёт прямо в этот
    /// буфер, поэтому на событие не приходится ни одной лишней аллокации и
    /// копии — при типичном размере полей буфер остаётся inline.
    pub fn log_payload(
        &self,
        event: EventId,
        payload: Payload,
        span: Option<SpanId>,
    ) -> Result<()> {
        let Some(desc) = self.inner.schema.event(event) else {
            return Err(Error::BadNamespace {
                name: self.inner.name.clone(),
                reason: "событие не объявлено в схеме",
            });
        };
        let item = Staged {
            ns: self.inner.id,
            channel: self.channel_of(desc.class)?,
            at: self.now(),
            record: StagedRecord::Message {
                event,
                span,
                payload,
            },
        };
        self.inner.writer.write(
            item,
            desc.class == StorageClass::CRITICAL,
            &self.inner.drops,
        )
    }

    /// Записать свободный текст без схемы: мост из `tracing`, panic-handler.
    pub fn log_text(
        &self,
        level: Level,
        target: Arc<str>,
        text: impl Into<Box<str>>,
        span: Option<SpanId>,
    ) -> Result<()> {
        let critical = level >= Level::Error;
        let class = if critical {
            StorageClass::CRITICAL
        } else {
            StorageClass::DEFAULT
        };
        let item = Staged {
            ns: self.inner.id,
            channel: self.channel_of(class)?,
            at: self.now(),
            record: StagedRecord::Text {
                level,
                span,
                target,
                text: text.into(),
            },
        };
        self.inner.writer.write(item, critical, &self.inner.drops)
    }

    /// Открыть серию телеметрии: `(метрика, тэги)` интернируются один раз,
    /// дальше сэмпл стоит один вызов без поиска.
    pub fn series(&self, metric: MetricId, tags: &[(&str, &str)]) -> Result<Series> {
        let Some(desc) = self.inner.schema.metric(metric) else {
            return Err(Error::BadNamespace {
                name: self.inner.name.clone(),
                reason: "метрика не объявлена в схеме",
            });
        };
        let id = SeriesRegistry::intern(&self.inner.series, metric, desc.value_type, tags);
        Ok(Series {
            ns: self.clone(),
            id,
            channel: self.channel_of(desc.class)?,
            critical: desc.class == StorageClass::CRITICAL,
            value_type: desc.value_type,
        })
    }

    /// Начать спан. Возвращает страж: конец записывается при его уничтожении,
    /// в том числе при развёртке стека — незакрытый спан неотличим от краха.
    pub fn span(&self, kind: SpanKindId) -> Result<SpanGuard> {
        self.span_with_parent(kind, None)
    }

    /// Начать спан с явным родителем.
    pub fn span_with_parent(&self, kind: SpanKindId, parent: Option<SpanId>) -> Result<SpanGuard> {
        let Some(desc) = self.inner.schema.span(kind) else {
            return Err(Error::BadNamespace {
                name: self.inner.name.clone(),
                reason: "вид спана не объявлен в схеме",
            });
        };
        let span = next_span_id(&self.inner.next_span);
        let channel = self.channel_of(desc.class)?;
        let critical = desc.class == StorageClass::CRITICAL;

        self.inner.writer.write(
            Staged {
                ns: self.inner.id,
                channel,
                at: self.now(),
                record: StagedRecord::SpanStart { span, kind, parent },
            },
            critical,
            &self.inner.drops,
        )?;

        Ok(SpanGuard {
            ns: self.clone(),
            span,
            channel,
            critical,
            closed: false,
        })
    }

    /// Дождаться, пока накопленное окажется на носителе.
    pub fn sync(&self) -> Result<()> {
        self.inner.writer.sync(Some(self.inner.id))
    }
}

/// Открытая серия телеметрии.
#[derive(Debug, Clone)]
pub struct Series {
    ns: Namespace,
    id: SeriesId,
    channel: ChannelIdx,
    critical: bool,
    value_type: dduroc_format::ValueType,
}

impl Series {
    pub fn id(&self) -> SeriesId {
        self.id
    }

    pub fn value_type(&self) -> dduroc_format::ValueType {
        self.value_type
    }

    /// Записать отсчёт.
    ///
    /// Тип значения обязан совпасть с объявленным у метрики: расхождение —
    /// ошибка схемы, а не данных, и молча писать «как получилось» нельзя.
    pub fn sample(&self, value: OwnedValue) -> Result<()> {
        if value.value_type() != self.value_type {
            return Err(Error::BadNamespace {
                name: self.ns.inner.name.clone(),
                reason: "тип значения не совпадает с объявленным у метрики",
            });
        }
        let item = Staged {
            ns: self.ns.inner.id,
            channel: self.channel,
            at: self.ns.now(),
            record: StagedRecord::Sample {
                series: self.id,
                value,
            },
        };
        self.ns
            .inner
            .writer
            .write(item, self.critical, &self.ns.inner.drops)
    }

    /// Скалярный отсчёт с плавающей точкой — самый частый случай.
    pub fn sample_f32(&self, v: f32) -> Result<()> {
        self.sample(OwnedValue::F32(v))
    }
}

/// Страж спана: конец записывается при уничтожении.
#[derive(Debug)]
pub struct SpanGuard {
    ns: Namespace,
    span: SpanId,
    channel: ChannelIdx,
    critical: bool,
    closed: bool,
}

impl SpanGuard {
    pub fn id(&self) -> SpanId {
        self.span
    }

    /// Вложенный спан: родителем становится этот.
    pub fn child(&self, kind: SpanKindId) -> Result<SpanGuard> {
        self.ns.span_with_parent(kind, Some(self.span))
    }

    /// Записать событие, привязанное к этому спану.
    pub fn log_raw(&self, event: EventId, payload: &[u8]) -> Result<()> {
        self.ns.log_raw(event, payload, Some(self.span))
    }

    /// То же, но payload уже собран в буфере записи.
    pub fn log_payload(&self, event: EventId, payload: Payload) -> Result<()> {
        self.ns.log_payload(event, payload, Some(self.span))
    }

    /// Закрыть явно, увидев ошибку записи. Обычно не нужно: закрытие
    /// происходит при уничтожении.
    pub fn close(mut self) -> Result<()> {
        self.closed = true;
        self.write_end()
    }

    fn write_end(&self) -> Result<()> {
        self.ns.inner.writer.write(
            Staged {
                ns: self.ns.inner.id,
                channel: self.channel,
                at: self.ns.now(),
                record: StagedRecord::SpanEnd { span: self.span },
            },
            self.critical,
            &self.ns.inner.drops,
        )
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        if !self.closed {
            // Ошибку здесь показать некому; она уже учтена счётчиком потерь.
            let _ = self.write_end();
        }
    }
}

/// Уровни и классы — реэкспорт для потребителей ручки.
pub use schema::StorageClass as Class;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{EventDesc, Language, MetricDesc, SpanDesc};
    use crate::store::{Store, StoreConfig};
    use dduroc_format::ValueType;

    static LANGS: &[Language] = &[Language("en"), Language("ru")];
    static EVENTS: &[EventDesc] = &[
        EventDesc {
            id: EventId(1),
            name: "PowerSet",
            level: Level::Info,
            class: StorageClass::DEFAULT,
            tags: &["rf"],
            templates: &["power {dbm}", "мощность {dbm}"],
            fields: &[],
            decoders: None,
        },
        EventDesc {
            id: EventId(2),
            name: "Alarm",
            level: Level::Error,
            class: StorageClass::CRITICAL,
            tags: &[],
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

    fn open_store(dir: &Path) -> (Arc<Store>, StoreConfig) {
        let cfg = StoreConfig::new(dir).with_budget(8 * 1024 * 1024);
        (Store::open(cfg.clone()).unwrap(), cfg)
    }

    #[test]
    fn writes_land_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();

        for i in 0..100 {
            ns.log_raw(EventId(1), &[i as u8; 4], None).unwrap();
        }
        ns.sync().unwrap();

        let stats = store.stats();
        assert_eq!(stats.records_written, 100);
        assert!(stats.blocks_written >= 1);
        assert!(stats.is_clean(), "потерь быть не должно: {stats:?}");

        // Сегмент появился в канале «default».
        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let files: Vec<_> = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "seg"))
            .collect();
        assert_eq!(files.len(), 1, "создан ровно один сегмент");
    }

    #[test]
    fn critical_events_are_synced_immediately() {
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();

        ns.log_raw(EventId(2), &[1, 2, 3], None).unwrap();
        // Ждём, пока writer обработает: sync — барьер по той же очереди.
        ns.sync().unwrap();
        assert!(
            store.stats().syncs >= 1,
            "критическое событие обязано быть синхронизировано"
        );

        let crit_dir = dir.path().join("orc-radio-0").join("critical");
        assert!(crit_dir.is_dir(), "критический канал создан отдельно");
    }

    #[test]
    fn overload_loses_only_what_it_reports() {
        // Под давлением обычный канал вправе терять записи — но ровно
        // столько, сколько признал потерянными. Расхождение между «принято»
        // и «записано» означало бы тихую дыру.
        //
        // Пропускная способность как таковая здесь не проверяется: она
        // зависит от загрузки машины и меряется бенчмарками.
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();

        const N: u64 = 50_000;
        let mut accepted = 0u64;
        let mut refused = 0u64;
        for i in 0..N {
            match ns.log_raw(EventId(1), &[i as u8; 8], None) {
                Ok(()) => accepted += 1,
                Err(_) => refused += 1,
            }
        }
        ns.sync().unwrap();

        let stats = store.stats();
        assert_eq!(accepted + refused, N);
        assert!(accepted > 0, "хоть что-то обязано пройти");
        assert!(
            stats.records_written >= accepted,
            "записано {} при принятых {accepted} — тихая потеря",
            stats.records_written
        );
        assert_eq!(stats.dropped, refused, "потери учтены ровно те, что были");
        assert_eq!(stats.io_errors, 0, "ошибок ввода-вывода быть не должно");
    }

    #[test]
    fn critical_burst_is_one_group_commit() {
        // Смысл политики Immediate — не «fdatasync на запись», а «синхронизация
        // при первой возможности». Всплеск аварийных сообщений должен стоить
        // единиц обращений к носителю: на eMMC каждый fdatasync это 1–10 мс,
        // и пятьсот таких обращений превратили бы аварию в секунды записи и
        // лишний износ флеша.
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();

        const BURST: usize = 500;
        for i in 0..BURST {
            while ns.log_raw(EventId(2), &[i as u8], None).is_err() {
                std::thread::yield_now();
            }
        }
        ns.sync().unwrap();

        let stats = store.stats();
        assert!(stats.records_written >= BURST as u64);
        assert!(
            stats.syncs < BURST as u64 / 4,
            "всплеск из {BURST} записей стоил {} обращений к носителю — \
             это не групповая фиксация",
            stats.syncs
        );
        assert!(
            stats.blocks_written < BURST as u64 / 4,
            "{BURST} записей уложены в {} блоков — заголовок на запись",
            stats.blocks_written
        );
    }

    #[test]
    fn namespace_cannot_be_opened_twice() {
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let a = store.namespace("orc-radio-0", schema(), &cfg).unwrap();
        let err = store.namespace("orc-radio-0", schema(), &cfg).unwrap_err();
        assert!(matches!(err, Error::NamespaceBusy(_)), "получено {err}");

        // Отпущенное имя обязано освободиться: иначе сервис не смог бы
        // переоткрыть свой неймспейс после перенастройки.
        drop(a);
        store
            .namespace("orc-radio-0", schema(), &cfg)
            .expect("имя свободно после уничтожения ручки");
    }

    #[test]
    fn foreign_schema_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        drop(store.namespace("orc-radio-0", schema(), &cfg).unwrap());
        store.shutdown();
        drop(store);

        let (store2, cfg) = open_store(dir.path());
        let other = Schema {
            name: "другая-схема",
            ..schema()
        };
        let err = store2.namespace("orc-radio-0", other, &cfg).unwrap_err();
        assert!(
            matches!(err, Error::SchemaMismatch { .. }),
            "получено {err}"
        );
    }

    #[test]
    fn protocol_from_future_refused() {
        let dir = tempfile::tempdir().unwrap();
        let ns_dir = dir.path().join("orc-radio-0");
        std::fs::create_dir_all(&ns_dir).unwrap();
        let meta = NsMeta {
            schema_name: "radio".to_owned(),
            protocol_version: 99,
        };
        std::fs::write(ns_dir.join(NS_META), postcard::to_allocvec(&meta).unwrap()).unwrap();

        let (store, cfg) = open_store(dir.path());
        let err = store.namespace("orc-radio-0", schema(), &cfg).unwrap_err();
        assert!(
            matches!(err, Error::ProtocolFromFuture { stored: 99, .. }),
            "получено {err}"
        );
    }

    #[test]
    fn bad_namespace_names_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        for bad in ["../escape", "a/b", "", ".hidden"] {
            assert!(
                store.namespace(bad, schema(), &cfg).is_err(),
                "{bad:?} обязано отвергаться"
            );
        }
    }

    #[test]
    fn telemetry_series_and_spans() {
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();

        let temp = ns.series(MetricId(1), &[("sensor", "pa")]).unwrap();
        for i in 0..50 {
            temp.sample_f32(20.0 + i as f32).unwrap();
        }
        // Тип значения обязан совпасть с объявленным.
        assert!(temp.sample(OwnedValue::U64(1)).is_err());

        {
            let cal = ns.span(SpanKindId(1)).unwrap();
            cal.log_raw(EventId(1), &[7]).unwrap();
            let _child = cal.child(SpanKindId(1)).unwrap();
        } // оба спана закрываются здесь

        ns.sync().unwrap();
        let stats = store.stats();
        // 50 сэмплов + 1 SeriesDef + 2 SpanStart + 1 событие + 2 SpanEnd
        assert!(stats.records_written >= 55, "получено {stats:?}");
        assert!(stats.is_clean());
    }

    #[test]
    fn sync_waits_for_everything_already_enqueued() {
        // Управляющие команды идут отдельной очередью и без явного
        // вычерпывания обгоняли бы записи в полёте: sync отчитывался бы
        // об успехе, не записав их, а shutdown запечатывал бы сегменты
        // поверх недописанного. Проверяем на потоке, заведомо обгоняющем
        // writer.
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();

        const N: usize = 20_000;
        let mut accepted = 0u64;
        for i in 0..N {
            // Ретраи, чтобы отделить потери очереди (штатное поведение
            // обычного канала) от потерь при синхронизации.
            while ns.log_raw(EventId(1), &[i as u8; 4], None).is_err() {
                std::thread::yield_now();
            }
            accepted += 1;
        }
        ns.sync().unwrap();

        let stats = store.stats();
        // Записей может оказаться БОЛЬШЕ принятых: неудачные попытки
        // try_send оставляют в потоке отметку о потере. Меньше — нельзя.
        assert!(
            stats.records_written >= accepted,
            "sync обязан дождаться всех принятых записей, а не только успевших: \
             записано {}, принято {accepted}",
            stats.records_written
        );
    }

    #[test]
    fn shutdown_persists_everything_accepted() {
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();

        const N: usize = 20_000;
        let mut accepted = 0u64;
        for i in 0..N {
            while ns.log_raw(EventId(1), &[i as u8; 4], None).is_err() {
                std::thread::yield_now();
            }
            accepted += 1;
        }
        store.shutdown();

        let written = store.stats().records_written;
        assert!(
            written >= accepted,
            "shutdown обязан дописать очередь, а не запечатать поверх неё: \
             записано {written}, принято {accepted}"
        );
    }

    #[test]
    fn segment_name_matches_time_of_its_first_record() {
        // Имя и база сегмента обязаны совпадать со временем ПЕРВОЙ его
        // записи: на этом стоит отбор сегментов по диапазону при чтении.
        // Брать время предыдущей записи (у нового канала — ноль) значило бы
        // молча отдавать читателю сегменты, которых он не просил, и
        // пропускать те, что нужны.
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();

        // Небольшая занятая пауза, чтобы время старта заведомо было > 0.
        let mut spin = 0u64;
        while ns.now().0 < 1_000 && spin < 200_000_000 {
            spin += 1;
        }
        let before = ns.now();
        ns.log_raw(EventId(1), &[1], None).unwrap();
        ns.sync().unwrap();
        let after = ns.now();

        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let name = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .find(|n| n.ends_with(".seg"))
            .expect("сегмент создан");
        let parsed = dduroc_format::segment::SegmentName::parse(&name).expect("имя разбирается");

        assert!(
            parsed.base >= before && parsed.base <= after,
            "имя сегмента {name} должно нести время первой записи ({before}..{after})"
        );
        assert_ne!(parsed.base.0, 0, "нулевая база — признак старой ошибки");
    }

    #[test]
    fn namespace_keeps_store_alive() {
        // Store при уничтожении останавливает writer. Переживший его
        // Namespace писал бы в никуда, возвращая Ok на каждый вызов —
        // худший вид потери данных: без единого признака.
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();
        drop(store);

        for i in 0..100 {
            ns.log_raw(EventId(1), &[i as u8], None)
                .expect("запись обязана продолжать работать");
        }
        ns.sync().expect("синхронизация обязана работать");

        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let total: u64 = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|x| x == "seg"))
            .count() as u64;
        assert!(total >= 1, "данные записаны на диск");
    }

    #[test]
    fn failed_namespace_open_releases_the_name() {
        // Пометка «занято» ставится до подъёма; ранний выход обязан её снять,
        // иначе имя останется недоступным до конца жизни процесса.
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());

        let broken = Schema {
            version: ProtocolVersion(0), // недопустима
            ..schema()
        };
        assert!(store.namespace("orc-radio-0", broken, &cfg).is_err());
        // Имя снова свободно.
        store
            .namespace("orc-radio-0", schema(), &cfg)
            .expect("имя обязано освободиться после неудачи");
    }

    #[test]
    fn unknown_ids_are_refused() {
        let dir = tempfile::tempdir().unwrap();
        let (store, cfg) = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema(), &cfg).unwrap();

        assert!(ns.log_raw(EventId(99), &[], None).is_err());
        assert!(ns.series(MetricId(99), &[]).is_err());
        assert!(ns.span(SpanKindId(99)).is_err());
    }
}
