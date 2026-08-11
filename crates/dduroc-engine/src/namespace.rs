//! Неймспейс: рабочая ручка микросервиса.
//!
//! Неймспейс — runtime-сущность: каталог со своими сегментами, привязанный к
//! compile-time схеме. Четыре экземпляра сервиса усилителя поднимают
//! `orc-radio-0`…`orc-radio-3` с одной схемой, и принадлежность записи
//! определяется её местоположением — в самих записях никакого «кто это
//! написал» не хранится.

use crate::Clock;
use crate::error::{Error, Result};
use crate::fsutil;
use crate::limits::{EffectiveLimits, LimitsRegistry, MetricLimits};
use crate::metric::{Metric, MetricValue, Untyped};
use crate::schema::{MetricDesc, MetricKind, Schema, Severity, StorageClass, Thresholds};
use crate::staged::{ChannelIdx, DropCounters, NsId, OwnedValue, Payload, Staged, StagedRecord};
use crate::stats::Counters;
use crate::store::next_span_id;
use crate::writer::Writer;
use dduroc_format::{
    BootTime, EventId, Level, MetricId, Micros, ProtocolVersion, SpanId, SpanKindId,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;

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
                // `protocol_version` в мете — версия последней **завершённой**
                // миграции, и переписывать её здесь нельзя: подъём — не
                // прогон, сегменты остались в прежней раскладке. Проштамповав
                // мету, этот билд объявил бы неймспейс мигрированным, и
                // будущий `Namespace::migrate` обошёл бы старые сегменты
                // стороной — их разобрали бы декодерами новой версии, молча и
                // неверно. Смешанное состояние легально: версию несёт
                // заголовок каждого сегмента, и она у них своя. Штампует мету
                // только успешный прогон.
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
    /// Каталог неймспейса — здесь живёт `ns-meta`.
    dir: std::path::PathBuf,
    /// Каталоги каналов в порядке `classes`: классы могут жить на разных
    /// носителях, и прогону миграции пути нужны готовыми.
    channel_dirs: Vec<std::path::PathBuf>,
    /// Идентичность хранилища: прогон не трогает сегменты чужих приборов.
    store_id: u64,
    schema: Schema,
    /// Классы хранения в том же порядке, в каком зарегистрированы каналы.
    classes: Vec<StorageClass>,
    writer: Arc<Writer>,
    clock: Clock,
    drops: Arc<DropCounters>,
    next_span: Arc<AtomicU32>,
    meta: NsMeta,
    /// Версия последней завершённой миграции — живое значение.
    ///
    /// В `meta` лежит то, что было прочитано при открытии; успешный
    /// `Namespace::migrate` двигает версию, не переоткрывая неймспейс.
    migrated_to: std::sync::atomic::AtomicU16,
    /// Прогон единоличен: два одновременных переписывали бы одни файлы.
    migrate_lock: std::sync::Mutex<()>,
    /// Пределы значений: дефолты схемы плюс то, что выставила установка.
    /// Живут в памяти и **не пишутся на диск** — см. [`crate::limits`].
    limits: LimitsRegistry,
    /// Какие нарушения контракта уже объявлены в потоке — по биту на вид.
    ///
    /// Объявлять каждое значило бы залить журнал: нарушение контракта не
    /// случайность, оно повторяется на каждом витке цикла. Одного раза
    /// достаточно, чтобы дефект нашли; дальше растёт только счётчик.
    announced: std::sync::atomic::AtomicU8,
}

impl Namespace {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        store: Arc<dyn std::any::Any + Send + Sync>,
        id: NsId,
        name: String,
        dir: std::path::PathBuf,
        channel_dirs: Vec<std::path::PathBuf>,
        store_id: u64,
        schema: Schema,
        classes: Vec<StorageClass>,
        writer: Arc<Writer>,
        clock: Clock,
        drops: Arc<DropCounters>,
        next_span: Arc<AtomicU32>,
        meta: NsMeta,
    ) -> Self {
        let limits = LimitsRegistry::new();
        let migrated_to = std::sync::atomic::AtomicU16::new(meta.protocol_version);
        Self {
            inner: Arc::new(NamespaceInner {
                _store: store,
                id,
                name,
                dir,
                channel_dirs,
                store_id,
                schema,
                classes,
                writer,
                clock,
                drops,
                next_span,
                meta,
                migrated_to,
                migrate_lock: std::sync::Mutex::new(()),
                limits,
                announced: std::sync::atomic::AtomicU8::new(0),
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

    /// Метаданные неймспейса — с живой версией протокола.
    ///
    /// По значению, а не ссылкой: успешный [`Namespace::migrate`] двигает
    /// версию, и снимок обязан это отражать.
    pub fn meta(&self) -> NsMeta {
        NsMeta {
            schema_name: self.inner.meta.schema_name.clone(),
            protocol_version: self
                .inner
                .migrated_to
                .load(std::sync::atomic::Ordering::Acquire),
        }
    }

    /// Версия схемы ЭТОГО билда — та, которой пишутся новые сегменты.
    ///
    /// Не путать с версией последней завершённой миграции
    /// ([`NsMeta::protocol_version`]): их расхождение и есть непогашенный долг,
    /// который называет [`Namespace::pending_migration`].
    pub fn schema_version(&self) -> ProtocolVersion {
        self.inner.schema.version
    }

    /// Незавершённая миграция: `(с какой версии, до какой)`.
    ///
    /// `Some` означает, что в каталоге лежат сегменты прежней раскладки:
    /// схема этого билда новее, чем версия последней завершённой миграции.
    /// Читаются они правильно — сегмент прежней версии проходит через шаги
    /// прямо при чтении, — но каждое чтение платит за преобразование, а
    /// история лежит в раскладке, которой больше ни один билд не пишет.
    /// Привести её физически — [`Namespace::migrate`]; когда его звать,
    /// решает приложение: прогон жжёт ресурс флеша и занимает носитель.
    ///
    /// Новые сегменты пишутся текущей версией схемы; смешанное состояние
    /// каталога легально и ожидаемо.
    pub fn pending_migration(&self) -> Option<(u16, u16)> {
        let stored = self
            .inner
            .migrated_to
            .load(std::sync::atomic::Ordering::Acquire);
        let current = self.inner.schema.version.0;
        (stored < current).then_some((stored, current))
    }

    /// Привести сегменты неймспейса к текущей версии схемы.
    ///
    /// Явный вызов, а не автоматика при открытии: прогон читает и переписывает
    /// историю целиком — минуты на гигабайтах — и жжёт ресурс флеша. Когда
    /// прибору это позволительно, знает только приложение (после старта, в
    /// тихий час); долг тем временем виден в [`Namespace::pending_migration`],
    /// а читается всё правильно и без прогона — шаги применяются при чтении.
    ///
    /// Тяжёлая работа идёт на потоке вызывающего; writer участвует только в
    /// фиксации каждого сегмента (rename поверх старого имени), поэтому запись
    /// не останавливается. Прогон идемпотентен и возобновляем: обрыв на любом
    /// месте оставляет каждый сегмент либо прежним, либо уже переписанным, и
    /// следующий вызов продолжит с оставшихся.
    ///
    /// Сегменты, которые ни один шаг не затрагивает, не переписываются и
    /// сохраняют прежнюю версию в заголовке — это легально: незатронутость и
    /// означает, что текущие декодеры читают их верно.
    pub fn migrate(&self) -> Result<crate::migrate::MigrationReport> {
        let Some((_, to)) = self.pending_migration() else {
            return Ok(crate::migrate::MigrationReport::default());
        };
        let Ok(_guard) = self.inner.migrate_lock.try_lock() else {
            return Err(Error::MigrationBusy(self.inner.name.clone()));
        };

        let report = crate::migrate::run_namespace(
            &self.inner.schema,
            self.inner.store_id,
            &self.inner.writer,
            self.inner.id,
            &self.inner.channel_dirs,
        )?;

        // Штамп — только после того, как КАЖДЫЙ сегмент оказался либо
        // текущим, либо переписанным, либо доказуемо незатронутым: мета
        // означает «завершено», и объявить это раньше значило бы, что
        // будущие прогоны обойдут недоделанное стороной.
        let meta = NsMeta {
            schema_name: self.inner.meta.schema_name.clone(),
            protocol_version: to,
        };
        fsutil::write_atomic(
            &self.inner.dir.join(NS_META),
            &postcard::to_allocvec(&meta)?,
        )?;
        self.inner
            .migrated_to
            .store(to, std::sync::atomic::Ordering::Release);
        Ok(report)
    }

    /// Текущий момент — в тех же координатах, в каких его вернёт читатель.
    ///
    /// Именно момент, а не только микросекунды: сравнивать `Micros` разных
    /// запусков между собой бессмысленно, и тип это запрещает.
    pub fn now(&self) -> BootTime {
        self.inner.clock.now_at()
    }

    /// Метка для записи: только микросекунды.
    ///
    /// Запуск в запись не идёт — он имплицитен из заголовка сегмента, и
    /// повторять его в каждой записи значило бы платить четыре байта за то,
    /// что и так известно из имени файла.
    #[inline]
    fn stamp(&self) -> Micros {
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
            .ok_or(Error::ClassNotDeclared {
                schema: self.inner.schema.name,
                class: class.as_str(),
            })
    }

    /// Учесть исход записи, о котором вызывающему не сообщают.
    ///
    /// Путь записи ничего не возвращает — прикладной код всё равно не может
    /// сделать с отказом ничего осмысленного, — но исход не исчезает:
    ///
    /// - **потеря** (очередь не успевает, writer мёртв) уже учтена счётчиком
    ///   writer'а и объявляется отметкой в самом потоке;
    /// - **нарушение контракта** (id из чужой схемы) — дефект сборки: он
    ///   считается отдельно и один раз объявляется записью в журнал, чтобы его
    ///   нашли, а не искали причину тишины.
    fn report(&self, outcome: Result<()>) {
        let Err(e) = outcome else { return };
        if e.loses_record() {
            return;
        }
        use std::sync::atomic::Ordering;
        Counters::bump(&self.inner.writer.counters().rejected);
        let bit = contract_bit(&e);
        if self.inner.announced.fetch_or(bit, Ordering::Relaxed) & bit == 0 {
            // Само объявление идёт обычным путём и может быть потеряно под
            // нагрузкой; повторять его незачем — счётчик остаётся.
            let _ = self.try_log_text(Level::Error, "dduroc", e.to_string(), None);
        }
    }

    /// Учесть отказ, случившийся слоем выше — при сериализации полей события
    /// или в мосте из `tracing`.
    ///
    /// Нужно, чтобы такой отказ попал в те же счётчики и то же однократное
    /// объявление, что и отказы самого движка: иначе типизированный слой поверх
    /// ручки терял бы записи тише, чем она сама.
    pub fn note_failure(&self, e: Error) {
        self.report(Err(e));
    }

    /// Записать событие с уже сериализованным payload'ом.
    pub fn log_raw(&self, event: EventId, payload: &[u8], span: Option<SpanId>) {
        self.report(self.try_log_raw(event, payload, span));
    }

    /// То же с ответом об исходе — см. [`Namespace::try_log_payload`].
    pub fn try_log_raw(&self, event: EventId, payload: &[u8], span: Option<SpanId>) -> Result<()> {
        self.try_log_payload(event, Payload::from_slice(payload), span)
    }

    /// Записать событие; payload уже собран в буфере записи.
    ///
    /// Ничего не возвращает намеренно: логирование не должно влиять на
    /// управление в прикладном коде, а сделать с отказом на месте вызова
    /// нечего — повтор при отстающем диске только усугубляет. Что именно
    /// случилось, видно в [`crate::stats::Stats`] и в самом потоке записей;
    /// кому нужен ответ на месте — [`Namespace::try_log_payload`].
    pub fn log_payload(&self, event: EventId, payload: Payload, span: Option<SpanId>) {
        self.report(self.try_log_payload(event, payload, span));
    }

    /// Записать событие и сказать, чем это кончилось.
    ///
    /// `Err` бывает двух родов, и различать их важнее, чем текст:
    /// [`Error::loses_record`] — запись не дошла до носителя (диск отстаёт),
    /// [`Error::breaks_contract`] — событие не из этой схемы, то есть дефект
    /// сборки.
    ///
    /// Вердикт — вызывающему целиком: нарушение контракта здесь **не**
    /// попадает в счётчики и не объявляется в журнале (это делают «тихие»
    /// методы). Обработали отказ — он ваш; отдать его движку —
    /// [`Namespace::note_failure`]. Потери диска учтены в любом случае:
    /// их считает сам writer.
    pub fn try_log_payload(
        &self,
        event: EventId,
        payload: Payload,
        span: Option<SpanId>,
    ) -> Result<()> {
        let Some(desc) = self.inner.schema.event(event) else {
            return Err(Error::UnknownEvent {
                schema: self.inner.schema.name,
                event: event.0,
            });
        };
        let item = Staged {
            ns: self.inner.id,
            channel: self.channel_of(desc.class)?,
            at: self.stamp(),
            record: StagedRecord::Message {
                event,
                span,
                payload,
            },
        };
        self.inner.writer.write(
            item,
            desc.class == StorageClass::Critical,
            &self.inner.drops,
        )
    }

    /// Записать свободный текст без схемы: мост из `tracing`, panic-handler.
    ///
    /// `target` — источник строки (`"app"`, имя модуля); он интернируется
    /// (`Arc<str>`), потому что у моста один и тот же target повторяется
    /// тысячами, но на месте вызова достаточно строкового литерала.
    pub fn log_text(
        &self,
        level: Level,
        target: impl Into<Arc<str>>,
        text: impl Into<Box<str>>,
        span: Option<SpanId>,
    ) {
        self.report(self.try_log_text(level, target, text, span));
    }

    /// То же с ответом об исходе.
    pub fn try_log_text(
        &self,
        level: Level,
        target: impl Into<Arc<str>>,
        text: impl Into<Box<str>>,
        span: Option<SpanId>,
    ) -> Result<()> {
        let target = target.into();
        // Свободный текст не объявлен в схеме, поэтому и класса хранения у
        // него нет. Ошибочный уровень просится в критический канал, но схема
        // не обязана его объявлять: отказать значило бы промолчать ровно там,
        // где текст и пишется ради того, чтобы не молчать (объявление дефекта
        // сборки, сообщение моста, паника). Берём критический, если он есть,
        // иначе обычный — он есть всегда (`Schema::classes`).
        //
        // Очередь выбирается по каналу, который достался на самом деле:
        // ожидание места ради доставки в канал с отложенной синхронизацией
        // было бы платой без гарантии.
        let (channel, critical) = match level >= Level::Error {
            true => match self.channel_of(StorageClass::Critical) {
                Ok(idx) => (idx, true),
                Err(_) => (self.channel_of(StorageClass::Default)?, false),
            },
            false => (self.channel_of(StorageClass::Default)?, false),
        };
        let item = Staged {
            ns: self.inner.id,
            channel,
            at: self.stamp(),
            record: StagedRecord::Text {
                level,
                span,
                target,
                text: text.into(),
            },
        };
        self.inner.writer.write(item, critical, &self.inner.drops)
    }

    /// Открыть ряд телеметрии.
    ///
    /// Ряд — это метрика, и ничего кроме: рантайм-размерностей нет. Ручка
    /// разрешает дескриптор один раз, дальше отсчёт стоит один вызов без
    /// всякого поиска. Четыре датчика температуры — четыре метрики схемы:
    /// иначе различающий их признак пришлось бы писать в каждый отсчёт.
    ///
    /// Тип значения приходит из константы метрики, поэтому `sample` принимает
    /// только объявленное — см. [`crate::metric`]. Открытие возвращает
    /// `Result`, потому что метрика может быть из чужой схемы; это один вызов
    /// на ряд, а не на отсчёт.
    pub fn series<M>(&self, metric: Metric<M>) -> Result<Series<M>> {
        let id: MetricId = metric.into();
        let desc = self.metric_desc(id)?;
        Ok(Series {
            ns: self.clone(),
            metric: id,
            channel: self.channel_of(desc.class)?,
            critical: desc.class == StorageClass::Critical,
            value_type: desc.value_type,
            desc,
            _value: std::marker::PhantomData,
        })
    }

    /// Открыть ряд по идентификатору, известному только в рантайме.
    ///
    /// Тип значения на этапе компиляции неизвестен, поэтому такой ряд
    /// принимает лишь [`Series::sample_raw`], который сверяет тип со схемой.
    /// Нужно веб-слою и миграциям — прикладному коду нужен [`Namespace::series`].
    pub fn series_untyped(&self, metric: MetricId) -> Result<Series<Untyped>> {
        let desc = self.metric_desc(metric)?;
        Ok(Series {
            ns: self.clone(),
            metric,
            channel: self.channel_of(desc.class)?,
            critical: desc.class == StorageClass::Critical,
            value_type: desc.value_type,
            desc,
            _value: std::marker::PhantomData,
        })
    }

    fn metric_desc(&self, metric: MetricId) -> Result<&'static MetricDesc> {
        self.inner
            .schema
            .metric(metric)
            .ok_or(Error::UnknownMetric {
                metric_id: metric.0,
            })
    }

    /// Выставить границы значений поверх схемных.
    ///
    /// Для случая, когда они известны только в рантайме: внешняя система
    /// определила модель железа и знает, что для этого усилителя нормально.
    /// Диапазоны записываются так же, как в схеме, — обычными range-выражениями:
    ///
    /// ```text
    /// ns.set_thresholds(metrics::TempPa, ..=60.0, ..=75.0)?;   // сверху
    /// ns.set_thresholds(metrics::Vswr, 1.0..=1.5, 1.0..=2.0)?; // с двух сторон
    /// ```
    ///
    /// **На диск не пишется.** Границы — свойство установки, а не измерения;
    /// подробнее в [`crate::limits`].
    pub fn set_thresholds(
        &self,
        metric: impl Into<MetricId>,
        warn: impl std::ops::RangeBounds<f64>,
        alarm: impl std::ops::RangeBounds<f64>,
    ) -> Result<()> {
        self.set_limits(metric, MetricLimits::numeric(Thresholds::new(warn, alarm)))
    }

    /// Выставить переопределение целиком: границы, важность состояний или и то
    /// и другое.
    pub fn set_limits(&self, metric: impl Into<MetricId>, limits: MetricLimits) -> Result<()> {
        self.inner
            .limits
            .set(&self.inner.schema, metric.into(), Some(limits))
    }

    /// Снять переопределение: снова действует объявленное в схеме.
    pub fn clear_limits(&self, metric: impl Into<MetricId>) -> Result<()> {
        self.inner
            .limits
            .set(&self.inner.schema, metric.into(), None)
    }

    /// Взять диагноз метрики целиком на себя: замыкание вместо диапазонов.
    ///
    /// Для правил, которые данными не выразить, — гистерезис, зависимость от
    /// захваченного контекста (модель железа, режим работы). Побеждает и
    /// схему, и [`Namespace::set_limits`]; значение приходит числом (код
    /// состояния — как число), blob-метрике замыкание не поставить. Как и
    /// остальные пределы, на диск не пишется — читатель дампа его не увидит.
    pub fn set_severity_fn(
        &self,
        metric: impl Into<MetricId>,
        check: impl Fn(f64) -> Severity + Send + Sync + 'static,
    ) -> Result<()> {
        self.inner
            .limits
            .set_fn(&self.inner.schema, metric.into(), Some(Box::new(check)))
    }

    /// Снять замыкание диагноза: снова действуют данные — переопределения и
    /// схема.
    pub fn clear_severity_fn(&self, metric: impl Into<MetricId>) -> Result<()> {
        self.inner
            .limits
            .set_fn(&self.inner.schema, metric.into(), None)
    }

    /// Действующие пределы метрики: схема плюс переопределения.
    pub fn limits(&self, metric: impl Into<MetricId>) -> Result<EffectiveLimits> {
        self.inner
            .limits
            .effective(&self.inner.schema, metric.into())
    }

    /// Насколько значение требует внимания по действующим пределам.
    ///
    /// Сама запись пределов не касается: движок не решает за приложение, что
    /// делать с выходом за границы, и не порождает событий сам. Метод — для
    /// того, кто хочет проверить значение (и, например, записать событие).
    pub fn severity_of(&self, metric: impl Into<MetricId>, value: &OwnedValue) -> Severity {
        self.inner
            .limits
            .severity_of(&self.inner.schema, metric.into(), &value.as_value())
    }

    /// Начать спан. Возвращает страж: конец записывается при его уничтожении,
    /// в том числе при развёртке стека — незакрытый спан неотличим от краха.
    ///
    /// Страж выдаётся всегда, даже если начало спана не удалось записать:
    /// иначе прикладной код получал бы `?` на месте, где вложенность важнее
    /// самой записи, а читатель и так обязан уметь показать спан без начала —
    /// именно так выглядит спан, оборванный крахом процесса.
    pub fn span(&self, kind: SpanKindId) -> SpanGuard {
        self.span_with_parent(kind, None)
    }

    /// Начать спан с явным родителем.
    pub fn span_with_parent(&self, kind: SpanKindId, parent: Option<SpanId>) -> SpanGuard {
        let span = next_span_id(&self.inner.next_span);
        // Вид спана не из этой схемы — не повод не открывать спан: класс
        // хранения берём обычный, о нарушении контракта сообщаем как обычно.
        let class = self
            .inner
            .schema
            .span(kind)
            .map_or(StorageClass::Default, |d| d.class);
        let channel = match self.channel_of(class) {
            Ok(c) => c,
            Err(e) => {
                self.report(Err(e));
                ChannelIdx(0)
            }
        };
        let critical = class == StorageClass::Critical;

        if self.inner.schema.span(kind).is_none() {
            self.report(Err(Error::UnknownSpanKind {
                schema: self.inner.schema.name,
                kind: kind.0,
            }));
        }

        self.report(self.inner.writer.write(
            Staged {
                ns: self.inner.id,
                channel,
                at: self.stamp(),
                record: StagedRecord::SpanStart { span, kind, parent },
            },
            critical,
            &self.inner.drops,
        ));

        SpanGuard {
            ns: self.clone(),
            span,
            channel,
            critical,
            closed: false,
        }
    }

    /// Дождаться, пока накопленное окажется на носителе.
    pub fn sync(&self) -> Result<()> {
        self.inner.writer.sync(Some(self.inner.id))
    }
}

/// Открытый ряд телеметрии.
///
/// Идентичность ряда — метрика. Дескриптор разрешён при открытии, поэтому
/// отсчёт не делает ни поиска по схеме, ни обращения к какому-либо реестру.
///
/// Параметр `M` — маркер типа значения из константы метрики; он и определяет,
/// что примет [`Series::sample`].
#[derive(Debug, Clone)]
pub struct Series<M> {
    ns: Namespace,
    metric: MetricId,
    channel: ChannelIdx,
    critical: bool,
    /// Копия объявленного типа значения. Дескриптор весит полторы сотни байт
    /// и лежит в `.rodata`; сверка на каждом отсчёте не должна за ним ходить.
    value_type: dduroc_format::ValueType,
    desc: &'static crate::schema::MetricDesc,
    _value: std::marker::PhantomData<fn() -> M>,
}

impl<M> Series<M> {
    pub fn metric(&self) -> MetricId {
        self.metric
    }

    pub fn value_type(&self) -> dduroc_format::ValueType {
        self.value_type
    }

    /// Как величина ведёт себя между отсчётами.
    pub fn kind(&self) -> MetricKind {
        self.desc.kind
    }

    /// Имя метрики из схемы.
    pub fn name(&self) -> &'static str {
        self.desc.name
    }

    /// Записать отсчёт с уже собранным значением.
    ///
    /// Путь для случая, когда тип известен лишь в рантайме. Прикладному коду
    /// нужен [`Series::sample`]: там тип сверяет компилятор.
    pub fn sample_raw(&self, value: OwnedValue) {
        self.ns.report(self.try_sample_raw(value));
    }

    /// То же с ответом об исходе.
    pub fn try_sample_raw(&self, value: OwnedValue) -> Result<()> {
        if value.value_type() != self.value_type {
            return Err(Error::ValueTypeMismatch {
                metric_id: self.metric.0,
                declared: self.value_type,
                got: value.value_type(),
            });
        }
        let item = Staged {
            ns: self.ns.inner.id,
            channel: self.channel,
            at: self.ns.stamp(),
            record: StagedRecord::Sample {
                metric: self.metric,
                value,
            },
        };
        self.ns
            .inner
            .writer
            .write(item, self.critical, &self.ns.inner.drops)
    }

    /// Важность значения по действующим пределам.
    pub fn severity_of(&self, value: &OwnedValue) -> Severity {
        self.ns.severity_of(self.metric, value)
    }
}

impl<M> Series<M> {
    /// Записать отсчёт.
    ///
    /// Принимает только то, что объявлено у метрики: `Series<f32>` — `f32`,
    /// `Series<LinkState>` — состояния этой метрики и ничьи другие. Ничего не
    /// возвращает по той же причине, что и запись событий (см.
    /// [`Namespace::log_payload`]); кому нужен ответ — [`Series::try_sample`].
    #[inline]
    pub fn sample<V: MetricValue<M>>(&self, value: V) {
        self.ns.report(self.try_sample(value));
    }

    /// То же с ответом об исходе.
    #[inline]
    pub fn try_sample<V: MetricValue<M>>(&self, value: V) -> Result<()> {
        self.try_sample_raw(self.coerce(value.into_owned()))
    }

    /// Привести значение к объявленному представлению.
    ///
    /// Нужно ровно для перечислений: код состояния приходит целым, а метрика
    /// может быть объявлена как `bool` (два состояния) или `i64`. Остальные
    /// значения проходят как есть — их тип уже совпадает по построению.
    #[inline]
    fn coerce(&self, value: OwnedValue) -> OwnedValue {
        match (self.value_type, &value) {
            (dduroc_format::ValueType::Bool, OwnedValue::U64(code)) => OwnedValue::Bool(*code != 0),
            (dduroc_format::ValueType::I64, OwnedValue::U64(code)) => OwnedValue::I64(*code as i64),
            _ => value,
        }
    }
}

/// Страж спана: конец записывается при уничтожении.
///
/// Значение обязано быть куда-то положено: `ns.span(kind);` без привязки
/// уничтожает стража тем же выражением, которое его создало, и спан
/// схлопывается в нулевую длительность — две записи подряд вместо отрезка
/// работы. Внешне это выглядит как «спан есть, но пустой», и разбирать такое
/// в журнале не по чему.
#[derive(Debug)]
#[must_use = "спан живёт, пока жив страж: `ns.span(kind);` закрывает его сразу же"]
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

    /// Неймспейс, которому принадлежит спан — нужен слою поверх ручки.
    pub fn namespace(&self) -> &Namespace {
        &self.ns
    }

    /// Вложенный спан: родителем становится этот.
    pub fn child(&self, kind: SpanKindId) -> SpanGuard {
        self.ns.span_with_parent(kind, Some(self.span))
    }

    /// Записать событие, привязанное к этому спану.
    pub fn log_raw(&self, event: EventId, payload: &[u8]) {
        self.ns.log_raw(event, payload, Some(self.span));
    }

    /// То же, но payload уже собран в буфере записи.
    pub fn log_payload(&self, event: EventId, payload: Payload) {
        self.ns.log_payload(event, payload, Some(self.span));
    }

    /// То же с ответом об исходе.
    pub fn try_log_payload(&self, event: EventId, payload: Payload) -> Result<()> {
        self.ns.try_log_payload(event, payload, Some(self.span))
    }

    /// Закрыть явно, увидев ошибку записи. Обычно не нужно: закрытие
    /// происходит при уничтожении.
    ///
    /// В отличие от уничтожения, здесь действует обычная политика канала:
    /// у критического спана вызывающий подождёт места в очереди, зато конец
    /// спана гарантированно не потеряется.
    pub fn close(mut self) -> Result<()> {
        self.closed = true;
        self.write_end(true)
    }

    fn write_end(&self, may_wait: bool) -> Result<()> {
        let item = Staged {
            ns: self.ns.inner.id,
            channel: self.channel,
            at: self.ns.stamp(),
            record: StagedRecord::SpanEnd { span: self.span },
        };
        let writer = &self.ns.inner.writer;
        if may_wait {
            writer.write(item, self.critical, &self.ns.inner.drops)
        } else {
            writer.write_no_wait(item, self.critical, &self.ns.inner.drops)
        }
    }
}

impl Drop for SpanGuard {
    fn drop(&mut self) {
        if !self.closed {
            // Без ожидания места — намеренно. `Drop` вызывается в том числе
            // при развёртке стека после паники, и у критического спана
            // обычный путь ждал бы место в очереди до пяти секунд: аварийное
            // завершение превратилось бы в зависание, а вложенные стражи
            // сложили бы свои таймауты друг к другу.
            //
            // Цена — конец спана может быть потерян под давлением. Он учтён
            // счётчиком потерь и отметкой в потоке, а незакрытый спан читатель
            // и так обязан уметь показать: спан, оборванный крахом процесса,
            // выглядит точно так же.
            let _ = self.write_end(false);
        }
    }
}

/// Бит вида нарушения контракта: объявляется по одному разу на вид.
fn contract_bit(e: &Error) -> u8 {
    match e {
        Error::UnknownEvent { .. } => 1 << 0,
        Error::UnknownMetric { .. } => 1 << 1,
        Error::UnknownSpanKind { .. } => 1 << 2,
        Error::ValueTypeMismatch { .. } => 1 << 3,
        Error::ClassNotDeclared { .. } => 1 << 4,
        Error::EncodeFailed { .. } => 1 << 5,
        // Прочее (например, отказ носителя) — тоже один раз, но своим битом:
        // залить журнал повторами оно способно ровно так же.
        _ => 1 << 7,
    }
}

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
            class: StorageClass::Default,
            tags: &["rf"],
            templates: &["power {dbm}", "мощность {dbm}"],
            fields: &[],
            decoders: None,
        },
        EventDesc {
            id: EventId(2),
            name: "Alarm",
            level: Level::Error,
            class: StorageClass::Critical,
            tags: &[],
            templates: &["alarm", "авария"],
            fields: &[],
            decoders: None,
        },
    ];
    static LINK_STATES: &[crate::schema::StateDesc] = &[
        crate::schema::StateDesc {
            code: 0,
            name: "Los",
            severity: Severity::Alarm,
        },
        crate::schema::StateDesc {
            code: 1,
            name: "Sync",
            severity: Severity::Warn,
        },
        crate::schema::StateDesc {
            code: 2,
            name: "Lock",
            severity: Severity::Normal,
        },
    ];
    static METRICS: &[MetricDesc] = &[
        MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(1),
            name: "temp",
            value_type: ValueType::F32,
            class: StorageClass::Default,
            unit: "°C",
            tags: &["thermal"],
            kind: MetricKind::Gauge,
            states: &[],
            thresholds: crate::schema::Thresholds {
                warn: crate::schema::Range {
                    min: None,
                    max: Some(70.0),
                },
                alarm: crate::schema::Range {
                    min: None,
                    max: Some(85.0),
                },
            },
        },
        MetricDesc {
            warn_if: None,
            alarm_if: None,
            id: MetricId(2),
            name: "link",
            value_type: ValueType::U64,
            class: StorageClass::Default,
            unit: "",
            tags: &["rf"],
            kind: MetricKind::State,
            states: LINK_STATES,
            thresholds: crate::schema::Thresholds::NONE,
        },
    ];

    use crate::metric::{Metric, MetricState, MetricValue};

    /// Типизированные константы метрик — то, что для настоящей схемы порождает
    /// макрос: тип значения приходит вместе с идентификатором.
    const TEMP: Metric<f32> = Metric::new(MetricId(1));
    const LINK: Metric<LinkState> = Metric::new(MetricId(2));

    /// Состояние канала — то, что для настоящей схемы породил бы макрос.
    #[derive(Debug, Clone, Copy)]
    enum LinkState {
        Los = 0,
        Sync = 1,
        Lock = 2,
    }

    impl MetricValue<LinkState> for LinkState {
        fn into_owned(self) -> OwnedValue {
            OwnedValue::U64(self as u64)
        }
    }

    impl MetricState for LinkState {
        fn metric() -> MetricId {
            MetricId(2)
        }
        fn code(self) -> u64 {
            self as u64
        }
        fn name(self) -> &'static str {
            LINK_STATES[self as usize].name
        }
    }
    static SPANS: &[SpanDesc] = &[SpanDesc {
        id: SpanKindId(1),
        name: "Calibration",
        class: StorageClass::Default,
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

    fn open_store(dir: &Path) -> Arc<Store> {
        Store::open(StoreConfig::new(dir).with_budget_per_class(16 * 1024 * 1024)).unwrap()
    }

    #[test]
    fn writes_land_on_disk() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        for i in 0..100 {
            ns.log_raw(EventId(1), &[i as u8; 4], None);
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
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        ns.log_raw(EventId(2), &[1, 2, 3], None);
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
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        const N: u64 = 50_000;
        let mut accepted = 0u64;
        let mut refused = 0u64;
        for i in 0..N {
            match ns.try_log_raw(EventId(1), &[i as u8; 8], None) {
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
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        const BURST: usize = 500;
        for i in 0..BURST {
            while ns.try_log_raw(EventId(2), &[i as u8], None).is_err() {
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
        let store = open_store(dir.path());
        let a = store.namespace("orc-radio-0", schema()).unwrap();
        let err = store.namespace("orc-radio-0", schema()).unwrap_err();
        assert!(matches!(err, Error::NamespaceBusy(_)), "получено {err}");

        // Отпущенное имя обязано освободиться: иначе сервис не смог бы
        // переоткрыть свой неймспейс после перенастройки.
        drop(a);
        store
            .namespace("orc-radio-0", schema())
            .expect("имя свободно после уничтожения ручки");
    }

    #[test]
    fn foreign_schema_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        drop(store.namespace("orc-radio-0", schema()).unwrap());
        store.shutdown();
        drop(store);

        let store2 = open_store(dir.path());
        let other = Schema {
            name: "другая-схема",
            ..schema()
        };
        let err = store2.namespace("orc-radio-0", other).unwrap_err();
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

        let store = open_store(dir.path());
        let err = store.namespace("orc-radio-0", schema()).unwrap_err();
        assert!(
            matches!(err, Error::ProtocolFromFuture { stored: 99, .. }),
            "получено {err}"
        );
    }

    #[test]
    fn pending_migration_is_reported_and_meta_is_not_stamped() {
        // `protocol_version` в мете — версия последней ЗАВЕРШЁННОЙ миграции.
        // Проштамповав её при подъёме, этот билд объявил бы неймспейс
        // мигрированным, хотя физического прогона ещё нет. Будущая миграция
        // обошла бы старые сегменты стороной, и их разобрали бы декодерами
        // новой версии — молча и неверно. Регрессия была бы совершенно тихой.
        use crate::schema::{DecodeError, Migration, MigrationInput, MigrationOutcome};

        fn noop(
            _: MigrationInput<'_>,
        ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::AsIs))
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: true,
            events: &[],
            metrics: &[],
            migrate: noop,
        }];

        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            assert_eq!(ns.pending_migration(), None, "версии совпадают");
            ns.log_raw(EventId(1), &[1], None);
            ns.sync().unwrap();
            store.shutdown();
        }

        let v2 = Schema {
            version: ProtocolVersion(2),
            migrations: STEPS,
            ..schema()
        };
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", v2).unwrap();
        assert_eq!(
            ns.pending_migration(),
            Some((1, 2)),
            "незавершённая миграция обязана быть названа"
        );
        assert_eq!(
            ns.meta().protocol_version,
            1,
            "мета остаётся на версии последней завершённой миграции"
        );
        ns.log_raw(EventId(1), &[2], None);
        ns.sync().unwrap();
        store.shutdown();

        // На диске мета по-прежнему объявляет версию 1.
        let raw = std::fs::read(dir.path().join("orc-radio-0").join(NS_META)).unwrap();
        let meta: NsMeta = postcard::from_bytes(&raw).unwrap();
        assert_eq!(meta.protocol_version, 1);

        // А сегменты несут каждый свою версию — смешанное состояние легально.
        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let mut versions: Vec<u16> = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "seg"))
            .map(|p| {
                crate::segment::SegmentReader::open(&p)
                    .unwrap()
                    .header()
                    .protocol_version
                    .0
            })
            .collect();
        versions.sort_unstable();
        assert_eq!(
            versions,
            vec![1, 2],
            "старый сегмент сохранил свою версию, новый получил текущую"
        );
    }

    #[test]
    fn migrate_rewrites_history_and_stamps_the_meta() {
        // Полный цикл физического прогона: запись v1 → подъём v2 →
        // `Namespace::migrate` — старый сегмент переписан по шагу, активный
        // не тронут, мета заштампована, повтор пуст. Всё это на живом
        // хранилище с работающим writer'ом: фиксация идёт через него.
        use crate::migrate::MigrationReport;
        use crate::schema::{DecodeError, Migration, MigrationInput, MigrationOutcome};

        fn step(
            r: MigrationInput<'_>,
        ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
            match (r.event_id(), r.metric_id()) {
                // PowerSet перекодируется в фиксированный новый payload.
                (Some(EventId(1)), _) => Ok(Some(MigrationOutcome::Message {
                    event: EventId(1),
                    payload: vec![0xAA, 0xBB],
                })),
                // Отсчёты temp удаляются целиком.
                (_, Some(MetricId(1))) => Ok(None),
                _ => Ok(Some(MigrationOutcome::AsIs)),
            }
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: false,
            events: &[EventId(1)],
            metrics: &[MetricId(1)],
            migrate: step,
        }];

        let dir = tempfile::tempdir().unwrap();
        // Запуск 1: история версии 1 — событие, отсчёт и спан.
        {
            let store = open_store(dir.path());
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            ns.log_raw(EventId(1), &[1, 2, 3], None);
            ns.series(TEMP).unwrap().sample(36.6);
            ns.log_raw(EventId(1), &[4, 5], None);
            ns.sync().unwrap();
            store.shutdown();
        }

        let v2 = Schema {
            version: ProtocolVersion(2),
            migrations: STEPS,
            ..schema()
        };
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", v2).unwrap();
        assert_eq!(ns.pending_migration(), Some((1, 2)));

        // Живой сегмент версии 2 — прогон обязан пройти мимо него.
        ns.log_raw(EventId(1), &[9], None);
        ns.sync().unwrap();

        let report = ns.migrate().expect("прогон проходит");
        assert_eq!(report.rewritten, 1, "{report:?}");
        assert_eq!(report.already_current, 1, "активный сегмент не тронут");
        assert_eq!(report.records_rewritten, 2, "два PowerSet пережили шаг");
        assert_eq!(report.records_dropped, 1, "отсчёт temp удалён");
        assert_eq!(report.skipped_untouched + report.emptied, 0, "{report:?}");

        assert_eq!(ns.pending_migration(), None, "долга больше нет");
        assert_eq!(ns.meta().protocol_version, 2, "мета заштампована в памяти");
        let raw = std::fs::read(dir.path().join("orc-radio-0").join(NS_META)).unwrap();
        let meta: NsMeta = postcard::from_bytes(&raw).unwrap();
        assert_eq!(meta.protocol_version, 2, "и на диске");

        // Переписанный сегмент: версия текущая, footer есть, содержимое — по
        // шагу: payload'ы заменены, отсчётов не осталось.
        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let mut rewritten_payloads = Vec::new();
        let mut samples = 0;
        for entry in std::fs::read_dir(&seg_dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|x| x != "seg") {
                continue;
            }
            let reader = crate::segment::SegmentReader::open(&path).unwrap();
            assert_eq!(
                reader.header().protocol_version.0,
                2,
                "прежних версий на диске не осталось: {path:?}"
            );
            let mut buf = Vec::new();
            for offset in reader.scan_block_offsets().0 {
                reader.read_block_at(offset, &mut buf).unwrap();
                let block = crate::segment::parse_block(&buf).unwrap().unwrap();
                for item in block.records() {
                    match item.unwrap().1 {
                        dduroc_format::Record::Message(m) => {
                            rewritten_payloads.push(m.payload.to_vec());
                        }
                        dduroc_format::Record::Sample(_) => samples += 1,
                        _ => {}
                    }
                }
            }
        }
        rewritten_payloads.sort();
        assert_eq!(
            rewritten_payloads,
            vec![vec![9], vec![0xAA, 0xBB], vec![0xAA, 0xBB]],
            "старые payload'ы переписаны шагом, новый — как был"
        );
        assert_eq!(samples, 0, "удалённые шагом отсчёты не воскресают");

        // Повторный прогон — честный no-op.
        assert_eq!(ns.migrate().unwrap(), MigrationReport::default());
        store.shutdown();
    }

    #[test]
    fn an_untouched_segment_is_skipped_and_keeps_its_version() {
        // Сегмент, в котором нет ни одного затронутого типа, не тратит цикла
        // записи флеша: он не переписывается и сохраняет прежнюю версию в
        // заголовке — при заштампованной мете. Это легально: незатронутость
        // и означает, что текущие декодеры читают его верно.
        use crate::schema::{DecodeError, Migration, MigrationInput, MigrationOutcome};

        fn nope(
            _: MigrationInput<'_>,
        ) -> std::result::Result<Option<MigrationOutcome>, DecodeError> {
            Ok(Some(MigrationOutcome::Message {
                event: EventId(2),
                payload: vec![0xEE],
            }))
        }
        static STEPS: &[Migration] = &[Migration {
            from: 1,
            touches_all: false,
            // Шаг затрагивает только Alarm — его в истории не будет.
            events: &[EventId(2)],
            metrics: &[],
            migrate: nope,
        }];

        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            let ns = store.namespace("orc-radio-0", schema()).unwrap();
            ns.log_raw(EventId(1), &[7], None);
            ns.sync().unwrap();
            store.shutdown();
        }

        let v2 = Schema {
            version: ProtocolVersion(2),
            migrations: STEPS,
            ..schema()
        };
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", v2).unwrap();
        let report = ns.migrate().unwrap();
        assert_eq!(report.skipped_untouched, 1, "{report:?}");
        assert_eq!(report.rewritten, 0, "флеш не тронут");
        assert_eq!(ns.pending_migration(), None, "мета заштампована");

        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let old = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .unwrap();
        let reader = crate::segment::SegmentReader::open(&old).unwrap();
        assert_eq!(
            reader.header().protocol_version.0,
            1,
            "незатронутый сегмент остаётся при своей версии — и это легально"
        );
        store.shutdown();
    }

    #[test]
    fn bad_namespace_names_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        for bad in ["../escape", "a/b", "", ".hidden"] {
            assert!(
                store.namespace(bad, schema()).is_err(),
                "{bad:?} обязано отвергаться"
            );
        }
    }

    #[test]
    fn telemetry_series_and_spans() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        let temp = ns.series(TEMP).unwrap();
        for i in 0..50 {
            temp.sample(20.0 + i as f32);
        }
        // Динамический путь сверяет тип со схемой сам: у типизированного
        // `sample` такое значение просто не собралось бы.
        let dyn_temp = ns.series_untyped(MetricId(1)).unwrap();
        let e = dyn_temp
            .try_sample_raw(OwnedValue::U64(1))
            .expect_err("тип значения обязан совпасть с объявленным");
        assert!(e.breaks_contract(), "это дефект вызова, а не потеря: {e}");

        {
            let cal = ns.span(SpanKindId(1));
            cal.log_raw(EventId(1), &[7]);
            let _child = cal.child(SpanKindId(1));
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
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        const N: usize = 20_000;
        let mut accepted = 0u64;
        for i in 0..N {
            // Ретраи, чтобы отделить потери очереди (штатное поведение
            // обычного канала) от потерь при синхронизации.
            while ns.try_log_raw(EventId(1), &[i as u8; 4], None).is_err() {
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
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        const N: usize = 20_000;
        let mut accepted = 0u64;
        for i in 0..N {
            while ns.try_log_raw(EventId(1), &[i as u8; 4], None).is_err() {
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
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        // Небольшая занятая пауза, чтобы время старта заведомо было > 0.
        let mut spin = 0u64;
        while ns.now().at.0 < 1_000 && spin < 200_000_000 {
            spin += 1;
        }
        let before = ns.now();
        ns.log_raw(EventId(1), &[1], None);
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
            parsed.start() >= before && parsed.start() <= after,
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
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();
        drop(store);

        for i in 0..100 {
            ns.try_log_raw(EventId(1), &[i as u8], None)
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
        let store = open_store(dir.path());

        let broken = Schema {
            version: ProtocolVersion(0), // недопустима
            ..schema()
        };
        assert!(store.namespace("orc-radio-0", broken).is_err());
        // Имя снова свободно.
        store
            .namespace("orc-radio-0", schema())
            .expect("имя обязано освободиться после неудачи");
    }

    #[test]
    fn enum_states_are_written_as_plain_integers() {
        // Смысл модели: состояние на диске — обычное целое, всё остальное
        // (имя, важность) живёт в схеме и места не занимает.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        let link = ns.series(LINK).unwrap();
        assert_eq!(link.kind(), MetricKind::State);
        for st in [LinkState::Los, LinkState::Sync, LinkState::Lock] {
            link.sample(st);
        }
        ns.sync().unwrap();

        let stats = store.stats();
        assert_eq!(stats.records_written, 3, "ни одной служебной записи");
        assert!(stats.is_clean(), "{stats:?}");

        // Три отсчёта: каждый — заголовок, дельта времени, метрика, код.
        assert!(
            stats.bytes_written < 32 + 3 * 8,
            "три состояния заняли {} байт вместе с заголовком блока",
            stats.bytes_written
        );
    }

    #[test]
    fn sealed_segment_lists_its_metrics_in_the_footer() {
        // По этому множеству миграция решает, переписывать ли сегмент, а
        // читатель — есть ли смысл его открывать. Пустое множество означало бы,
        // что миграция молча пропустит всю историю телеметрии, а поиск
        // состояния перед окном отбросит сегмент, в котором оно есть.
        // Регрессия была бы совершенно тихой — отсюда тест.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        ns.series(TEMP).unwrap().sample(36.6);
        ns.series(LINK).unwrap().sample(LinkState::Lock);
        ns.log_raw(EventId(1), &[1], None);
        store.shutdown();

        let seg_dir = dir.path().join("orc-radio-0").join("default");
        let path = std::fs::read_dir(&seg_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().is_some_and(|x| x == "seg"))
            .expect("сегмент создан");

        let reader = crate::segment::SegmentReader::open(&path).unwrap();
        assert!(reader.is_sealed(), "shutdown запечатывает сегмент");
        let footer = reader.footer().expect("footer читается");
        assert_eq!(
            footer.metrics,
            vec![MetricId(1), MetricId(2)],
            "обе метрики обязаны быть перечислены"
        );
        assert_eq!(footer.events, vec![EventId(1)]);
        assert!(
            footer.touches(&[], &[MetricId(2)]),
            "миграция обязана увидеть, что сегмент затронут"
        );
        assert!(!footer.touches(&[], &[MetricId(9)]));
    }

    #[test]
    fn state_of_a_foreign_metric_is_refused() {
        // Коды состояний у разных метрик свои. Записать чужой код значило бы
        // получить график, подписанный чужими именами.
        //
        // На типизированном пути это **ошибка компиляции**: `Series<f32>` не
        // примет `LinkState`, и проверять тут нечего. Проверяется динамический
        // путь — единственный оставшийся способ подсунуть чужое значение.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        let temp = ns.series_untyped(MetricId(1)).unwrap();
        let err = temp
            .try_sample_raw(OwnedValue::U64(LinkState::Lock as u64))
            .unwrap_err();
        assert!(
            matches!(err, Error::ValueTypeMismatch { .. }),
            "получено {err}"
        );
        assert!(err.breaks_contract());
    }

    #[test]
    fn limits_default_from_schema_and_override_at_runtime() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        // Дефолты схемы.
        let sev = |v: f32| ns.severity_of(MetricId(1), &OwnedValue::F32(v));
        assert_eq!(sev(36.6), Severity::Normal);
        assert_eq!(sev(75.0), Severity::Warn);
        assert_eq!(sev(90.0), Severity::Alarm);

        // Внешняя система определила модель усилителя и сузила пределы —
        // теми же range-выражениями, что и в схеме.
        ns.set_thresholds(TEMP, ..=40.0, ..=50.0).unwrap();
        assert_eq!(sev(45.0), Severity::Warn, "схема сказала бы Normal");
        let eff = ns.limits(MetricId(1)).unwrap();
        assert!(eff.overridden);
        assert_eq!(eff.unit, "°C");
        assert_eq!(eff.kind, MetricKind::Gauge);

        // Состояния: важность приходит из состояния, а не из диапазонов.
        let link = ns.limits(MetricId(2)).unwrap();
        assert_eq!(link.kind, MetricKind::State);
        assert_eq!(link.states.len(), 3);
        assert_eq!(link.states[0].name, "Los");
        assert_eq!(link.states[0].severity, Severity::Alarm);
        assert_eq!(
            ns.severity_of(MetricId(2), &OwnedValue::U64(0)),
            Severity::Alarm
        );

        // Пределы не попадают в поток записей.
        ns.sync().unwrap();
        assert_eq!(
            store.stats().records_written,
            0,
            "пределы — настройка, а не данные: писать их некуда"
        );
        // Снятие переопределения возвращает объявленное в схеме.
        ns.clear_limits(TEMP).unwrap();
        assert_eq!(sev(45.0), Severity::Normal);
        assert!(!ns.limits(TEMP).unwrap().overridden);

        // Метрика не из этой схемы — дефект вызова, а не потеря.
        let e = ns.clear_limits(MetricId(99)).unwrap_err();
        assert!(e.breaks_contract(), "{e}");
    }

    #[test]
    fn limits_are_per_namespace_not_per_process() {
        // У каждого экземпляра своё железо: общие на процесс пределы были бы
        // неверны для всех сразу.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let a = store.namespace("orc-radio-0", schema()).unwrap();
        let b = store.namespace("orc-radio-1", schema()).unwrap();

        a.set_thresholds(TEMP, ..=10.0, ..).unwrap();

        assert_eq!(
            a.severity_of(MetricId(1), &OwnedValue::F32(20.0)),
            Severity::Warn
        );
        assert_eq!(
            b.severity_of(MetricId(1), &OwnedValue::F32(20.0)),
            Severity::Normal,
            "сосед сохранил свои пределы"
        );
    }

    #[test]
    fn unknown_ids_are_counted_and_announced_once() {
        // Id из чужой схемы — дефект сборки. Путь записи ничего не
        // возвращает, поэтому такой вызов не должен пропадать бесследно: он
        // учитывается отдельным счётчиком и **один раз** объявляется записью в
        // журнал. Объявлять каждый значило бы залить журнал — нарушение
        // контракта повторяется на каждом витке цикла.
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ns = store.namespace("orc-radio-0", schema()).unwrap();

        let e = ns.try_log_raw(EventId(99), &[], None).unwrap_err();
        assert!(e.breaks_contract(), "{e}");
        assert!(ns.series_untyped(MetricId(99)).is_err());

        for _ in 0..100 {
            ns.log_raw(EventId(99), &[], None);
        }
        ns.sync().unwrap();

        let stats = store.stats();
        assert_eq!(stats.rejected, 100, "каждый отказ учтён");
        assert_eq!(stats.dropped, 0, "это не потеря из-за диска");
        assert!(!stats.is_clean(), "дефект обязан быть виден");
        assert_eq!(
            stats.records_written, 1,
            "объявление одно на все сто вызовов: получено {}",
            stats.records_written
        );

        // Вид спана из чужой схемы: страж выдаётся всё равно, иначе
        // вложенность прикладного кода зависела бы от схемы.
        let span = ns.span(SpanKindId(99));
        assert!(span.id().0 > 0);
    }
}
