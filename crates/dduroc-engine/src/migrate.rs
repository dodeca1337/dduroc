//! Применение шагов миграции к записям.
//!
//! Одно ядро на двоих: и чтение старых сегментов, и физический прогон
//! (`Namespace::migrate`) прогоняют записи через **одну и ту же** цепочку —
//! иначе «как прочитается» и «как перепишется» могли бы разойтись, а это два
//! ответа на один вопрос.
//!
//! # Семантика объявленных множеств
//!
//! `events`/`metrics` шага — не подсказка, а **связывающий фильтр**: шаг
//! вызывается только для записей, чьи идентификаторы в них названы. Поэтому
//! множества одновременно решают и «переписывать ли сегмент» (по footer'у),
//! и «показывать ли запись шагу» — расхождения между объявленным и
//! фактическим не существует по построению. Шаг с `touches_all` видит каждую
//! запись, включая текст и спаны.
//!
//! Записи, которых нет в множествах шага, минуют его без вызова: их байты не
//! копируются и не перекодируются.

use crate::error::{Error, Result};
use crate::schema::{DecodeError, MigratedRecord, Migration, OwnedRecord, Schema};
use crate::segment::{SegmentReader, SegmentWriter};
use crate::staged::{ChannelIdx, NsId};
use crate::writer::{MigrationCommit, Writer};
use dduroc_format::block::BlockBuilder;
use dduroc_format::record::{Message, Sample};
use dduroc_format::segment::SegmentHeader;
use dduroc_format::{EventId, FooterBuilder, MetricId, ProtocolVersion, Record, SpanId};
use std::path::Path;

/// Цепочка шагов, приводящая записи версии `seg_version` к текущей.
///
/// Пустая цепочка — сегмент уже текущей версии (или новее: этот случай
/// вызывающий обязан отсечь сам, у цепочки нет шагов «вперёд»).
///
/// Ошибка — в цепочке дыра. Для схемы, прошедшей [`Schema::validate`], это
/// недостижимо, но схему читателю отдаёт приложение, и молча пропустить шаг
/// значило бы прочитать записи в чужой раскладке.
pub fn chain(
    schema: &Schema,
    seg_version: u16,
) -> std::result::Result<Vec<&'static Migration>, ChainError> {
    chain_between(schema.migrations, seg_version, schema.version.0)
}

/// То же по голому списку шагов — для читателя, которому схема доступна
/// только частями ([`Schema`] против его воли не таскают через курсоры).
pub fn chain_between(
    migrations: &'static [Migration],
    from: u16,
    to: u16,
) -> std::result::Result<Vec<&'static Migration>, ChainError> {
    let mut steps = Vec::new();
    for v in from..to {
        steps.push(migrations.iter().find(|m| m.from == v).ok_or(ChainError {
            step_from: v,
            kind: ChainErrorKind::MissingStep,
        })?);
    }
    Ok(steps)
}

/// Затрагивает ли хоть один шаг цепочки сегмент с таким footer'ом.
///
/// `false` означает, что в сегменте нет ни одной записи, которую хоть один
/// шаг взял бы в работу, — его можно и читать текущими декодерами, и не
/// переписывать. Это одно и то же правило для чтения и для прогона.
pub fn chain_touches(steps: &[&Migration], footer: &dduroc_format::Footer) -> bool {
    steps.iter().any(|m| m.touches(footer))
}

/// Исход применения цепочки к одной записи.
#[derive(Debug)]
pub enum Chained<'a> {
    /// Ни один шаг запись не тронул: байты исходные.
    Same(Record<'a>),
    /// Запись преобразована — payload или идентификаторы новые.
    Owned(OwnedChained<'a>),
    /// Какой-то шаг запись удалил.
    Dropped,
}

impl<'a> Chained<'a> {
    /// Запись, если она пережила цепочку.
    pub fn record(&self) -> Option<Record<'_>> {
        match self {
            Chained::Same(r) => Some(*r),
            Chained::Owned(o) => Some(o.as_record()),
            Chained::Dropped => None,
        }
    }
}

/// Преобразованная запись: владеет тем, что шаг перекодировал, и заимствует
/// то, чего не трогал (значение сэмпла живёт в буфере блока).
#[derive(Debug)]
pub enum OwnedChained<'a> {
    Message {
        event: EventId,
        /// Спан исходной записи: шаг меняет тип и payload, но не привязку —
        /// у него и нет способа её выразить.
        span: Option<SpanId>,
        payload: Vec<u8>,
    },
    Sample {
        metric: MetricId,
        original: Sample<'a>,
    },
}

impl OwnedChained<'_> {
    pub fn as_record(&self) -> Record<'_> {
        match self {
            OwnedChained::Message {
                event,
                span,
                payload,
            } => Record::Message(Message {
                event: *event,
                span: *span,
                payload,
            }),
            OwnedChained::Sample { metric, original } => Record::Sample(Sample {
                metric: *metric,
                value: original.value,
            }),
        }
    }
}

/// Отказ цепочки на конкретном шаге.
///
/// Для чтения это повреждение (запись выпадает из ответа с объявлением), для
/// прогона — отказ переписывания сегмента: оригинал остаётся нетронутым.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("шаг миграции {step_from} → {}: {kind}", step_from + 1)]
pub struct ChainError {
    /// Версия, из которой мигрировал отказавший шаг.
    pub step_from: u16,
    pub kind: ChainErrorKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChainErrorKind {
    /// Шаг не смог разобрать payload: запись не той раскладки, которую он
    /// ожидал, — либо порча, либо ошибка в объявлении history.
    #[error("payload не разобрался")]
    Decode,
    /// Шаг вернул исход, не применимый к виду записи: `SampleMetric` для
    /// сообщения или `Message` для текста. Ошибка кода шага, а не данных.
    #[error("исход шага не применим к виду записи")]
    WrongOutcome,
    /// В цепочке нет шага с этой версии: схема не проходила валидацию.
    #[error("шаг отсутствует в схеме")]
    MissingStep,
}

/// Видит ли шаг эту запись.
///
/// Связывающий фильтр по объявленным множествам; `touches_all` видит всё —
/// включая текст, спаны и расширения, которые множествами не выразить.
fn step_applies(step: &Migration, record: &Record<'_>) -> bool {
    if step.touches_all {
        return true;
    }
    match record {
        Record::Message(m) => step.events.contains(&m.event),
        Record::Sample(s) => step.metrics.contains(&s.metric),
        Record::SpanStart(_) | Record::SpanEnd { .. } | Record::Text(_) | Record::Ext { .. } => {
            false
        }
    }
}

/// Прогнать запись через цепочку шагов.
///
/// Шаги применяются по порядку; каждый видит запись в раскладке,
/// оставленной предыдущим, — так прыжок через несколько версий складывается
/// из тех же шагов, что и по одной.
pub fn apply<'a>(
    steps: &[&Migration],
    record: Record<'a>,
) -> std::result::Result<Chained<'a>, ChainError> {
    let mut current = Chained::Same(record);
    for step in steps {
        let viewed = match &current {
            Chained::Same(r) => *r,
            Chained::Owned(o) => o.as_record(),
            Chained::Dropped => unreachable!("после удаления цикл прерван"),
        };
        if !step_applies(step, &viewed) {
            continue;
        }
        let failed = |kind| ChainError {
            step_from: step.from,
            kind,
        };
        let outcome = (step.migrate)(MigratedRecord { record: viewed })
            .map_err(|DecodeError| failed(ChainErrorKind::Decode))?;
        current = match outcome {
            None => return Ok(Chained::Dropped),
            Some(OwnedRecord::AsIs) => current,
            Some(OwnedRecord::Message { event, payload }) => {
                let span = match viewed {
                    Record::Message(m) => m.span,
                    Record::Text(t) => t.span,
                    _ => return Err(failed(ChainErrorKind::WrongOutcome)),
                };
                Chained::Owned(OwnedChained::Message {
                    event,
                    span,
                    payload,
                })
            }
            Some(OwnedRecord::SampleMetric(metric)) => match current {
                // Ремап поверх ремапа: значение так и заимствует исходный
                // буфер, меняется только идентификатор.
                Chained::Owned(OwnedChained::Sample { original, .. }) => {
                    Chained::Owned(OwnedChained::Sample { metric, original })
                }
                Chained::Same(Record::Sample(original)) => {
                    Chained::Owned(OwnedChained::Sample { metric, original })
                }
                _ => return Err(failed(ChainErrorKind::WrongOutcome)),
            },
        };
    }
    Ok(current)
}

// ════════════════════════════════════════════════════════════════════════════
// Физический прогон
// ════════════════════════════════════════════════════════════════════════════

/// Итог физического прогона миграции.
///
/// Сегменты: каждый из перечисленных при прогоне попал ровно в одну графу.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MigrationReport {
    /// Переписаны в текущую раскладку.
    pub rewritten: usize,
    /// Пропущены по footer'у: ни один шаг их не затрагивает. Их заголовок
    /// сохраняет прежнюю версию — это легально, текущие декодеры читают их
    /// верно именно потому, что затронутых типов в них нет.
    pub skipped_untouched: usize,
    /// Уже текущей (или более новой) версии.
    pub already_current: usize,
    /// Удалены целиком: шаги удалили все их записи.
    pub emptied: usize,
    /// Исчезли под прогоном: ротация успела раньше. Не потеря — их история
    /// была бы выброшена той же ротацией и без миграции.
    pub rotated_away: usize,
    /// Не разобрались (битый заголовок) либо принадлежат чужому хранилищу.
    /// Прогон их не трогает; читателю они и так известны как повреждение
    /// или чужак — штампу меты они не мешают, потому что никакая версия
    /// схемы их бы не прочла.
    pub unreadable: usize,
    /// Записей легло в переписанные сегменты.
    pub records_rewritten: u64,
    /// Записей удалено шагами.
    pub records_dropped: u64,
}

/// Прогнать все каналы неймспейса. Возвращает суммарный отчёт; штамп меты —
/// забота вызывающего (`Namespace::migrate`), у него и файл, и атомик.
pub(crate) fn run_namespace(
    schema: &Schema,
    store_id: u64,
    writer: &Writer,
    ns: NsId,
    channel_dirs: &[std::path::PathBuf],
) -> Result<MigrationReport> {
    let mut report = MigrationReport::default();
    for (idx, channel_dir) in channel_dirs.iter().enumerate() {
        run_channel(
            channel_dir,
            schema,
            store_id,
            writer,
            ns,
            ChannelIdx(idx as u16),
            &mut report,
        )?;
    }
    Ok(report)
}

fn run_channel(
    dir: &Path,
    schema: &Schema,
    store_id: u64,
    writer: &Writer,
    ns: NsId,
    channel: ChannelIdx,
    report: &mut MigrationReport,
) -> Result<()> {
    // След прежнего прерванного прогона: содержимое `*.tmp` никем не
    // адресуется, а занятое им имя помешало бы новой попытке.
    crate::fsutil::sweep_tmp(dir)?;

    for name in crate::rotation::Inventory::scan_names(dir)? {
        let path = dir.join(name.to_string());
        let reader = match SegmentReader::open(&path) {
            Ok(r) => r,
            // Файл исчез под ногами — ротация работает параллельно.
            Err(Error::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
                report.rotated_away += 1;
                continue;
            }
            // Битый заголовок не прочтёт никакая версия схемы: переписывать
            // нечего, и держать из-за него мету незаштампованной значило бы
            // сделать миграцию невозможной навсегда.
            Err(Error::Corrupt { .. }) => {
                report.unreadable += 1;
                continue;
            }
            Err(e) => return Err(e),
        };
        if reader.header().store_id != store_id {
            // Чужой дамп, подложенный в каталог: не наше — не трогаем.
            report.unreadable += 1;
            continue;
        }
        let seg_version = reader.header().protocol_version.0;
        if seg_version >= schema.version.0 {
            report.already_current += 1;
            continue;
        }
        let steps = chain(schema, seg_version).map_err(|e| Error::Corrupt {
            path: path.clone(),
            reason: e.to_string(),
        })?;
        if reader.is_sealed()
            && let Some(footer) = reader.footer()
            && !chain_touches(&steps, &footer)
        {
            report.skipped_untouched += 1;
            continue;
        }

        let rewrite = match rewrite_segment(&reader, &path, schema.version, &steps) {
            Ok(r) => r,
            Err(e) => {
                // Оригинал не тронут; недописанный tmp подметёт следующая
                // попытка. Прогон обрывается: продолжать после отказа значило
                // бы заштамповать мету поверх непереписанного сегмента.
                let _ = std::fs::remove_file(tmp_path(&path));
                return Err(e);
            }
        };
        let emptied = matches!(rewrite.commit, MigrationCommit::Remove);
        if writer.commit_migration(ns, channel, name, rewrite.commit)? {
            if emptied {
                report.emptied += 1;
            } else {
                report.rewritten += 1;
            }
            report.records_rewritten += rewrite.records_rewritten;
            report.records_dropped += rewrite.records_dropped;
        } else {
            // Ротация успела раньше: результат никому не нужен, и в отчёте
            // его тоже нет — эти записи выброшены, а не переписаны.
            let _ = std::fs::remove_file(tmp_path(&path));
            report.rotated_away += 1;
        }
    }
    Ok(())
}

fn tmp_path(path: &Path) -> std::path::PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".tmp");
    std::path::PathBuf::from(s)
}

/// Итог переписывания одного сегмента — до фиксации.
struct Rewrite {
    commit: MigrationCommit,
    records_rewritten: u64,
    records_dropped: u64,
}

/// Переписать сегмент во временный файл. Файл синхронизирован; фиксация —
/// за writer'ом.
///
/// Границы блоков сохраняются: блок на входе — блок на выходе (опустевший
/// выпадает). Имя и `base` тоже: даже если головные записи удалены, отбор
/// сегментов по имени от этого лишь консервативнее — см. SPEC.
fn rewrite_segment(
    reader: &SegmentReader,
    path: &Path,
    to: ProtocolVersion,
    steps: &[&Migration],
) -> Result<Rewrite> {
    let src = reader.header();
    let header = SegmentHeader {
        protocol_version: to,
        boot: src.boot,
        base: src.base,
        store_id: src.store_id,
    };
    // Ёмкость с запасом: добавленное шагом поле раздувает записи, а упереться
    // в потолок посреди переписывания значит бросить работу. Хвост обрежет
    // seal.
    let capacity = reader.len().saturating_mul(2).max(1 << 20);
    let tmp = tmp_path(path);
    let mut seg = SegmentWriter::create_at(&tmp, header, capacity)?;

    let offsets: Vec<u64> = match reader.footer() {
        Some(f) => f.blocks.iter().map(|b| b.offset).collect(),
        // Незапечатанный (или с битым footer'ом) сегмент обходится сканом;
        // повреждённый хвост при этом отбрасывается — ровно как при чтении.
        None => reader.scan_block_offsets().0,
    };

    let mut footer = FooterBuilder::new();
    let mut builder = BlockBuilder::new();
    let mut buf = Vec::new();
    let mut out = Vec::new();
    let mut rewritten = 0u64;
    let mut dropped = 0u64;

    for offset in offsets {
        match reader.read_block_at(offset, &mut buf) {
            Ok(Some(_)) => {}
            Ok(None) => continue,
            Err(e) => return Err(e),
        }
        let Some(block) = crate::segment::parse_block(&buf)? else {
            continue;
        };
        let compression = block.header.compression;
        let mut last = block.header.base;
        for item in block.records() {
            let (at, record) = item.map_err(|e| Error::Corrupt {
                path: path.to_owned(),
                reason: format!("запись сегмента не разобралась: {e}"),
            })?;
            let chained = apply(steps, record).map_err(|e| Error::Corrupt {
                path: path.to_owned(),
                reason: format!(
                    "{e}: запись не приводится к текущей версии — history и \
                     фактическая раскладка разошлись, оригинал не тронут"
                ),
            })?;
            let Some(rec) = chained.record() else {
                dropped += 1;
                continue;
            };
            match rec {
                Record::Message(m) => footer.add_event(m.event),
                Record::Sample(s) => footer.add_metric(s.metric),
                _ => {}
            }
            builder.push(at, &rec)?;
            last = at;
            rewritten += 1;
        }
        if builder.is_empty() {
            footer.discard_pending();
            continue;
        }
        out.clear();
        let h = builder.finish(seg.next_seq(), compression, &mut out)?;
        if !seg.fits(out.len() as u64) {
            return Err(Error::Corrupt {
                path: path.to_owned(),
                reason: "переписанный сегмент не влез в отведённую ёмкость".to_owned(),
            });
        }
        let at = seg.append_block(&out)?;
        footer.add_block(at, &h, last);
    }

    if footer.is_empty() {
        // Все записи удалены: сегмента больше нет.
        drop(seg);
        std::fs::remove_file(&tmp).ok();
        return Ok(Rewrite {
            commit: MigrationCommit::Remove,
            records_rewritten: rewritten,
            records_dropped: dropped,
        });
    }
    let size = seg.data_end() + {
        let bytes = footer.build();
        let len = bytes.len() as u64;
        seg.seal(&bytes)?;
        len
    };
    Ok(Rewrite {
        commit: MigrationCommit::Replace { tmp, size },
        records_rewritten: rewritten,
        records_dropped: dropped,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dduroc_format::record::Text;
    use dduroc_format::{Level, Value};
    use std::result::Result;

    fn msg(event: u16, payload: &[u8]) -> Record<'_> {
        Record::Message(Message {
            event: EventId(event),
            span: Some(SpanId(7)),
            payload,
        })
    }

    fn sample(metric: u16, v: u64) -> Record<'static> {
        Record::Sample(Sample {
            metric: MetricId(metric),
            value: Value::U64(v),
        })
    }

    fn step(
        from: u16,
        events: &'static [EventId],
        metrics: &'static [MetricId],
        migrate: fn(MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError>,
    ) -> Migration {
        Migration {
            from,
            touches_all: events.is_empty() && metrics.is_empty(),
            events,
            metrics,
            migrate,
        }
    }

    #[test]
    fn declared_sets_are_binding_not_advisory() {
        // Шаг объявил события [1] — записей других типов он не видит вовсе.
        // Иначе «объявил одно, преобразует другое» жило бы молча: сегменты
        // отбирались бы по объявленному, а переписывалось бы фактическое.
        fn poison(_: MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError> {
            Ok(Some(OwnedRecord::Message {
                event: EventId(0xEE),
                payload: vec![0xEE],
            }))
        }
        let s = step(1, &[EventId(1)], &[], poison);
        let steps = [&s];

        // Объявленное — преобразуется.
        match apply(&steps, msg(1, &[1])).unwrap() {
            Chained::Owned(OwnedChained::Message { event, span, .. }) => {
                assert_eq!(event, EventId(0xEE));
                assert_eq!(span, Some(SpanId(7)), "привязка к спану пережила замену");
            }
            other => panic!("ожидалась замена, получено {other:?}"),
        }

        // Необъявленное — минует шаг с исходными байтами.
        for rec in [
            msg(2, &[2]),
            sample(9, 42),
            Record::Text(Text {
                level: Level::Info,
                span: None,
                target: "t",
                text: "x",
            }),
        ] {
            match apply(&steps, rec).unwrap() {
                Chained::Same(r) => assert_eq!(r, rec, "байты обязаны быть исходными"),
                other => panic!("{rec:?} не должен был меняться: {other:?}"),
            }
        }
    }

    #[test]
    fn touches_all_sees_even_what_sets_cannot_name() {
        // Текст и спаны не выразить множествами типов — но шаг с touches_all
        // обязан их видеть: он единственный способ, например, вычистить
        // свободный текст с чувствительными данными.
        fn drop_text(r: MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError> {
            Ok(match r.record {
                Record::Text(_) => None,
                _ => Some(OwnedRecord::AsIs),
            })
        }
        let s = step(1, &[], &[], drop_text);
        let steps = [&s];

        let text = Record::Text(Text {
            level: Level::Info,
            span: None,
            target: "t",
            text: "секрет",
        });
        assert!(matches!(apply(&steps, text).unwrap(), Chained::Dropped));
        assert!(matches!(
            apply(&steps, msg(1, &[1])).unwrap(),
            Chained::Same(_)
        ));
    }

    #[test]
    fn steps_compose_in_order_and_each_sees_the_previous_layout() {
        // Прыжок v1 → v3: шаг 1 перекодирует payload и меняет тип, шаг 2
        // обязан увидеть уже НОВЫЙ тип — так один и тот же шаг работает и в
        // одиночку, и в цепочке.
        fn one(r: MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError> {
            let old: (u8,) = r.decode()?;
            Ok(Some(OwnedRecord::Message {
                event: EventId(2),
                payload: postcard::to_allocvec(&(u16::from(old.0) * 2,)).unwrap(),
            }))
        }
        fn two(r: MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError> {
            let mid: (u16,) = r.decode()?;
            Ok(Some(OwnedRecord::Message {
                event: EventId(3),
                payload: postcard::to_allocvec(&(u32::from(mid.0) + 1,)).unwrap(),
            }))
        }
        let s1 = step(1, &[EventId(1)], &[], one);
        let s2 = step(2, &[EventId(2)], &[], two);
        let steps = [&s1, &s2];

        let payload = postcard::to_allocvec(&(21u8,)).unwrap();
        let out = apply(&steps, msg(1, &payload)).unwrap();
        match out {
            Chained::Owned(OwnedChained::Message { event, payload, .. }) => {
                assert_eq!(event, EventId(3));
                let v: (u32,) = postcard::from_bytes(&payload).unwrap();
                assert_eq!(v.0, 43, "21 * 2 + 1: шаги сложились по порядку");
            }
            other => panic!("{other:?}"),
        }

        // Запись, появившаяся уже в раскладке v2, проходит только шаг 2.
        let payload = postcard::to_allocvec(&(5u16,)).unwrap();
        let out = apply(&steps[1..], msg(2, &payload)).unwrap();
        match out {
            Chained::Owned(OwnedChained::Message { event, .. }) => assert_eq!(event, EventId(3)),
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn metric_remap_keeps_the_value_bytes() {
        fn remap(_: MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError> {
            Ok(Some(OwnedRecord::SampleMetric(MetricId(0x20))))
        }
        let s = step(1, &[], &[MetricId(0x10)], remap);
        let steps = [&s];

        match apply(&steps, sample(0x10, 777)).unwrap() {
            Chained::Owned(o @ OwnedChained::Sample { metric, .. }) => {
                assert_eq!(metric, MetricId(0x20));
                match o.as_record() {
                    Record::Sample(s) => assert_eq!(s.value, Value::U64(777)),
                    other => panic!("{other:?}"),
                }
            }
            other => panic!("{other:?}"),
        }
        // Чужая метрика не тронута.
        assert!(matches!(
            apply(&steps, sample(0x11, 1)).unwrap(),
            Chained::Same(_)
        ));
    }

    #[test]
    fn a_failing_step_names_itself() {
        fn broken(r: MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError> {
            let _: (u64, u64, u64, u64) = r.decode()?; // payload заведомо короче
            Ok(Some(OwnedRecord::AsIs))
        }
        let s1 = step(1, &[EventId(1)], &[], |_| Ok(Some(OwnedRecord::AsIs)));
        let s2 = step(2, &[EventId(1)], &[], broken);
        let steps = [&s1, &s2];

        let err = apply(&steps, msg(1, &[1])).unwrap_err();
        assert_eq!(err.step_from, 2, "виновник назван, а не «где-то в цепочке»");
        assert_eq!(err.kind, ChainErrorKind::Decode);
    }

    #[test]
    fn an_outcome_that_does_not_fit_the_record_is_a_step_bug() {
        // SampleMetric для сообщения — ошибка кода шага. Молча пропустить её
        // значило бы оставить запись в старой раскладке и объявить успех.
        fn wrong(_: MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError> {
            Ok(Some(OwnedRecord::SampleMetric(MetricId(1))))
        }
        let s = step(1, &[EventId(1)], &[], wrong);
        let err = apply(&[&s], msg(1, &[1])).unwrap_err();
        assert_eq!(err.kind, ChainErrorKind::WrongOutcome);
    }

    #[test]
    fn chain_is_resolved_or_refused_never_guessed() {
        use crate::schema::tests_support::minimal_schema_with_migrations;
        fn asis(_: MigratedRecord<'_>) -> Result<Option<OwnedRecord>, DecodeError> {
            Ok(Some(OwnedRecord::AsIs))
        }
        static STEPS: &[Migration] = &[
            Migration {
                from: 1,
                touches_all: true,
                events: &[],
                metrics: &[],
                migrate: asis,
            },
            Migration {
                from: 2,
                touches_all: true,
                events: &[],
                metrics: &[],
                migrate: asis,
            },
        ];
        let schema = minimal_schema_with_migrations(3, STEPS);

        assert_eq!(chain(&schema, 3).unwrap().len(), 0, "текущая версия");
        assert_eq!(chain(&schema, 2).unwrap().len(), 1);
        let full = chain(&schema, 1).unwrap();
        assert_eq!(full.len(), 2);
        assert_eq!(full[0].from, 1);
        assert_eq!(full[1].from, 2);

        // Дыра в цепочке — отказ, а не молчаливый пропуск шага.
        static GAPPY: &[Migration] = &[Migration {
            from: 2,
            touches_all: true,
            events: &[],
            metrics: &[],
            migrate: asis,
        }];
        let schema = minimal_schema_with_migrations(3, GAPPY);
        let err = chain(&schema, 1).unwrap_err();
        assert_eq!(err.step_from, 1);
        assert_eq!(err.kind, ChainErrorKind::MissingStep);
    }
}
