//! Байтовый формат хранения dduroc: сегменты, блоки, записи.
//!
//! Крейт содержит **только кодеки и валидацию** — никакого I/O, никакого
//! состояния, ничего асинхронного. Всё, что здесь есть, — чистые функции над
//! срезами байт, поэтому слой полностью покрывается unit- и property-тестами,
//! а движок ([`dduroc-engine`]) отвечает отдельно за файлы, потоки и политики.
//!
//! # Структура
//!
//! ```text
//! <root>/<namespace>/<channel>/<boot:08x>-<micros:016x>.seg
//!   [SegmentHeader 32B] [Block]* [Footer]?
//!        Block = [BlockHeader 32B] [тело: записи, опц. сжатое]
//! ```
//!
//! Неймспейс и канал в байтах не кодируются — они имплицитны из пути файла.
//!
//! # Принципы
//!
//! - **На диск только динамика.** Уровни, шаблоны текста, тэги сообщений,
//!   имена типов живут в схеме бинарника и резолвятся при чтении.
//! - **Дельты времени.** Записи внутри блока хранят varint-дельту от
//!   предыдущей записи: 1–3 байта против 10-байтового ключа в LSM-прототипе.
//! - **Целостность на блок.** CRC32C и сжатие амортизируются на блок, а не
//!   на запись.
//! - **Обрыв питания — норма.** Незавершённый хвост активного сегмента
//!   отличим от порчи: см. [`Error::is_torn_tail`].
//! - **armv7.** Размеры и смещения файлов — всегда `u64`, никогда `usize`;
//!   mmap не используется.
//!
//! [`dduroc-engine`]: https://docs.rs/dduroc-engine

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod block;
mod cursor;
mod error;
pub mod footer;
mod ids;
mod level;
pub mod record;
pub mod segment;
mod value;
pub mod varint;

pub use block::{Block, BlockBuilder, BlockHeader, Compression, restamp_seq};
pub use error::{Error, Result};
pub use footer::{Footer, FooterBuilder, Trailer};
pub use ids::{
    BootCounter, BootTime, EventId, MetricId, Micros, ProtocolVersion, SpanId, SpanKindId,
};
pub use level::Level;
pub use record::{Framed, Message, Record, Sample, SpanStart, Text};
pub use segment::{SegmentHeader, SegmentName};
pub use value::{Value, ValueType};

/// Версия контейнера — байтового формата как такового. Не путать с версией
/// протокола схемы ([`ProtocolVersion`]), которая принадлежит приложению и
/// меняется его миграциями.
///
/// | версия | что изменилось |
/// |---|---|
/// | 1 | первая раскладка: сэмпл ссылался на сегментно-локальный номер серии, отдельная запись `SeriesDef` связывала его с метрикой и рантайм-тэгами, таблица серий дублировалась в footer'е |
/// | 2 | рантайм-тэгов нет: идентичность ряда равна метрике, сэмпл несёт `metric_id`, запись `SeriesDef` и таблица серий удалены |
///
/// Читатель принимает **только** текущую версию: несовпадение — внятная
/// ошибка [`Error::UnsupportedContainerVersion`], а не попытка угадать
/// раскладку. Файл прежней версии остаётся читаемым прежним билдом.
pub const CONTAINER_VERSION: u8 = 2;
