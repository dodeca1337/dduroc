//! Чтение хранилища dduroc.
//!
//! Слой отвечает на вопрос «что произошло»: отбирает неймспейсы и каналы,
//! сливает их потоки по времени, восстанавливает то, чего нет на диске
//! (уровни, имена, шаблоны, UTC), и честно сообщает о повреждённых
//! фрагментах вместо того, чтобы выдать неполный ответ за полный.
//!
//! Работает и на устройстве, и в офлайн-вьюере: там и там нужны каталог
//! хранилища и схемы приложения, потому что расшифровать записи без схемы
//! нельзя — на диске лежат только идентификаторы и бинарные поля.

#![forbid(unsafe_code)]
#![warn(missing_debug_implementations)]

pub mod cursor;
mod error;
pub mod query;
mod reader;

pub use cursor::{Damage, OwnedRecord, OwnedSampleValue, RawEntry};
pub use error::{ReadError, Result};
pub use query::{Filter, KindFilter, NsSelect, Order, Query};
pub use reader::{Entry, EntryKind, NamespaceInfo, QueryResult, Reader};
