//! Ошибки движка.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },

    #[error("формат: {0}")]
    Format(#[from] dduroc_format::Error),

    #[error("сериализация метаданных: {0}")]
    Postcard(#[from] postcard::Error),

    #[error("недопустимое имя неймспейса {name:?}: {reason}")]
    BadNamespace { name: String, reason: &'static str },

    #[error("схема {name:?} не проходит проверку: {reason}")]
    BadSchema { name: String, reason: String },

    #[error(
        "событие {event} не объявлено в схеме {schema:?}: id из чужой схемы означал бы \
         чужой декодер при чтении"
    )]
    UnknownEvent { schema: &'static str, event: u16 },

    #[error("вид спана {kind} не объявлен в схеме {schema:?}")]
    UnknownSpanKind { schema: &'static str, kind: u16 },

    #[error("поля события {event:?} не сериализуются")]
    EncodeFailed { event: &'static str },

    #[error(
        "класс хранения {class:?} не объявлен ни одним типом схемы {schema:?}: \
         канала под него нет"
    )]
    ClassNotDeclared {
        schema: &'static str,
        class: &'static str,
    },

    #[error("недопустимое имя канала {name:?}: {reason}")]
    BadChannel { name: String, reason: &'static str },

    #[error("неймспейс {0:?} уже открыт в этом процессе")]
    NamespaceBusy(String),

    #[error(
        "неймспейс {namespace:?} записан схемой {stored:?}, открывается схемой {opening:?} — \
         разные схемы в одном неймспейсе смешали бы несовместимые id событий"
    )]
    SchemaMismatch {
        namespace: String,
        stored: String,
        opening: String,
    },

    #[error(
        "неймспейс {namespace:?} имеет версию протокола {stored}, схема билда — {current}: \
         данные из будущего, эта прошивка их не поймёт"
    )]
    ProtocolFromFuture {
        namespace: String,
        stored: u16,
        current: u16,
    },

    #[error("нет шага миграции {from} → {to} для схемы {schema:?}")]
    MissingMigration { schema: String, from: u16, to: u16 },

    /// Второй одновременный `Namespace::migrate` на одном неймспейсе.
    ///
    /// Отказ, а не ожидание: прогон занимает минуты, и молча повисший второй
    /// вызов выглядел бы как зависание приложения. Повторить его имеет смысл
    /// только после завершения первого — когда, скорее всего, уже нечего.
    #[error("миграция неймспейса {0:?} уже идёт")]
    MigrationBusy(String),

    #[error("метрика {metric_id} не объявлена в схеме")]
    UnknownMetric { metric_id: u16 },

    #[error(
        "метрика {metric_id} объявлена как {declared:?}, а значение — {got:?}: \
         тип отсчёта — свойство метрики, а не отдельной записи"
    )]
    ValueTypeMismatch {
        metric_id: u16,
        declared: dduroc_format::ValueType,
        got: dduroc_format::ValueType,
    },

    #[error("недопустимые пределы метрики {metric_name:?}: {reason}")]
    BadLimits {
        metric_name: &'static str,
        reason: &'static str,
    },

    #[error("повреждён {path}: {reason}")]
    Corrupt { path: PathBuf, reason: String },

    #[error(
        "хранилище {0} уже открыто: два писателя на одном каталоге выдавали бы \
         одинаковые номера запусков и сталкивались бы именами сегментов"
    )]
    StoreBusy(PathBuf),

    #[error("хранилище закрывается")]
    ShuttingDown,

    #[error("очередь записи переполнена: диск не успевает")]
    QueueFull,

    #[error("writer-поток умер: запись невозможна")]
    WriterDead,

    /// Синхронизация не догнала очередь.
    ///
    /// Прикладные потоки ставят записи в очередь быстрее, чем writer успевает
    /// их разбирать, и `sync` прекратил попытки, отработав отведённое число
    /// проходов: иначе он не вернулся бы, пока пишущие не замолчат.
    ///
    /// Записи **не потеряны** — они по-прежнему в очереди и лягут на носитель
    /// обычным ходом; на носителе нет только их. Поэтому это и не потеря
    /// ([`Error::loses_record`] — `false`), и не дефект вызова: это отчёт о
    /// том, что обещание «всё накопленное на носителе» в этот раз выполнено
    /// не до конца.
    #[error("синхронизация не догнала очередь: часть записей ещё не на носителе")]
    SyncIncomplete,

    #[error("нет места на устройстве: {0}")]
    NoSpace(PathBuf),

    #[error(
        "сегмент {path} создан другим хранилищем (ожидалось {expected:#018x}, \
         в файле {found:#018x}): у него своя нумерация запусков и своя привязка \
         ко времени"
    )]
    ForeignSegment {
        path: PathBuf,
        expected: u64,
        found: u64,
    },
}

pub type Result<T> = std::result::Result<T, Error>;

/// Добавляет к io-ошибке путь и операцию: без этого «No such file or directory»
/// в логе не позволяет понять, какой из тысяч файлов не открылся.
pub(crate) trait IoContext<T> {
    fn ctx(self, what: &str) -> Result<T>;
    fn ctx_path(self, what: &str, path: &std::path::Path) -> Result<T>;
}

impl<T> IoContext<T> for std::io::Result<T> {
    fn ctx(self, what: &str) -> Result<T> {
        self.map_err(|source| Error::Io {
            context: what.to_owned(),
            source,
        })
    }

    fn ctx_path(self, what: &str, path: &std::path::Path) -> Result<T> {
        self.map_err(|source| Error::Io {
            context: format!("{what} {}", path.display()),
            source,
        })
    }
}

impl<T> IoContext<T> for std::result::Result<T, rustix::io::Errno> {
    fn ctx(self, what: &str) -> Result<T> {
        self.map_err(|e| Error::Io {
            context: what.to_owned(),
            source: e.into(),
        })
    }

    fn ctx_path(self, what: &str, path: &std::path::Path) -> Result<T> {
        self.map_err(|e| Error::Io {
            context: format!("{what} {}", path.display()),
            source: e.into(),
        })
    }
}

impl Error {
    /// Означает ли ошибка, что **запись потеряна**.
    ///
    /// Главный вопрос вызывающего на пути записи, и он не должен требовать
    /// разбора всего перечисления: потеря — это состояние носителя (очередь не
    /// успевает, writer мёртв, хранилище останавливается), а не ошибка вызова.
    /// Нарушение контракта (id из чужой схемы) — наоборот, дефект кода, и
    /// реагировать на него надо иначе: не повтором, а исправлением.
    pub fn loses_record(&self) -> bool {
        matches!(
            self,
            Error::QueueFull | Error::WriterDead | Error::ShuttingDown
        )
    }

    /// Ошибка вызова: объявленное в схеме и переданное не совпали.
    ///
    /// Такое не лечится повтором и не зависит от нагрузки — это дефект сборки
    /// (обычно тип из одной схемы передан в неймспейс другой).
    pub fn breaks_contract(&self) -> bool {
        matches!(
            self,
            Error::UnknownEvent { .. }
                | Error::UnknownMetric { .. }
                | Error::UnknownSpanKind { .. }
                | Error::ValueTypeMismatch { .. }
                | Error::ClassNotDeclared { .. }
                | Error::EncodeFailed { .. }
        )
    }

    /// Закончилось место на устройстве — для этого случая политика особая:
    /// движок обязан продолжать ротацию и не терять критические данные молча.
    pub fn is_no_space(&self) -> bool {
        match self {
            Error::NoSpace(_) => true,
            Error::Io { source, .. } => source.raw_os_error() == Some(libc_enospc()),
            _ => false,
        }
    }
}

const fn libc_enospc() -> i32 {
    // ENOSPC одинаков на всех Linux-ABI (включая armv7).
    28
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lost_records_are_distinguishable_from_caller_bugs() {
        // Ради этого разделения предикаты и существуют: на потерю прикладной
        // код реагирует счётчиком и деградацией, на нарушение контракта —
        // исправлением сборки. Перепутать их значит либо молча терпеть баг,
        // либо поднимать тревогу из-за занятого диска.
        for lost in [Error::QueueFull, Error::WriterDead, Error::ShuttingDown] {
            assert!(lost.loses_record(), "{lost}");
            assert!(!lost.breaks_contract(), "{lost}");
        }

        let bugs = [
            Error::UnknownEvent {
                schema: "radio",
                event: 7,
            },
            Error::UnknownMetric { metric_id: 7 },
            Error::UnknownSpanKind {
                schema: "radio",
                kind: 7,
            },
            Error::ValueTypeMismatch {
                metric_id: 1,
                declared: dduroc_format::ValueType::F32,
                got: dduroc_format::ValueType::U64,
            },
            Error::ClassNotDeclared {
                schema: "radio",
                class: "critical",
            },
            Error::EncodeFailed { event: "PowerSet" },
        ];
        for bug in bugs {
            assert!(bug.breaks_contract(), "{bug}");
            assert!(!bug.loses_record(), "{bug}");
        }

        // Отказ носителя — ни то, ни другое: запись могла и дойти.
        let io = Error::NoSpace(PathBuf::from("/data"));
        assert!(!io.loses_record());
        assert!(!io.breaks_contract());
    }
}
