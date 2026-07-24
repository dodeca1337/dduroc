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

    #[error("метрика {id} не объявлена в схеме")]
    UnknownMetric { id: u16 },

    #[error("недопустимые пределы метрики {metric:?}: {reason}")]
    BadLimits {
        metric: &'static str,
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
