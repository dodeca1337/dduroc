//! Ошибки чтения.

use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum ReadError {
    #[error(transparent)]
    Engine(#[from] dduroc_engine::Error),

    #[error("формат: {0}")]
    Format(#[from] dduroc_format::Error),

    #[error(
        "сегмент {path} принадлежит другому хранилищу (ожидалось {expected:#018x}, \
         в файле {found:#018x})"
    )]
    ForeignStore {
        path: PathBuf,
        expected: u64,
        found: u64,
    },

    #[error(
        "дамп неполон: у неймспейса {namespace:?} нет дерева класса {class:?} — \
         хранилище писало этот класс в свой корень, назовите ВСЕ корни дампа"
    )]
    IncompleteDump {
        namespace: String,
        class: &'static str,
    },

    #[error("недопустимый шаблон выбора неймспейсов {0:?}")]
    BadPattern(String),

    #[error("недопустимый курсор пагинации")]
    BadCursor,

    #[error("io: {context}: {source}")]
    Io {
        context: String,
        #[source]
        source: std::io::Error,
    },
}

pub type Result<T> = std::result::Result<T, ReadError>;
