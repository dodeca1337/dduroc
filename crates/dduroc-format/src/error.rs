//! Ошибки кодирования/декодирования формата.
//!
//! Разделение важно для recovery: [`Error::Truncated`] и [`Error::CrcMismatch`]
//! на **последнем** блоке сегмента — норма (обрыв питания посреди записи),
//! читатель просто обрезает хвост. Остальные варианты означают повреждение
//! или чужой формат.

/// Ошибки формата.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("данные оборваны: ожидалось больше байт")]
    Truncated,

    #[error("varint не влезает в u64 либо не канонический")]
    VarintOverflow,

    #[error("несовпадение CRC32C: в заголовке {expected:#010x}, посчитано {actual:#010x}")]
    CrcMismatch { expected: u32, actual: u32 },

    #[error("неверный magic: ожидался {expected:?}, получен {actual:?}")]
    BadMagic { expected: [u8; 4], actual: [u8; 4] },

    #[error("версия контейнера {0} не поддерживается (ожидалась {1})")]
    UnsupportedContainerVersion(u8, u8),

    #[error("неизвестный тип записи {0:#x}")]
    UnknownRecordKind(u8),

    #[error(
        "запись типа {0:#x} принадлежит контейнеру версии 1: тело блока записано \
         прежней раскладкой, где отсчёт ссылался на локальный номер серии"
    )]
    RetiredRecordKind(u8),

    #[error("неизвестный тип значения серии {0:#x}")]
    UnknownValueType(u8),

    #[error("неверный уровень {0}")]
    BadLevel(u8),

    #[error("строка не является корректным UTF-8")]
    BadUtf8,

    #[error("превышен лимит: {what} = {value}, максимум {max}")]
    LimitExceeded {
        what: &'static str,
        value: u64,
        max: u64,
    },

    #[error("неизвестный алгоритм сжатия {0:#x}")]
    UnknownCompression(u8),

    #[error("ошибка распаковки блока: {0}")]
    Decompress(&'static str),

    #[error("попытка завершить пустой блок")]
    EmptyBlock,

    /// Нарушено соглашение о зарезервированном значении: резервные биты не
    /// нулевые, либо использовано значение, которое формат резервирует
    /// (нулевой `span_id`, нулевая дельта в строго возрастающем множестве).
    #[error("нарушено соглашение о зарезервированном значении")]
    ReservedValue,
}

pub type Result<T> = core::result::Result<T, Error>;

impl Error {
    /// `true` для ошибок, ожидаемых на последнем блоке после обрыва питания.
    /// Recovery по такой ошибке обрезает хвост, а не объявляет файл битым.
    pub fn is_torn_tail(&self) -> bool {
        matches!(
            self,
            Error::Truncated | Error::CrcMismatch { .. } | Error::VarintOverflow
        )
    }
}
