//! Эпохи: связь относительного времени с абсолютным.
//!
//! Два уровня идентичности:
//!
//! - **run** (`boot_counter`) — один запуск процесса. Растёт при каждом старте.
//! - **hardware boot** (`hw_boot_id`) — одна загрузка железа. Определяется по
//!   `/proc/sys/kernel/random/boot_id`: ядро генерирует этот UUID при загрузке,
//!   и он не зависит от того, в какой момент стартовало ПО. (Прототип различал
//!   загрузки по возрастанию `CLOCK_BOOTTIME`, что ошибалось при быстром
//!   рестарте после перезагрузки.)
//!
//! **UTC-якорь хранится на hardware boot** — это UTC-время, соответствующее
//! `CLOCK_BOOTTIME == 0`. Одна синхронизация даёт абсолютное время всем
//! событиям этой загрузки, включая записанные **до** синхронизации: конверсия
//! выполняется при чтении.
//!
//! Якорь **обновляемый, с приоритетом источника**: `User < Ntp < Gps`.
//! Сначала оператор мог ввести время руками, потом пришёл GPS — якорь
//! уточняется. Обратно (ручное поверх GPS) — нет.

use crate::clock::boottime_us;
use crate::error::{Error, IoContext, Result};
use crate::fsutil;
use chrono::{DateTime, Utc};
use dduroc_format::{BootTime, Micros};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Имя файла эпох в корне хранилища.
pub const EPOCHS_FILE: &str = "epochs.bin";

/// Источник синхронизации времени. Порядок — приоритет: более достоверный
/// источник перезаписывает менее достоверный, но не наоборот.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[repr(u8)]
pub enum SyncSource {
    /// Введено оператором.
    User = 1,
    Ntp = 2,
    Gps = 3,
}

impl SyncSource {
    pub const fn as_str(self) -> &'static str {
        match self {
            SyncSource::User => "user",
            SyncSource::Ntp => "ntp",
            SyncSource::Gps => "gps",
        }
    }
}

/// Один запуск процесса.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Run {
    pub boot_counter: u32,
    pub hw_boot_id: u32,
    /// `CLOCK_BOOTTIME` в момент регистрации run'а. Та же величина служит
    /// базой [`crate::clock::Clock`], поэтому
    /// `boottime_at_init_us + micros_события` — точное BOOTTIME события.
    pub boottime_at_init_us: u64,
}

/// Одна загрузка железа.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct HwBoot {
    pub hw_boot_id: u32,
    /// UUID из `/proc/sys/kernel/random/boot_id`.
    pub kernel_boot_id: [u8; 16],
    /// UTC (мс), соответствующее `CLOCK_BOOTTIME == 0`. `None` — не было
    /// синхронизации: события этой загрузки имеют только относительное время.
    ///
    /// На диске — миллисекунды целым, а не разобранная дата: восемь байт
    /// против двенадцати у сериализованного `DateTime`, и никакой зависимости
    /// формата файла от представления даты в чужом крейте. Наружу отдаётся
    /// нормальный тип — [`HwBoot::utc_anchor`].
    pub utc_anchor_ms: Option<i64>,
    pub anchor_source: Option<SyncSource>,
    /// `CLOCK_BOOTTIME` в момент фиксации якоря.
    pub anchor_captured_us: Option<u64>,
}

impl HwBoot {
    /// Якорь как момент времени. `None` — синхронизации не было.
    pub fn utc_anchor(&self) -> Option<DateTime<Utc>> {
        DateTime::from_timestamp_millis(self.utc_anchor_ms?)
    }
}

/// Куда настенный момент попадает в относительной шкале запуска.
///
/// Трёхзначность здесь по делу: «раньше старта» и «якоря нет» — разные вещи.
/// Первое — обычное дело для нижней границы окна (весь запуск лежит внутри),
/// второе означает, что сравнивать нечем, и запуск выпадает из выборки, о чём
/// придётся сказать вызывающему.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunOffset {
    /// Момент раньше старта запуска — в его шкале не выражается.
    BeforeStart,
    /// Микросекунды от старта запуска.
    At(Micros),
    /// Запуск неизвестен или его загрузка не синхронизирована.
    Unanchored,
}

/// Содержимое `epochs.bin`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Epochs {
    pub runs: Vec<Run>,
    pub hw_boots: Vec<HwBoot>,
}

impl Epochs {
    /// Запуск по номеру.
    ///
    /// Поиск двоичный: `to_utc` вызывается на **каждую** запись ответа, а
    /// запусков в файле накапливается тем больше, чем дольше живёт прибор
    /// (двадцать перезапусков в сутки за пять лет — тридцать шесть тысяч).
    /// Линейный обход означал бы тридцать шесть тысяч сравнений на строчку
    /// журнала. Записи добавляются с возрастающим `boot_counter`, а уборка
    /// порядок сохраняет; на файле, изменённом руками, есть честный откат.
    pub fn run(&self, boot_counter: u32) -> Option<&Run> {
        match self
            .runs
            .binary_search_by_key(&boot_counter, |r| r.boot_counter)
        {
            Ok(i) => Some(&self.runs[i]),
            Err(_) => self.runs.iter().find(|r| r.boot_counter == boot_counter),
        }
    }

    pub fn hw_boot(&self, id: u32) -> Option<&HwBoot> {
        match self.hw_boots.binary_search_by_key(&id, |b| b.hw_boot_id) {
            Ok(i) => Some(&self.hw_boots[i]),
            Err(_) => self.hw_boots.iter().find(|b| b.hw_boot_id == id),
        }
    }

    /// Перевести относительное время в настенное. `None` — для hardware
    /// boot'а этого run'а нет якоря, и абсолютного времени у записи нет.
    ///
    /// Якорь хранится с точностью до миллисекунды, но смещение внутри run'а
    /// прибавляется микросекундами: округлять относительное время до
    /// миллисекунд незачем — его-то как раз измерили точно.
    pub fn to_utc(&self, at: BootTime) -> Option<DateTime<Utc>> {
        let run = self.run(at.boot.0)?;
        let hw = self.hw_boot(run.hw_boot_id)?;
        let anchor_ms = hw.utc_anchor_ms?;
        let event_boottime_us = run.boottime_at_init_us.checked_add(at.at.0)?;
        let total_us = i128::from(anchor_ms) * 1_000 + i128::from(event_boottime_us);
        DateTime::from_timestamp_micros(i64::try_from(total_us).ok()?)
    }

    /// Обратный перевод: где настенный момент лежит в шкале данного запуска.
    ///
    /// Нужен запросу с границами по настенным часам: сравнивать записи с ним
    /// напрямую нельзя — у каждого запуска своя шкала и свой якорь.
    pub fn from_utc(&self, boot_counter: u32, utc: DateTime<Utc>) -> RunOffset {
        let Some(run) = self.run(boot_counter) else {
            return RunOffset::Unanchored;
        };
        let Some(anchor_ms) = self.hw_boot(run.hw_boot_id).and_then(|hw| hw.utc_anchor_ms) else {
            return RunOffset::Unanchored;
        };
        let boottime_us = i128::from(utc.timestamp_micros()) - i128::from(anchor_ms) * 1_000;
        let from_start = boottime_us - i128::from(run.boottime_at_init_us);
        if from_start < 0 {
            return RunOffset::BeforeStart;
        }
        // Насыщение вместо ошибки: 2^64 µs — это 584 тысячи лет, столько
        // относительное время не набирает, а паниковать на арифметике границы
        // запроса тем более не за что.
        RunOffset::At(Micros(u64::try_from(from_start).unwrap_or(u64::MAX)))
    }

    /// Есть ли у запуска якорь: можно ли его записи сопоставить с настенными
    /// часами вообще.
    pub fn is_anchored(&self, boot_counter: u32) -> bool {
        self.run(boot_counter)
            .and_then(|r| self.hw_boot(r.hw_boot_id))
            .is_some_and(|hw| hw.utc_anchor_ms.is_some())
    }

    /// Запуски в хронологическом порядке регистрации.
    pub fn runs(&self) -> &[Run] {
        &self.runs
    }

    /// Обновить якорь загрузки. Возвращает `true`, если якорь принят.
    ///
    /// Правило: источник не ниже текущего. Равный приоритет допускается —
    /// свежий GPS уточняет старый GPS (дрейф часов между синхронизациями
    /// реален, отбрасывать уточнение нельзя).
    pub fn set_anchor(
        &mut self,
        hw_boot_id: u32,
        utc: DateTime<Utc>,
        source: SyncSource,
        now_boottime_us: u64,
    ) -> bool {
        let Some(hw) = self
            .hw_boots
            .iter_mut()
            .find(|b| b.hw_boot_id == hw_boot_id)
        else {
            return false;
        };
        if let Some(current) = hw.anchor_source
            && source < current
        {
            return false;
        }
        // Якорь — это UTC момента, когда BOOTTIME был нулём.
        hw.utc_anchor_ms = Some(
            utc.timestamp_millis()
                .saturating_sub((now_boottime_us / 1_000) as i64),
        );
        hw.anchor_source = Some(source);
        hw.anchor_captured_us = Some(now_boottime_us);
        true
    }

    /// Забыть run'ы, которых нет среди `alive`, и осиротевшие загрузки.
    ///
    /// Без этого файл рос бы вечно: 20 перезапусков в сутки за пять лет —
    /// 36 тысяч записей, которые читаются и переписываются целиком при
    /// каждом старте.
    pub fn retain_runs(&mut self, alive: &dyn Fn(u32) -> bool) {
        self.runs.retain(|r| alive(r.boot_counter));
        let used: std::collections::BTreeSet<u32> =
            self.runs.iter().map(|r| r.hw_boot_id).collect();
        self.hw_boots.retain(|b| used.contains(&b.hw_boot_id));
    }
}

/// Исход чтения файла эпох.
enum Loaded {
    Missing,
    Corrupt,
    Ok(Epochs),
}

/// Файл эпох: чтение, регистрация run'а, обновление якоря.
#[derive(Debug)]
pub struct EpochStore {
    path: PathBuf,
    epochs: Epochs,
    current: Run,
}

impl EpochStore {
    /// Открыть файл и зарегистрировать новый run.
    ///
    /// `boottime_at_init_us` передаётся снаружи, чтобы совпасть с базой часов
    /// ровно до микросекунды.
    ///
    /// `floor_boot` — наибольший номер запуска, написанный на именах уже
    /// лежащих сегментов. Спрашивается **только** если файл эпох не пережил
    /// прошлый запуск (потерян или уведён в карантин после порчи): у целого
    /// файла максимум по `runs` и так покрывает всё, что есть на диске, а
    /// обход каталогов при тысячах неймспейсов стоит дороже, чем экономит, —
    /// ровно поэтому уборка эпох и делается по порогу, а не на каждом старте.
    ///
    /// Без этой границы потеря `epochs.bin` начинала бы нумерацию заново,
    /// поверх запусков, чьи сегменты никуда не делись. Имя сегмента — это
    /// `(boot, µs)`, и его лексикографический порядок объявлен временным:
    /// повторно выданный номер поставил бы новые сегменты в историю **перед**
    /// старыми. Ротация, удаляющая старейшее, принялась бы за свежие записи, а
    /// читатель отдал бы историю вперемешку.
    pub fn open_and_register(
        root: &Path,
        boottime_at_init_us: u64,
        floor_boot: &dyn Fn() -> Result<Option<u32>>,
    ) -> Result<Self> {
        let path = root.join(EPOCHS_FILE);
        let (mut epochs, lost) = Self::load_for_write(&path)?;
        let floor_boot = if lost { floor_boot()? } else { None };
        let kernel_boot_id = read_kernel_boot_id()?;

        // Та же загрузка железа, что и у предыдущего run'а? Сверяем UUID ядра.
        let hw_boot_id = match epochs
            .hw_boots
            .iter()
            .find(|b| b.kernel_boot_id == kernel_boot_id)
        {
            Some(b) => b.hw_boot_id,
            None => {
                let id = epochs
                    .hw_boots
                    .iter()
                    .map(|b| b.hw_boot_id)
                    .max()
                    .map_or(0, |m| m.saturating_add(1));
                epochs.hw_boots.push(HwBoot {
                    hw_boot_id: id,
                    kernel_boot_id,
                    utc_anchor_ms: None,
                    anchor_source: None,
                    anchor_captured_us: None,
                });
                id
            }
        };

        let boot_counter = epochs
            .runs
            .iter()
            .map(|r| r.boot_counter)
            .chain(floor_boot)
            .max()
            .map_or(0, |m| m.saturating_add(1));

        let current = Run {
            boot_counter,
            hw_boot_id,
            boottime_at_init_us,
        };
        epochs.runs.push(current);

        let store = Self {
            path,
            epochs,
            current,
        };
        store.persist()?;
        Ok(store)
    }

    /// Открыть только для чтения (вьюер, офлайн-анализ) — run не регистрируется.
    ///
    /// **Ничего не пишет.** Дамп, принесённый на анализ, может лежать на
    /// носителе только для чтения, принадлежать другому прибору или быть
    /// вещественным доказательством разбираемой аварии: карантин повреждённого
    /// файла — операция записи, и в этом режиме она недопустима.
    pub fn open_read_only(root: &Path) -> Result<Epochs> {
        Ok(match Self::read(&root.join(EPOCHS_FILE))? {
            Loaded::Ok(e) => e,
            // Относительное время самодостаточно; без эпох теряется только
            // конверсия в UTC.
            Loaded::Missing | Loaded::Corrupt => Epochs::default(),
        })
    }

    /// Прочитать файл, не трогая его.
    fn read(path: &Path) -> Result<Loaded> {
        let Some(bytes) = fsutil::read_optional(path)? else {
            return Ok(Loaded::Missing);
        };
        Ok(match postcard::from_bytes(&bytes) {
            Ok(e) => Loaded::Ok(e),
            Err(_) => Loaded::Corrupt,
        })
    }

    /// То же для пишущей стороны: повреждённый файл уводится в карантин.
    ///
    /// Молча затирать его нельзя — по нему разбирают, что случилось с
    /// привязкой ко времени.
    ///
    /// Второй элемент — «прежнего состояния не осталось»: файла не было или
    /// он оказался нечитаем. Это единственный случай, когда нумерацию
    /// запусков приходится восстанавливать по диску.
    fn load_for_write(path: &Path) -> Result<(Epochs, bool)> {
        match Self::read(path)? {
            Loaded::Ok(e) => Ok((e, false)),
            Loaded::Missing => Ok((Epochs::default(), true)),
            Loaded::Corrupt => {
                let backup = path.with_extension("corrupt");
                let _ = std::fs::rename(path, &backup);
                Ok((Epochs::default(), true))
            }
        }
    }

    pub fn current_run(&self) -> Run {
        self.current
    }

    pub fn epochs(&self) -> &Epochs {
        &self.epochs
    }

    /// Зафиксировать синхронизацию времени для текущей загрузки железа.
    /// Возвращает `false`, если источник менее достоверен, чем текущий якорь.
    pub fn record_sync(&mut self, utc: DateTime<Utc>, source: SyncSource) -> Result<bool> {
        let accepted = self
            .epochs
            .set_anchor(self.current.hw_boot_id, utc, source, boottime_us());
        if accepted {
            self.persist()?;
        }
        Ok(accepted)
    }

    /// Убрать записи о run'ах, от которых не осталось сегментов.
    pub fn retain_runs(&mut self, alive: &dyn Fn(u32) -> bool) -> Result<()> {
        let current = self.current.boot_counter;
        let before = self.epochs.runs.len();
        // Текущий run удалять нельзя ни при каких условиях: он ещё пишет.
        self.epochs
            .retain_runs(&|boot| boot == current || alive(boot));
        if self.epochs.runs.len() != before {
            self.persist()?;
        }
        Ok(())
    }

    fn persist(&self) -> Result<()> {
        let bytes = postcard::to_allocvec(&self.epochs)?;
        fsutil::write_atomic(&self.path, &bytes)
    }
}

/// Прочитать UUID загрузки ядра.
fn read_kernel_boot_id() -> Result<[u8; 16]> {
    const PATH: &str = "/proc/sys/kernel/random/boot_id";
    let raw = std::fs::read_to_string(PATH).ctx("чтение /proc/sys/kernel/random/boot_id")?;
    parse_uuid(raw.trim()).ok_or_else(|| Error::Corrupt {
        path: PathBuf::from(PATH),
        reason: "не UUID".to_owned(),
    })
}

/// Разбор UUID вида `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` без зависимости
/// на крейт uuid: нужно ровно одно место и ровно один формат.
fn parse_uuid(s: &str) -> Option<[u8; 16]> {
    let hex: Vec<u8> = s.bytes().filter(|&b| b != b'-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut out = [0u8; 16];
    for (i, pair) in hex.chunks_exact(2).enumerate() {
        let hi = (pair[0] as char).to_digit(16)?;
        let lo = (pair[1] as char).to_digit(16)?;
        out[i] = ((hi << 4) | lo) as u8;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(dir: &Path, base_us: u64) -> EpochStore {
        EpochStore::open_and_register(dir, base_us, &|| Ok(None)).unwrap()
    }

    /// То же, но с известной нижней границей номера запуска: так открывается
    /// хранилище, у которого сегменты пережили файл эпох.
    fn store_with_floor(dir: &Path, base_us: u64, floor: u32) -> EpochStore {
        EpochStore::open_and_register(dir, base_us, &|| Ok(Some(floor))).unwrap()
    }

    #[test]
    fn a_lost_epochs_file_does_not_restart_run_numbering() {
        // `boot_counter` попадает в имя каждого сегмента, а порядок имён
        // объявлен временным. Начать нумерацию заново поверх сегментов,
        // переживших потерю файла эпох, значило бы поставить новые записи в
        // историю ПЕРЕД старыми: ротация принялась бы за свежее, а читатель
        // отдал бы историю вперемешку.
        let dir = tempfile::tempdir().unwrap();

        // Первый запуск: номер ноль, файл эпох создан.
        assert_eq!(store(dir.path(), 1_000).current_run().boot_counter, 0);
        // Второй помнит первый.
        assert_eq!(store(dir.path(), 2_000).current_run().boot_counter, 1);

        // Файл эпох потерян (порча увела бы его в карантин — тот же исход).
        std::fs::remove_file(dir.path().join(EPOCHS_FILE)).unwrap();

        // Без границы нумерация началась бы заново; с ней — продолжается за
        // тем номером, который виден на именах сегментов.
        assert_eq!(store(dir.path(), 3_000).current_run().boot_counter, 0);
        std::fs::remove_file(dir.path().join(EPOCHS_FILE)).unwrap();
        assert_eq!(
            store_with_floor(dir.path(), 4_000, 1)
                .current_run()
                .boot_counter,
            2,
            "нумерация продолжается за последним запуском, чьи сегменты живы"
        );
    }

    #[test]
    fn an_intact_epochs_file_is_never_second_guessed_by_the_disk() {
        // Граница спрашивается лениво: обход имён при тысячах неймспейсов
        // стоит дороже, чем экономит, и целому файлу эпох он не нужен —
        // его максимум и так покрывает всё, что лежит на диске.
        let dir = tempfile::tempdir().unwrap();
        store(dir.path(), 1_000);

        let asked = std::cell::Cell::new(false);
        let next = EpochStore::open_and_register(dir.path(), 2_000, &|| {
            asked.set(true);
            Ok(Some(1_000_000))
        })
        .unwrap();
        assert!(!asked.get(), "целый файл эпох не требует обхода диска");
        assert_eq!(next.current_run().boot_counter, 1);
    }

    /// Момент времени из миллисекунд эпохи — короче, чем разбирать дату.
    fn utc(ms: i64) -> DateTime<Utc> {
        DateTime::from_timestamp_millis(ms).expect("миллисекунды в пределах эпохи")
    }

    #[test]
    fn registers_runs_incrementally_within_one_hw_boot() {
        let dir = tempfile::tempdir().unwrap();
        let a = store(dir.path(), 1_000);
        assert_eq!(a.current_run().boot_counter, 0);
        drop(a);

        let b = store(dir.path(), 2_000);
        assert_eq!(b.current_run().boot_counter, 1);
        // kernel_boot_id тот же — значит та же загрузка железа.
        assert_eq!(b.current_run().hw_boot_id, 0);
        assert_eq!(b.epochs().hw_boots.len(), 1, "новая загрузка не выдумана");
    }

    #[test]
    fn anchor_is_retroactive_for_events_before_sync() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path(), 5_000_000); // run стартовал на 5-й секунде BOOTTIME
        let at = BootTime::from_raw(0, 1_000_000); // BOOTTIME 6 с

        assert_eq!(s.epochs().to_utc(at), None, "якоря ещё нет");

        // Синхронизация: сейчас BOOTTIME ~T, UTC = 1_700_000_000_000.
        let now_boottime = boottime_us();
        assert!(
            s.epochs
                .set_anchor(0, utc(1_700_000_000_000), SyncSource::Ntp, now_boottime)
        );

        let got = s.epochs().to_utc(at).expect("якорь есть");
        let expected = 1_700_000_000_000 - (now_boottime / 1_000) as i64 + 6_000;
        assert_eq!(
            got.timestamp_millis(),
            expected,
            "событие ДО синхронизации получило UTC"
        );

        // Обратный перевод возвращает то же относительное время: округление
        // якоря до миллисекунд не должно уводить границу запроса.
        assert_eq!(
            s.epochs().from_utc(0, got),
            RunOffset::At(Micros(1_000_000))
        );
    }

    #[test]
    fn from_utc_distinguishes_before_start_from_no_anchor() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path(), 5_000_000);

        // Без якоря сравнивать нечем — и это не то же самое, что «раньше».
        assert_eq!(
            s.epochs().from_utc(0, utc(1_700_000_000_000)),
            RunOffset::Unanchored
        );

        s.epochs
            .set_anchor(0, utc(1_700_000_000_000), SyncSource::Gps, 5_000_000);
        // Якорь = UTC при BOOTTIME 0, run стартовал на 5-й секунде.
        let anchor = s.epochs().hw_boot(0).unwrap().utc_anchor().unwrap();
        assert_eq!(
            s.epochs().from_utc(0, anchor),
            RunOffset::BeforeStart,
            "момент нулевого BOOTTIME раньше старта run'а"
        );
        assert_eq!(
            s.epochs()
                .from_utc(0, anchor + chrono::TimeDelta::seconds(5)),
            RunOffset::At(Micros(0)),
            "ровно старт run'а"
        );
        assert_eq!(
            s.epochs()
                .from_utc(0, anchor + chrono::TimeDelta::seconds(7)),
            RunOffset::At(Micros(2_000_000))
        );
        assert_eq!(
            s.epochs().from_utc(42, utc(1_700_000_000_000)),
            RunOffset::Unanchored
        );
    }

    #[test]
    fn anchored_runs_are_distinguishable() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path(), 1_000);
        assert!(!s.epochs().is_anchored(0));
        assert!(!s.epochs().is_anchored(7), "неизвестный запуск");
        s.record_sync(utc(1_700_000_000_000), SyncSource::Gps)
            .unwrap();
        assert!(s.epochs().is_anchored(0));
        assert_eq!(s.epochs().runs().len(), 1);
    }

    #[test]
    fn anchor_priority_rules() {
        let mut e = Epochs {
            runs: vec![Run {
                boot_counter: 0,
                hw_boot_id: 0,
                boottime_at_init_us: 0,
            }],
            hw_boots: vec![HwBoot {
                hw_boot_id: 0,
                kernel_boot_id: [0; 16],
                utc_anchor_ms: None,
                anchor_source: None,
                anchor_captured_us: None,
            }],
        };

        assert!(
            e.set_anchor(0, utc(1_000_000), SyncSource::User, 0),
            "первый якорь"
        );
        assert!(
            e.set_anchor(0, utc(2_000_000), SyncSource::Gps, 0),
            "GPS поверх ручного"
        );
        assert_eq!(e.hw_boots[0].utc_anchor_ms, Some(2_000_000));

        assert!(
            !e.set_anchor(0, utc(3_000_000), SyncSource::User, 0),
            "ручное поверх GPS — нет"
        );
        assert!(
            !e.set_anchor(0, utc(3_000_000), SyncSource::Ntp, 0),
            "NTP поверх GPS — нет"
        );
        assert_eq!(
            e.hw_boots[0].utc_anchor_ms,
            Some(2_000_000),
            "якорь не тронут"
        );

        assert!(
            e.set_anchor(0, utc(4_000_000), SyncSource::Gps, 0),
            "свежий GPS уточняет старый"
        );
        assert_eq!(e.hw_boots[0].utc_anchor_ms, Some(4_000_000));
        assert_eq!(e.hw_boots[0].utc_anchor(), Some(utc(4_000_000)));

        assert!(
            !e.set_anchor(99, utc(1), SyncSource::Gps, 0),
            "неизвестная загрузка"
        );
    }

    #[test]
    fn persists_across_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = store(dir.path(), 1_000);
            s.record_sync(utc(1_700_000_000_000), SyncSource::Gps)
                .unwrap();
        }
        let s = store(dir.path(), 2_000);
        assert_eq!(s.current_run().boot_counter, 1);
        let hw = s.epochs().hw_boot(0).unwrap();
        assert!(hw.utc_anchor_ms.is_some(), "якорь пережил перезапуск");
        assert_eq!(hw.anchor_source, Some(SyncSource::Gps));
        // Новый run наследует якорь своей загрузки железа.
        assert!(s.epochs().to_utc(BootTime::from_raw(1, 0)).is_some());
    }

    #[test]
    fn corrupt_file_is_quarantined_not_lost() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(EPOCHS_FILE), b"\xff\xff\xff not postcard").unwrap();

        let s = store(dir.path(), 1_000);
        assert_eq!(s.current_run().boot_counter, 0, "начали с чистого листа");
        assert!(
            dir.path().join("epochs.corrupt").exists(),
            "повреждённый файл сохранён для разбора, а не затёрт"
        );
    }

    #[test]
    fn read_only_open_never_writes() {
        // Дамп на анализ приходит с чужого прибора, иногда с носителя только
        // для чтения, иногда как вещдок по разбираемой аварии. Читатель не
        // имеет права его менять — даже ради карантина повреждённого файла.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(EPOCHS_FILE);
        std::fs::write(&path, b"\xff\xff\xff not postcard").unwrap();

        let epochs = EpochStore::open_read_only(dir.path()).unwrap();
        assert_eq!(
            epochs,
            Epochs::default(),
            "без эпох — только относительное время"
        );
        assert!(path.exists(), "файл обязан остаться на месте");
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"\xff\xff\xff not postcard",
            "содержимое не тронуто"
        );
        assert!(
            !dir.path().join("epochs.corrupt").exists(),
            "карантин — операция записи, читателю она запрещена"
        );

        // Целый файл читается как обычно.
        let mut s = store(dir.path(), 1_000);
        s.record_sync(utc(1_700_000_000_000), SyncSource::Gps)
            .unwrap();
        let epochs = EpochStore::open_read_only(dir.path()).unwrap();
        assert_eq!(epochs.runs.len(), 1);
    }

    #[test]
    fn retain_keeps_current_run_and_drops_orphan_hw_boots() {
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path(), 1_000);
        s.epochs.runs.push(Run {
            boot_counter: 100,
            hw_boot_id: 7,
            boottime_at_init_us: 0,
        });
        s.epochs.hw_boots.push(HwBoot {
            hw_boot_id: 7,
            kernel_boot_id: [9; 16],
            utc_anchor_ms: None,
            anchor_source: None,
            anchor_captured_us: None,
        });

        // Живых сегментов нет ни у кого — но текущий run обязан уцелеть.
        s.retain_runs(&|_| false).unwrap();
        assert_eq!(s.epochs().runs.len(), 1);
        assert_eq!(s.epochs().runs[0].boot_counter, 0);
        assert_eq!(s.epochs().hw_boots.len(), 1, "осиротевшая загрузка удалена");
    }

    #[test]
    fn uuid_parsing() {
        let id = parse_uuid("0f8fad5b-d9cb-469f-a165-70867728950e").unwrap();
        assert_eq!(id[0], 0x0f);
        assert_eq!(id[15], 0x0e);
        assert_eq!(parse_uuid("не uuid"), None);
        assert_eq!(parse_uuid(""), None);
        assert_eq!(parse_uuid("0f8fad5b-d9cb-469f-a165-70867728950"), None);
        assert_eq!(parse_uuid("zf8fad5b-d9cb-469f-a165-70867728950e"), None);
    }

    #[test]
    fn real_kernel_boot_id_is_readable() {
        // На Linux файл обязан существовать; тест ловит регресс парсера
        // на реальном формате ядра.
        let id = read_kernel_boot_id().expect("/proc доступен");
        assert_ne!(id, [0u8; 16], "boot_id ядра не бывает нулевым");
    }

    #[test]
    fn utc_conversion_handles_missing_data() {
        let e = Epochs::default();
        assert_eq!(e.to_utc(BootTime::from_raw(0, 0)), None, "нет run'а");
        assert_eq!(e.from_utc(0, utc(1_700_000_000_000)), RunOffset::Unanchored);
    }

    #[test]
    fn absurd_relative_time_does_not_panic() {
        // Битый сегмент может дать любое время. Конверсия обязана вернуть
        // `None`, а не переполниться.
        let dir = tempfile::tempdir().unwrap();
        let mut s = store(dir.path(), 1_000);
        s.record_sync(utc(1_700_000_000_000), SyncSource::Gps)
            .unwrap();
        assert_eq!(s.epochs().to_utc(BootTime::from_raw(0, u64::MAX)), None);
    }
}
