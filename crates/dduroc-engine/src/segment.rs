//! Файл сегмента: создание, дозапись блоков, запечатывание, восстановление.
//!
//! # Почему преаллокация
//!
//! Файл создаётся сразу на полный размер (`fallocate`). Это даёт три вещи:
//!
//! 1. **Дешёвый `fdatasync`**: дозапись блока не меняет размер файла, значит
//!    не трогает метаданные инода — синхронизировать нужно только данные.
//! 2. **Честный отказ по месту**: ENOSPC приходит один раз, при создании
//!    сегмента, а не посреди записи события.
//! 3. **Терминатор скана**: непрописанный хвост заполнен нулями, а нулевой
//!    заголовок блока по формату означает «конец данных» — восстановление
//!    отличает необорванный конец от порчи без дополнительных отметок.
//!
//! # Порядок операций при обрыве питания
//!
//! - *создание*: fallocate → запись заголовка → fdatasync → fsync каталога.
//!   Обрыв раньше fsync каталога — файла нет; позже — файл валиден и пуст.
//! - *дозапись*: pwrite блока → (политика) fdatasync. Обрыв в середине
//!   блока — CRC не сойдётся, восстановление обрежет хвост.
//! - *запечатывание*: ftruncate до конца данных → дозапись footer'а →
//!   fdatasync. Обрыв на любом шаге оставляет сегмент незапечатанным,
//!   то есть читаемым обычным сканом.

use crate::error::{Error, IoContext, Result};
use crate::fsutil;
use dduroc_format::block::BlockHeader;
use dduroc_format::footer::{FOOTER_MAGIC, Trailer};
use dduroc_format::segment::{SegmentHeader, SegmentName};
use dduroc_format::{FooterBuilder, Micros, block};
use std::fs::{File, OpenOptions};
use std::os::unix::fs::{FileExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

/// Открытый на запись сегмент.
#[derive(Debug)]
pub struct SegmentWriter {
    file: File,
    path: PathBuf,
    /// Смещение, куда ляжет следующий блок.
    end: u64,
    /// Ёмкость, выделенная при создании.
    capacity: u64,
    header: SegmentHeader,
    /// Номер следующего блока: разрыв нумерации отличает потерянный блок
    /// от порчи при чтении.
    next_seq: u32,
    dirty: bool,
}

impl SegmentWriter {
    /// Создать новый сегмент ёмкостью `capacity` байт.
    pub fn create(dir: &Path, header: SegmentHeader, capacity: u64) -> Result<Self> {
        let name = SegmentName::new(header.boot, header.base);
        let path = dir.join(name.to_string());

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            // create_new: сегмент с таким именем уже существовать не может —
            // иначе мы затёрли бы чужие данные тем же (boot, время) ключом.
            .create_new(true)
            .mode(fsutil::FILE_MODE)
            .open(&path)
            .ctx_path("создание сегмента", &path)?;

        let capacity = capacity.max(SegmentHeader::SIZE as u64 + BlockHeader::SIZE as u64);
        if let Err(e) = grow_to(&file, capacity, &path) {
            // Файл без места бесполезен и мешает следующей попытке.
            let _ = std::fs::remove_file(&path);
            return Err(e);
        }

        file.write_all_at(&header.to_bytes(), 0)
            .ctx_path("запись заголовка", &path)?;
        fsutil::sync_data(&file, &path)?;
        fsutil::sync_dir(dir)?;

        Ok(Self {
            file,
            path,
            end: SegmentHeader::SIZE as u64,
            capacity,
            header,
            next_seq: 0,
            dirty: false,
        })
    }

    /// Открыть существующий сегмент для продолжения записи, восстановив
    /// позицию конца данных.
    ///
    /// После восстановления хвост **обнуляется**: `ftruncate` до конца целых
    /// данных и обратное расширение до ёмкости. Без этого за новым, более
    /// коротким блоком остались бы байты прежней записи — и следующий скан
    /// прочитал бы их как блок. В худшем случае уцелевший старый блок сошёлся
    /// бы по CRC, и в лог вернулись бы записи из уже отброшенного хвоста
    /// с временами, нарушающими монотонность.
    pub fn reopen(path: &Path, expect_store: Option<u64>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .ctx_path("открытие сегмента", path)?;
        let capacity = file.metadata().ctx_path("stat", path)?.len();

        let scan = Scan::run(&file, capacity, path)?;
        scan.check_store(expect_store, path)?;

        file.set_len(scan.data_end)
            .ctx_path("обрезка повреждённого хвоста", path)?;
        grow_to(&file, capacity, path)?;
        fsutil::sync_data(&file, path)?;

        Ok(Self {
            file,
            path: path.to_owned(),
            end: scan.data_end,
            capacity,
            header: scan.header,
            next_seq: scan.next_seq,
            dirty: false,
        })
    }

    /// Номер, который получит следующий блок.
    pub fn next_seq(&self) -> u32 {
        self.next_seq
    }

    pub fn header(&self) -> SegmentHeader {
        self.header
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Смещение конца данных (= размер полезной части).
    pub fn data_end(&self) -> u64 {
        self.end
    }

    /// Сколько байт ещё поместится без выхода за преаллоцированную ёмкость.
    pub fn remaining(&self) -> u64 {
        self.capacity.saturating_sub(self.end)
    }

    /// Есть ли незасинхронизированные данные.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Поместится ли блок такого размера.
    pub fn fits(&self, block_len: u64) -> bool {
        // Оставляем место под нулевой заголовок-терминатор: без него скан
        // упёрся бы в конец файла вместо честного признака конца данных.
        self.remaining() >= block_len + BlockHeader::SIZE as u64
    }

    /// Дописать готовый блок (заголовок + тело). Возвращает его смещение.
    ///
    /// Блок обязан быть собран с номером [`Self::next_seq`].
    pub fn append_block(&mut self, bytes: &[u8]) -> Result<u64> {
        let offset = self.end;
        self.file
            .write_all_at(bytes, offset)
            .ctx_path("запись блока", &self.path)?;
        self.end += bytes.len() as u64;
        self.next_seq = self.next_seq.saturating_add(1);
        self.dirty = true;
        Ok(offset)
    }

    /// `fdatasync`: после возврата данные переживут потерю питания.
    pub fn sync(&mut self) -> Result<()> {
        if !self.dirty {
            return Ok(());
        }
        fsutil::sync_data(&self.file, &self.path)?;
        self.dirty = false;
        Ok(())
    }

    /// Запечатать сегмент: обрезать до конца данных и дописать footer.
    ///
    /// Footer — оптимизация чтения, поэтому его отсутствие не потеря: обрыв
    /// на любом шаге оставит сегмент валидным, но незапечатанным.
    pub fn seal(mut self, footer: &[u8]) -> Result<()> {
        self.sync()?;
        self.file
            .set_len(self.end)
            .ctx_path("обрезка до конца данных", &self.path)?;
        self.file
            .write_all_at(footer, self.end)
            .ctx_path("запись footer", &self.path)?;
        fsutil::sync_data(&self.file, &self.path)?;
        Ok(())
    }

    /// Обрезать до конца данных без footer'а — для аварийного закрытия.
    pub fn close_unsealed(mut self) -> Result<()> {
        self.sync()?;
        self.file
            .set_len(self.end)
            .ctx_path("обрезка до конца данных", &self.path)
    }
}

/// Довести файл до размера `capacity`, зарезервировав место на носителе.
///
/// Преаллокация — не оптимизация, а способ получить ENOSPC один раз, при
/// создании сегмента, а не посреди записи критического события.
fn grow_to(file: &File, capacity: u64, path: &Path) -> Result<()> {
    match rustix::fs::fallocate(file, rustix::fs::FallocateFlags::empty(), 0, capacity) {
        Ok(()) => Ok(()),
        // Часть ФС (tmpfs на старых ядрах, некоторые overlay) не умеет
        // fallocate. Место не резервируется, но формат от этого не страдает:
        // дотягиваем размер обычным ftruncate — хвост тоже будет нулевым.
        Err(rustix::io::Errno::OPNOTSUPP | rustix::io::Errno::NOSYS) => {
            file.set_len(capacity).ctx_path("ftruncate", path)
        }
        Err(rustix::io::Errno::NOSPC) => Err(Error::NoSpace(path.to_owned())),
        Err(e) => Err(e).ctx_path("fallocate", path),
    }
}

/// Что оборвало обход блоков.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanEnd {
    /// Нулевой заголовок: непрописанный хвост преаллокации. Штатный случай.
    ZeroTail,
    /// Данные кончились ровно на границе файла.
    FileEnd,
    /// Заголовок или CRC не сошлись — оборванная запись при потере питания
    /// либо порча носителя.
    Corrupt,
    /// Номер блока не на единицу больше предыдущего: часть данных не дошла
    /// до носителя, хотя последующие дошли (переупорядочивание writeback).
    SeqGap { expected: u32, found: u32 },
}

impl ScanEnd {
    /// Требует ли этот исход отбрасывания хвоста.
    pub fn is_damage(self) -> bool {
        matches!(self, ScanEnd::Corrupt | ScanEnd::SeqGap { .. })
    }
}

/// Результат восстановления сегмента.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Scan {
    pub header: SegmentHeader,
    /// Смещение конца последнего целого блока.
    pub data_end: u64,
    pub block_count: u32,
    /// Номер, который должен получить следующий блок.
    pub next_seq: u32,
    /// Время первой записи последнего блока (нижняя оценка максимума
    /// времени сегмента: точное значение требует разбора тела).
    pub last_base: Micros,
    /// Чем закончился обход.
    pub end: ScanEnd,
}

impl Scan {
    /// Хвост был повреждён и отброшен.
    pub fn truncated(&self) -> bool {
        self.end.is_damage()
    }

    /// Проверить, что сегмент принадлежит этому хранилищу.
    ///
    /// Файлы, скопированные с другого устройства, имеют собственную нумерацию
    /// `boot_counter` и собственную привязку ко времени: слив их с локальными
    /// дал бы событиям чужого прибора локальный UTC-якорь, то есть заведомо
    /// неверное абсолютное время.
    pub fn check_store(&self, expect: Option<u64>, path: &Path) -> Result<()> {
        match expect {
            Some(id) if self.header.store_id != id => Err(Error::ForeignSegment {
                path: path.to_owned(),
                expected: id,
                found: self.header.store_id,
            }),
            _ => Ok(()),
        }
    }
    /// Пройти сегмент по заголовкам блоков, найдя конец целых данных.
    ///
    /// Блоки читаются по одному в переиспользуемый буфер: сегмент может быть
    /// сотнями мегабайт, а на armv7 памяти мало — читать файл целиком нельзя.
    pub fn run(file: &File, file_len: u64, path: &Path) -> Result<Self> {
        Self::run_inner(file, file_len, path, None).map(|(scan, _)| scan)
    }

    /// То же с попутной сборкой footer'а.
    ///
    /// Обход и так читает каждое тело и сверяет его CRC, поэтому разбор
    /// записей ради множеств `event_id`/`metric_id` и индекса блоков стоит
    /// немногим больше самого обхода — второго прохода по сегменту не нужно.
    ///
    /// Второе значение — **полон ли** собранный footer. Тело, не разобравшееся
    /// при сошедшемся CRC, сбор прекращает, но обход — нет: байты прошли
    /// проверку целостности, значит это данные, и обрезать их из-за того, что
    /// их нечем разложить на записи (кодек не встроен в этот билд), нельзя.
    /// Неполный footer писать некуда — с ним читатель считал бы сегмент
    /// заканчивающимся раньше, чем он заканчивается.
    pub fn run_collecting(
        file: &File,
        file_len: u64,
        path: &Path,
        footer: &mut FooterBuilder,
    ) -> Result<(Self, bool)> {
        Self::run_inner(file, file_len, path, Some(footer))
    }

    fn run_inner(
        file: &File,
        file_len: u64,
        path: &Path,
        mut footer: Option<&mut FooterBuilder>,
    ) -> Result<(Self, bool)> {
        let mut footer_complete = true;
        let mut head = [0u8; SegmentHeader::SIZE];
        file.read_exact_at(&mut head, 0)
            .ctx_path("чтение заголовка сегмента", path)?;
        let header = SegmentHeader::parse(&head).map_err(|e| Error::Corrupt {
            path: path.to_owned(),
            reason: format!("заголовок сегмента: {e}"),
        })?;

        let mut offset = SegmentHeader::SIZE as u64;
        let mut block_count = 0u32;
        let mut last_base = header.base;
        let mut buf: Vec<u8> = Vec::new();
        let end;

        loop {
            if file_len.saturating_sub(offset) < BlockHeader::SIZE as u64 {
                end = ScanEnd::FileEnd;
                break;
            }
            let mut hdr = [0u8; BlockHeader::SIZE];
            if file.read_exact_at(&mut hdr, offset).is_err() {
                end = ScanEnd::Corrupt;
                break;
            }
            let parsed = match BlockHeader::parse(&hdr) {
                Ok(Some(h)) => h,
                // Полностью нулевой заголовок — непрописанный хвост.
                Ok(None) => {
                    end = ScanEnd::ZeroTail;
                    break;
                }
                Err(_) => {
                    end = ScanEnd::Corrupt;
                    break;
                }
            };

            // Разрыв нумерации: предыдущие блоки осели на носитель, а этот —
            // из другой эпохи записи. Диагноз отличается от порчи, потому что
            // отличается причина: не битый носитель, а недошедшая запись.
            if parsed.seq != block_count {
                end = ScanEnd::SeqGap {
                    expected: block_count,
                    found: parsed.seq,
                };
                break;
            }

            let body_len = u64::from(parsed.body_len);
            let block_end = offset + BlockHeader::SIZE as u64 + body_len;
            // Длина из повреждённого заголовка может быть любой: прежде чем
            // выделять буфер, сверяемся с реальным остатком файла.
            if block_end > file_len {
                end = ScanEnd::Corrupt;
                break;
            }

            buf.clear();
            buf.resize(body_len as usize, 0);
            if file
                .read_exact_at(&mut buf, offset + BlockHeader::SIZE as u64)
                .is_err()
            {
                end = ScanEnd::Corrupt;
                break;
            }
            if parsed.verify(&buf).is_err() {
                end = ScanEnd::Corrupt;
                break;
            }

            if let Some(fb) = footer.as_deref_mut() {
                // Тело разбирается ради множеств типов: миграция по ним решает,
                // переписывать ли сегмент, а читатель — что в нём вообще есть.
                //
                // Тело, не разобравшееся при сошедшемся CRC (кодек не встроен в
                // этот билд), обход не обрывает: байты прошли проверку
                // целостности, значит это данные. Прекращается только сбор —
                // неполный footer хуже, чем никакого.
                match block::Block::from_parts(parsed, &buf) {
                    Ok(block) => {
                        let mut last = parsed.base;
                        for item in block.records() {
                            let Ok((at, record)) = item else { break };
                            last = at;
                            match record {
                                dduroc_format::Record::Message(m) => fb.add_event(m.event),
                                dduroc_format::Record::Sample(s) => fb.add_metric(s.metric),
                                _ => {}
                            }
                        }
                        fb.add_block(offset, &parsed, last);
                    }
                    Err(_) => {
                        footer_complete = false;
                        footer = None;
                    }
                }
            }

            block_count += 1;
            last_base = parsed.base;
            offset = block_end;
        }

        Ok((
            Self {
                header,
                data_end: offset,
                block_count,
                next_seq: block_count,
                last_base,
                end,
            },
            footer_complete,
        ))
    }

    /// Прочитать сегмент с диска и восстановить его границы.
    pub fn of_path(path: &Path) -> Result<Self> {
        let file = File::open(path).ctx_path("открытие сегмента", path)?;
        let len = file.metadata().ctx_path("stat", path)?.len();
        Self::run(&file, len, path)
    }
}

// ════════════════════════════════════════════════════════════════════════════
// Восстановление оборванного сегмента
// ════════════════════════════════════════════════════════════════════════════

/// Что дало запечатывание оборванного сегмента.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Recovered {
    pub name: SegmentName,
    /// Размер файла после запечатывания.
    pub size: u64,
    /// Сколько байт преаллокации вернулось носителю.
    pub reclaimed: u64,
    /// Хвост был повреждён и отброшен (обрыв питания посреди блока).
    pub truncated: bool,
}

/// Запечатан ли сегмент — по сигнатуре в последних четырёх байтах.
///
/// Дешёвая проверка: одно чтение четырёх байт. Разбирать footer целиком ради
/// ответа «нужно ли восстановление» незачем, а при подъёме хранилища этот
/// вопрос задаётся каждому каналу.
fn is_sealed(file: &File, len: u64) -> Result<bool> {
    if len < (SegmentHeader::SIZE + Trailer::SIZE) as u64 {
        return Ok(false);
    }
    let mut magic = [0u8; 4];
    match file.read_exact_at(&mut magic, len - 4) {
        Ok(()) => Ok(magic == FOOTER_MAGIC),
        // Файл короче, чем сказал stat: кто-то его правит. Считаем
        // незапечатанным — восстановление разберётся честнее, чем догадка.
        Err(_) => Ok(false),
    }
}

/// Обрезать оборванный сегмент до конца целых данных и запечатать его.
///
/// Оборванный сегмент прошлого запуска читается и сканом, поэтому данные не
/// теряются и без этого. Но пока он не запечатан, за ним числится **вся**
/// преаллокация: файл на 256 МиБ, в котором лежит килобайт данных, занимает
/// в бюджете канала 256 МиБ. Несколько аварийных остановок подряд — и ротация
/// начинает удалять живую историю, чтобы уместить пустоту.
///
/// Заодно сегмент получает footer: индекс блоков читателю и множества типов
/// миграции. Второго прохода по файлу это не стоит — [`Scan::run_collecting`]
/// собирает их тем же обходом, которым ищет конец данных.
///
/// `Ok(None)` — сегмент уже запечатан либо это не сегмент.
pub fn seal_orphan(path: &Path, expect_store: Option<u64>) -> Result<Option<Recovered>> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .ctx_path("открытие сегмента для восстановления", path)?;
    let len = file.metadata().ctx_path("stat", path)?.len();
    if len < SegmentHeader::SIZE as u64 || is_sealed(&file, len)? {
        return Ok(None);
    }

    let mut footer = FooterBuilder::new();
    let (scan, footer_complete) = Scan::run_collecting(&file, len, path, &mut footer)?;
    // Чужой сегмент не наш, чтобы его переписывать: у него своя нумерация
    // запусков и своя привязка ко времени.
    scan.check_store(expect_store, path)?;

    // Обрезка возвращает преаллокацию в любом случае — ради неё всё и
    // затевается. Footer дописывается, только если он полон: индекс, который
    // перечисляет не все блоки, увёл бы читателя мимо остальных, а без него
    // сегмент честно читается сканом.
    file.set_len(scan.data_end)
        .ctx_path("обрезка до конца данных", path)?;
    let bytes = if footer_complete {
        let bytes = footer.build();
        file.write_all_at(&bytes, scan.data_end)
            .ctx_path("запись footer", path)?;
        bytes
    } else {
        Vec::new()
    };
    fsutil::sync_data(&file, path)?;

    let size = scan.data_end + bytes.len() as u64;
    Ok(Some(Recovered {
        name: scan.header.file_name(),
        size,
        reclaimed: len.saturating_sub(size),
        truncated: scan.truncated(),
    }))
}

/// Сегмент, открытый на чтение.
#[derive(Debug)]
pub struct SegmentReader {
    file: File,
    path: PathBuf,
    len: u64,
    header: SegmentHeader,
    /// Байты footer'а, если сегмент запечатан.
    footer_bytes: Option<Vec<u8>>,
    data_end: u64,
}

impl SegmentReader {
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path).ctx_path("открытие сегмента", path)?;
        let len = file.metadata().ctx_path("stat", path)?.len();

        let mut head = [0u8; SegmentHeader::SIZE];
        file.read_exact_at(&mut head, 0)
            .ctx_path("чтение заголовка", path)?;
        let header = SegmentHeader::parse(&head).map_err(|e| Error::Corrupt {
            path: path.to_owned(),
            reason: format!("заголовок сегмента: {e}"),
        })?;

        // Двухфазное чтение footer'а: сначала трейлер фиксированного размера,
        // из него — длина, потом сам footer. Так не читается лишнего.
        let mut footer_bytes = None;
        let mut data_end = len;
        if len >= (SegmentHeader::SIZE + Trailer::SIZE) as u64 {
            let mut tail = [0u8; Trailer::SIZE];
            file.read_exact_at(&mut tail, len - Trailer::SIZE as u64)
                .ctx_path("чтение трейлера", path)?;
            if let Ok(Some(trailer)) = Trailer::parse(&tail) {
                let total = trailer.total_len();
                // Длина из трейлера управляет и размером чтения, и границей
                // данных, а сам трейлер сверяется только по сигнатуре.
                // Поэтому footer принимается лишь после проверки CRC: иначе
                // испорченное поле длины молча отрезало бы часть блоков,
                // выдав усечённый сегмент за целый.
                if total <= len - SegmentHeader::SIZE as u64 {
                    let mut buf = vec![0u8; total as usize];
                    file.read_exact_at(&mut buf, len - total)
                        .ctx_path("чтение footer", path)?;
                    if matches!(dduroc_format::Footer::parse(&buf), Ok(Some(_))) {
                        data_end = len - total;
                        footer_bytes = Some(buf);
                    }
                }
            }
        }

        Ok(Self {
            file,
            path: path.to_owned(),
            len,
            header,
            footer_bytes,
            data_end,
        })
    }

    pub fn header(&self) -> SegmentHeader {
        self.header
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len <= SegmentHeader::SIZE as u64
    }

    pub fn is_sealed(&self) -> bool {
        self.footer_bytes.is_some()
    }

    /// Разобранный footer запечатанного сегмента.
    pub fn footer(&self) -> Option<dduroc_format::Footer> {
        let bytes = self.footer_bytes.as_ref()?;
        dduroc_format::Footer::parse(bytes).ok().flatten()
    }

    /// Прочитать блок по смещению в `buf`. `Ok(None)` — конец данных.
    ///
    /// Возвращает смещение следующего блока.
    pub fn read_block_at(&self, offset: u64, buf: &mut Vec<u8>) -> Result<Option<u64>> {
        if offset >= self.data_end || self.data_end - offset < BlockHeader::SIZE as u64 {
            return Ok(None);
        }
        let mut hdr = [0u8; BlockHeader::SIZE];
        self.file
            .read_exact_at(&mut hdr, offset)
            .ctx_path("чтение заголовка блока", &self.path)?;
        let Some(header) = BlockHeader::parse(&hdr).map_err(|e| Error::Corrupt {
            path: self.path.clone(),
            reason: format!("заголовок блока на {offset}: {e}"),
        })?
        else {
            return Ok(None);
        };

        let total = BlockHeader::SIZE as u64 + u64::from(header.body_len);
        if offset + total > self.data_end {
            return Err(Error::Corrupt {
                path: self.path.clone(),
                reason: format!("блок на {offset} выходит за конец данных"),
            });
        }

        buf.clear();
        buf.resize(total as usize, 0);
        self.file
            .read_exact_at(buf, offset)
            .ctx_path("чтение блока", &self.path)?;
        Ok(Some(offset + total))
    }

    /// Смещение первого блока.
    pub const fn first_block_offset() -> u64 {
        SegmentHeader::SIZE as u64
    }

    /// Смещения блоков: из footer'а, если он есть, иначе последовательным
    /// сканом.
    ///
    /// Повреждение обрывает **скан**, а не всю выборку: уже найденные блоки
    /// возвращаются. Иначе один битый заголовок в хвосте незапечатанного
    /// сегмента — обычное следствие обрыва питания — прятал бы от читателя
    /// весь сегмент целиком.
    pub fn block_offsets(&self) -> Result<Vec<u64>> {
        if let Some(footer) = self.footer() {
            return Ok(footer.blocks.iter().map(|b| b.offset).collect());
        }
        Ok(self.scan_block_offsets().0)
    }

    /// То же, но с сообщением о том, где скан оборвался.
    ///
    /// Помимо порчи скан ловит **разрыв нумерации блоков**: номера идут с нуля
    /// подряд, и пропуск означает, что кусок записи не дошёл до носителя, хотя
    /// последующие дошли. Диагноз отличается от порчи, потому что отличается
    /// причина, и молчать о нём нельзя: дальше по файлу лежат валидные блоки,
    /// между которыми образовалась дыра, а ответ без единого признака выглядел
    /// бы полным.
    pub fn scan_block_offsets(&self) -> (Vec<u64>, Option<(u64, String)>) {
        let mut offsets = Vec::new();
        let mut buf = Vec::new();
        let mut offset = Self::first_block_offset();
        let mut expected_seq = 0u32;
        loop {
            match self.read_block_at(offset, &mut buf) {
                Ok(Some(next)) => {
                    // Заголовок уже прочитан в буфер — разбор не стоит ни
                    // одного обращения к носителю.
                    if let Ok(Some(header)) = BlockHeader::parse(&buf)
                        && header.seq != expected_seq
                    {
                        return (
                            offsets,
                            Some((
                                offset,
                                format!(
                                    "разрыв нумерации блоков: ожидался {expected_seq}, \
                                     в файле {}",
                                    header.seq
                                ),
                            )),
                        );
                    }
                    expected_seq = expected_seq.saturating_add(1);
                    offsets.push(offset);
                    offset = next;
                }
                Ok(None) => return (offsets, None),
                Err(e) => return (offsets, Some((offset, e.to_string()))),
            }
        }
    }
}

/// Разобрать блок из сырых байт, прочитанных [`SegmentReader::read_block_at`].
pub fn parse_block(bytes: &[u8]) -> Result<Option<block::Block<'_>>> {
    Ok(block::Block::parse(bytes)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dduroc_format::block::{BlockBuilder, Compression};
    use dduroc_format::record::Message;
    use dduroc_format::{BootCounter, EventId, ProtocolVersion, Record};

    fn header(base: u64) -> SegmentHeader {
        SegmentHeader {
            protocol_version: ProtocolVersion(1),
            boot: BootCounter(7),
            base: Micros(base),
            store_id: 0,
        }
    }

    fn block_bytes(seq: u32, base: u64, count: usize) -> (Vec<u8>, BlockHeader) {
        let mut b = BlockBuilder::new();
        for i in 0..count {
            b.push(
                Micros(base + i as u64 * 100),
                &Record::Message(Message {
                    event: EventId(1),
                    span: None,
                    payload: &[0xAB; 8],
                }),
            )
            .unwrap();
        }
        let mut out = Vec::new();
        let h = b.finish(seq, Compression::None, &mut out).unwrap();
        (out, h)
    }

    /// Записать блок с номером, которого ждёт сегмент.
    fn append(w: &mut SegmentWriter, base: u64, count: usize) -> u64 {
        let (bytes, _) = block_bytes(w.next_seq(), base, count);
        w.append_block(&bytes).unwrap()
    }

    #[test]
    fn create_append_and_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let mut w = SegmentWriter::create(dir.path(), header(1_000), 64 * 1024).unwrap();
        assert_eq!(w.data_end(), SegmentHeader::SIZE as u64);
        let offset = append(&mut w, 1_000, 3);
        assert_eq!(offset, SegmentHeader::SIZE as u64);
        w.sync().unwrap();
        assert!(!w.is_dirty());
        let end = w.data_end();
        let path = w.path().to_owned();
        drop(w);

        // Файл преаллоцирован — размер больше, чем данных.
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 64 * 1024);

        let w2 = SegmentWriter::reopen(&path, None).unwrap();
        assert_eq!(w2.data_end(), end, "позиция конца данных восстановлена");
        assert_eq!(w2.header(), header(1_000));
        assert_eq!(w2.next_seq(), 1, "нумерация блоков продолжается");
    }

    #[test]
    fn zero_tail_terminates_scan() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(1_000), 32 * 1024).unwrap();
        append(&mut w, 1_000, 2);
        append(&mut w, 2_000, 2);
        w.sync().unwrap();
        let end = w.data_end();
        let path = w.path().to_owned();
        drop(w);

        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 2);
        assert_eq!(scan.end, ScanEnd::ZeroTail);
        assert!(!scan.truncated(), "целый хвост не считается повреждённым");
        assert_eq!(scan.data_end, end);
    }

    #[test]
    fn torn_block_is_truncated_not_fatal() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(1_000), 32 * 1024).unwrap();
        append(&mut w, 1_000, 4);
        let good_end = w.data_end();
        append(&mut w, 2_000, 4);
        w.sync().unwrap();
        let path = w.path().to_owned();
        drop(w);

        // Имитируем обрыв питания посреди второго блока: портим его хвост.
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.write_all_at(&[0xFF; 4], good_end + 34).unwrap();
        }

        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 1, "уцелел первый блок");
        assert_eq!(scan.end, ScanEnd::Corrupt, "обрыв распознан как порча");
        assert_eq!(scan.data_end, good_end, "конец данных — до битого блока");

        // Продолжение записи с восстановленной позиции затирает битый хвост.
        let mut w = SegmentWriter::reopen(&path, None).unwrap();
        assert_eq!(w.data_end(), good_end);
        assert_eq!(w.next_seq(), 1);
        append(&mut w, 3_000, 4);
        w.sync().unwrap();
        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 2);
        assert!(!scan.truncated());
    }

    #[test]
    fn tail_is_zeroed_after_recovery() {
        // Новый блок короче отброшенного: за ним не должно остаться байт
        // прежней записи, иначе следующий скан прочитает их как блок —
        // в худшем случае со сходящимся CRC, вернув в лог выброшенные записи.
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(0), 32 * 1024).unwrap();
        append(&mut w, 0, 1);
        let good_end = w.data_end();
        append(&mut w, 1_000, 50); // длинный блок, который «не долетит»
        w.sync().unwrap();
        let long_end = w.data_end();
        let path = w.path().to_owned();
        drop(w);

        // Портим заголовок длинного блока.
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.write_all_at(&[0xFF; 8], good_end + 4).unwrap();
        }

        let mut w = SegmentWriter::reopen(&path, None).unwrap();
        assert_eq!(w.data_end(), good_end);

        // Проверяем, что область прежнего длинного блока обнулена.
        let f = File::open(&path).unwrap();
        let mut probe = vec![0u8; (long_end - good_end) as usize];
        f.read_exact_at(&mut probe, good_end).unwrap();
        assert!(
            probe.iter().all(|&b| b == 0),
            "хвост после восстановления обязан быть нулевым"
        );

        append(&mut w, 2_000, 1);
        w.sync().unwrap();
        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 2);
        assert_eq!(scan.end, ScanEnd::ZeroTail, "чистый конец данных");
    }

    #[test]
    fn seq_gap_is_distinguished_from_corruption() {
        // Блоки B1 и B3 осели на носитель, B2 — нет (переупорядочивание
        // writeback). Это не порча носителя, и диагноз обязан отличаться.
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(0), 32 * 1024).unwrap();
        append(&mut w, 0, 1);
        let end_after_first = w.data_end();
        let (bytes, _) = block_bytes(5, 1_000, 1); // номер «из будущего»
        w.append_block(&bytes).unwrap();
        w.sync().unwrap();
        let path = w.path().to_owned();
        drop(w);

        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 1);
        assert_eq!(
            scan.end,
            ScanEnd::SeqGap {
                expected: 1,
                found: 5
            }
        );
        assert!(scan.truncated());
        assert_eq!(scan.data_end, end_after_first);
    }

    #[test]
    fn foreign_store_segment_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let mut h = header(0);
        h.store_id = 0xAAAA_BBBB_CCCC_DDDD;
        let w = SegmentWriter::create(dir.path(), h, 8 * 1024).unwrap();
        let path = w.path().to_owned();
        drop(w);

        let err = SegmentWriter::reopen(&path, Some(0x1111_2222_3333_4444)).unwrap_err();
        assert!(
            matches!(err, Error::ForeignSegment { .. }),
            "сегмент чужого хранилища обязан отвергаться: {err}"
        );
        // Со своим идентификатором открывается штатно.
        SegmentWriter::reopen(&path, Some(0xAAAA_BBBB_CCCC_DDDD)).unwrap();
    }

    #[test]
    fn garbage_block_length_does_not_allocate_wildly() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(0), 8 * 1024).unwrap();
        append(&mut w, 0, 1);
        w.sync().unwrap();
        let path = w.path().to_owned();
        let end = w.data_end();
        drop(w);

        // body_len = 0xFFFF_FFFF при файле в 8 КиБ.
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.write_all_at(&[0xFF, 0xFF, 0xFF, 0xFF], end).unwrap();
        }
        let scan = Scan::of_path(&path).unwrap();
        assert!(scan.truncated());
        assert_eq!(scan.data_end, end, "мусорная длина не увела скан за файл");
    }

    #[test]
    fn orphan_is_sealed_and_gives_back_its_preallocation() {
        // Оборванный сегмент читается и сканом, поэтому данные не теряются и
        // без запечатывания. Но пока footer'а нет, за файлом числится ВСЯ
        // преаллокация: 32 КиБ на пару сотен байт данных. Несколько аварийных
        // остановок подряд выедают бюджет канала пустотой, после чего ротация
        // принимается за живую историю — вот ради чего восстановление.
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(1_000), 32 * 1024).unwrap();
        append(&mut w, 1_000, 3);
        append(&mut w, 2_000, 3);
        w.sync().unwrap();
        let path = w.path().to_owned();
        let data_end = w.data_end();
        drop(w); // «обрыв питания»: seal не вызывался

        assert_eq!(std::fs::metadata(&path).unwrap().len(), 32 * 1024);
        assert!(!SegmentReader::open(&path).unwrap().is_sealed());

        let rec = seal_orphan(&path, Some(0))
            .unwrap()
            .expect("сегмент оборван");
        assert_eq!(rec.name, SegmentName::new(BootCounter(7), Micros(1_000)));
        assert!(!rec.truncated, "целый хвост — не повреждение");
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            rec.size,
            "размер файла совпал с объявленным"
        );
        assert!(rec.size < 4 * 1024, "преаллокация возвращена: {}", rec.size);
        assert_eq!(rec.reclaimed, 32 * 1024 - rec.size);

        // Footer собран тем же обходом, которым искался конец данных.
        let r = SegmentReader::open(&path).unwrap();
        assert!(r.is_sealed());
        let footer = r.footer().expect("footer читается");
        assert_eq!(footer.blocks.len(), 2);
        assert_eq!(footer.blocks[0].offset, SegmentHeader::SIZE as u64);
        assert_eq!(
            footer.events,
            vec![EventId(1)],
            "множество типов собрано по телам блоков — без него миграция \
             прошла бы мимо сегмента"
        );
        assert_eq!(r.block_offsets().unwrap().len(), 2);
        assert!(data_end > 0);

        // Повторный вызов ничего не делает: сегмент уже запечатан.
        assert_eq!(seal_orphan(&path, Some(0)).unwrap(), None);
    }

    #[test]
    fn orphan_recovery_drops_the_torn_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(0), 32 * 1024).unwrap();
        append(&mut w, 0, 2);
        let good_end = w.data_end();
        append(&mut w, 1_000, 2);
        w.sync().unwrap();
        let path = w.path().to_owned();
        drop(w);

        // Портим второй блок — обрыв питания посреди записи.
        {
            let f = OpenOptions::new().write(true).open(&path).unwrap();
            f.write_all_at(&[0xFF; 4], good_end + 34).unwrap();
        }

        let rec = seal_orphan(&path, Some(0)).unwrap().unwrap();
        assert!(rec.truncated, "порча хвоста обязана быть объявлена");
        let r = SegmentReader::open(&path).unwrap();
        assert!(r.is_sealed());
        assert_eq!(
            r.footer().unwrap().blocks.len(),
            1,
            "в footer попал только уцелевший блок"
        );
    }

    #[test]
    fn orphan_of_a_foreign_store_is_left_alone() {
        // Файл, принесённый с другого прибора, переписывать нельзя: у него
        // своя нумерация запусков и своя привязка ко времени.
        let dir = tempfile::tempdir().unwrap();
        let mut h = header(0);
        h.store_id = 0xAAAA_BBBB_CCCC_DDDD;
        let mut w = SegmentWriter::create(dir.path(), h, 16 * 1024).unwrap();
        append(&mut w, 0, 1);
        w.sync().unwrap();
        let path = w.path().to_owned();
        drop(w);

        let err = seal_orphan(&path, Some(0x1111_2222_3333_4444)).unwrap_err();
        assert!(
            matches!(err, Error::ForeignSegment { .. }),
            "получено {err}"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            16 * 1024,
            "чужой файл не тронут"
        );
    }

    #[test]
    fn scan_reports_a_hole_in_block_numbering() {
        // Номера блоков идут с нуля подряд. Пропуск означает, что кусок записи
        // не дошёл до носителя, хотя последующие дошли, — и между валидными
        // блоками образовалась дыра. Ответ без единого признака выглядел бы
        // полным, поэтому читатель обязан о ней сказать.
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(0), 32 * 1024).unwrap();
        append(&mut w, 0, 1);
        let (bytes, _) = block_bytes(5, 1_000, 1); // номер «из будущего»
        w.append_block(&bytes).unwrap();
        w.sync().unwrap();
        let path = w.path().to_owned();
        w.close_unsealed().unwrap();

        let r = SegmentReader::open(&path).unwrap();
        let (offsets, stopped) = r.scan_block_offsets();
        assert_eq!(offsets.len(), 1, "уцелевший блок остаётся в выборке");
        let (_, reason) = stopped.expect("разрыв обязан быть назван");
        assert!(
            reason.contains("разрыв нумерации"),
            "диагноз должен отличаться от порчи: {reason}"
        );
    }

    #[test]
    fn seal_writes_footer_and_trims() {
        use dduroc_format::FooterBuilder;

        let dir = tempfile::tempdir().unwrap();
        let (bytes, bh) = block_bytes(0, 1_000, 3);
        let mut w = SegmentWriter::create(dir.path(), header(1_000), 64 * 1024).unwrap();
        let offset = w.append_block(&bytes).unwrap();
        let path = w.path().to_owned();

        let mut fb = FooterBuilder::new();
        fb.add_block(offset, &bh, Micros(1_200));
        fb.add_event(EventId(1));
        w.seal(&fb.build()).unwrap();

        let size = std::fs::metadata(&path).unwrap().len();
        assert!(size < 64 * 1024, "хвост преаллокации обрезан: {size}");

        let r = SegmentReader::open(&path).unwrap();
        assert!(r.is_sealed());
        let footer = r.footer().expect("footer читается");
        assert_eq!(footer.blocks.len(), 1);
        assert_eq!(footer.blocks[0].offset, offset);
        assert_eq!(r.block_offsets().unwrap(), vec![offset]);
    }

    #[test]
    fn reader_falls_back_to_scan_without_footer() {
        let dir = tempfile::tempdir().unwrap();
        let mut w = SegmentWriter::create(dir.path(), header(1_000), 16 * 1024).unwrap();
        let o1 = append(&mut w, 1_000, 2);
        let o2 = append(&mut w, 2_000, 2);
        w.sync().unwrap();
        let path = w.path().to_owned();
        // Закрываем без footer'а — как после обрыва питания.
        w.close_unsealed().unwrap();

        let r = SegmentReader::open(&path).unwrap();
        assert!(!r.is_sealed());
        assert_eq!(r.block_offsets().unwrap(), vec![o1, o2], "скан нашёл блоки");

        let mut buf = Vec::new();
        let next = r.read_block_at(o1, &mut buf).unwrap();
        assert_eq!(next, Some(o2));
        let block = parse_block(&buf).unwrap().unwrap();
        assert_eq!(block.records().count(), 2);
    }

    #[test]
    fn refuses_to_overwrite_existing_segment() {
        let dir = tempfile::tempdir().unwrap();
        let w = SegmentWriter::create(dir.path(), header(1_000), 8 * 1024).unwrap();
        drop(w);
        // Тот же (boot, время) — существующий файл трогать нельзя.
        let err = SegmentWriter::create(dir.path(), header(1_000), 8 * 1024);
        assert!(err.is_err(), "повторное создание обязано провалиться");
    }

    #[test]
    fn fits_reserves_room_for_terminator() {
        let dir = tempfile::tempdir().unwrap();
        let capacity = SegmentHeader::SIZE as u64 + 200;
        let w = SegmentWriter::create(dir.path(), header(0), capacity).unwrap();
        assert!(w.fits(100));
        assert!(
            !w.fits(200 - BlockHeader::SIZE as u64 + 1),
            "место под нулевой терминатор обязано остаться"
        );
    }
}
