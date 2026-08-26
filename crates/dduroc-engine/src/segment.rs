//! Файл сегмента: создание, дозапись блоков, запечатывание, восстановление.
//!
//! # Почему преаллокация — и почему окном
//!
//! Место под сегмент резервируется заранее (`fallocate`). Это даёт три вещи:
//!
//! 1. **Дешёвый `fdatasync`**: дозапись блока не меняет размер файла, значит
//!    не трогает метаданные инода — синхронизировать нужно только данные.
//! 2. **Отказ по месту заранее**: ENOSPC приходит при резервировании, а не
//!    посреди записи события.
//! 3. **Терминатор скана**: непрописанный хвост заполнен нулями, а нулевой
//!    заголовок блока по формату означает «конец данных» — восстановление
//!    отличает необорванный конец от порчи без дополнительных отметок.
//!
//! Резервировать сразу **весь** сегмент эти три свойства не требуют, а цена
//! у такого решения не хранилищная, а поканальная: её платит каждый канал
//! флота. Двадцать четыре тысячи неймспейсов по восемь мегабайт — это сто
//! девяносто два гигабайта на носителе прибора, из которых записаны единицы
//! килобайт, и столько же нерасписанных экстентов, которые `fdatasync`
//! проталкивает при первом же блоке. Замер на две тысячи каналов: сегмент в
//! восемь мегабайт — закрытие 40 с и синхронизация 2.2 с; тот же флот с
//! сегментом в шестьдесят четыре килобайта — 16 мс и 136 мс.
//!
//! Поэтому резерв — **окно**: сразу берётся `FIRST_EXTENT`, дальше окно
//! растёт восьмыми долями предела, пока не дойдёт до `segment_bytes`. Все
//! три свойства сохраняются внутри окна; изменилось одно — ENOSPC приходит
//! на расширении окна, а не только при создании файла. Расширений на целый
//! сегмент восемь, и каждое проверяется тем же кодом, что и создание.
//!
//! # Порядок операций при обрыве питания
//!
//! - *создание*: fallocate → запись заголовка → fdatasync → fsync каталога.
//!   Обрыв раньше fsync каталога — файла нет; позже — файл валиден и пуст.
//! - *расширение окна*: fallocate до нового размера. Обрыв на любом шаге
//!   оставляет файл либо прежним, либо расширенным нулями — и то, и другое
//!   скан читает как конец данных на прежнем месте.
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

/// Потолок размера footer'а, который читатель согласен прочесть.
///
/// Длина берётся из трейлера, а трейлер сверяется только по сигнатуре — CRC
/// покрывает footer, но проверить его можно лишь прочитав ровно столько,
/// сколько там написано. Одного «влезает в файл» мало: у сегмента в четверть
/// гигабайта это разрешило бы четвертьгигабайтное чтение по одному числу из
/// хвоста, а на armv7 такая аллокация — не абстрактная угроза.
///
/// Восемь мегабайт с запасом покрывают реальность: запись индекса блока стоит
/// единицы байт, и даже сегмент максимального размера, набитый минимальными
/// блоками, столько не набирает.
const MAX_FOOTER: u64 = 8 * 1024 * 1024;

/// Сколько места сегмент занимает сразу при создании — первое окно резерва.
///
/// Равно умолчанию `block_max_bytes`: самый частый случай флота — канал,
/// написавший один блок и замолчавший, — обходится ровно одним `fallocate`
/// и ровно одним экстентом. Дальше окно растёт восьмыми долями предела
/// (см. [`SegmentWriter::reserve`]), то есть за восемь расширений доходит до
/// `segment_bytes`, каким бы тот ни был.
const FIRST_EXTENT: u64 = 64 * 1024;

/// Открытый на запись сегмент.
#[derive(Debug)]
pub struct SegmentWriter {
    file: File,
    path: PathBuf,
    /// Смещение, куда ляжет следующий блок.
    end: u64,
    /// Сколько байт зарезервировано на носителе прямо сейчас — текущее окно.
    capacity: u64,
    /// Предел роста: граница ротации, дальше которой сегмент не растёт.
    /// Именно по нему отвечает [`Self::fits`] — окно до него дотянут по
    /// мере надобности.
    limit: u64,
    header: SegmentHeader,
    /// Номер следующего блока: разрыв нумерации отличает потерянный блок
    /// от порчи при чтении.
    next_seq: u32,
    dirty: bool,
}

impl SegmentWriter {
    /// Создать новый сегмент с пределом роста `limit` байт.
    ///
    /// На носителе он займёт первое окно (`FIRST_EXTENT`) — предел это
    /// граница ротации, а не размер файла.
    pub fn create(dir: &Path, header: SegmentHeader, limit: u64) -> Result<Self> {
        let name = SegmentName::new(header.boot, header.base);
        Self::create_at(&dir.join(name.to_string()), header, limit)
    }

    /// То же по явному пути — для файлов, чьё имя не равно имени сегмента.
    ///
    /// Нужно миграции: она собирает новый сегмент во временном файле рядом со
    /// старым и подменяет его атомарным `rename` только после `fdatasync`.
    /// Писать сразу в конечное имя нельзя — оно занято оригиналом, который
    /// обязан пережить любой обрыв до последнего момента.
    pub fn create_at(path: &Path, header: SegmentHeader, limit: u64) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            // create_new: сегмент с таким именем уже существовать не может —
            // иначе мы затёрли бы чужие данные тем же (boot, время) ключом.
            .create_new(true)
            .mode(fsutil::FILE_MODE)
            .open(path)
            .ctx_path("создание сегмента", path)?;

        // Пол общий для предела и окна: сегмент, в который не влезает даже
        // заголовок с нулевым терминатором, — не сегмент.
        let floor = SegmentHeader::SIZE as u64 + BlockHeader::SIZE as u64;
        let limit = limit.max(floor);
        let capacity = FIRST_EXTENT.clamp(floor, limit);
        if let Err(e) = grow_to(&file, capacity, path) {
            // Файл без места бесполезен и мешает следующей попытке.
            let _ = std::fs::remove_file(path);
            return Err(e);
        }

        file.write_all_at(&header.to_bytes(), 0)
            .ctx_path("запись заголовка", path)?;
        fsutil::sync_data(&file, path)?;
        fsutil::sync_dir(path.parent().unwrap_or(Path::new(".")))?;

        Ok(Self {
            file,
            path: path.to_owned(),
            end: SegmentHeader::SIZE as u64,
            capacity,
            limit,
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
    ///
    /// `limit` — предел роста, до которого сегменту разрешено дописываться.
    /// `None` означает «предела не знаю»: за него берётся текущий размер
    /// файла, то есть сегмент открывается только на дочитывание.
    ///
    /// Места на носителе открытие **не просит**: файл остаётся обрезанным до
    /// конца данных, а окно резерва восстановит первая же запись. Отпущенный
    /// по бездействию сегмент отдал свой хвост именно затем, чтобы его не
    /// держать, — забирать его обратно на пробуждении значило бы платить
    /// `fallocate` на целый сегмент за канал, который, может быть, снова
    /// напишет сто байт.
    pub fn reopen(path: &Path, expect_store: Option<u64>, limit: Option<u64>) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .ctx_path("открытие сегмента", path)?;
        let on_disk = file.metadata().ctx_path("stat", path)?.len();
        let limit = limit.unwrap_or(on_disk).max(on_disk);

        let scan = Scan::run(&file, on_disk, path)?;
        scan.check_store(expect_store, path)?;

        file.set_len(scan.data_end)
            .ctx_path("обрезка повреждённого хвоста", path)?;
        fsutil::sync_data(&file, path)?;

        Ok(Self {
            file,
            path: path.to_owned(),
            end: scan.data_end,
            // Окно схлопнуто до данных: всё, что было за ними, только что
            // отрезано. Расширит его `reserve` при первой записи.
            capacity: scan.data_end,
            limit,
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

    /// Сколько байт ещё поместится до предела роста.
    pub fn remaining(&self) -> u64 {
        self.limit.saturating_sub(self.end)
    }

    /// Сколько байт сегмент занимает на носителе прямо сейчас: заголовок,
    /// данные и нерасписанный хвост текущего окна резерва.
    ///
    /// Это и есть та величина, которой сегмент числится в бюджете класса, —
    /// не предел роста: держать в бюджете место, которое не занято, значит
    /// вытеснять чужую историю ради пустоты.
    pub fn capacity(&self) -> u64 {
        self.capacity
    }

    /// Есть ли незасинхронизированные данные.
    pub fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// Поместится ли блок такого размера — по пределу роста, не по окну.
    ///
    /// Окно до него дотянет [`Self::reserve`]; спрашивать о нём вызывающего
    /// незачем — он решает другой вопрос: не пора ли ротировать сегмент.
    pub fn fits(&self, block_len: u64) -> bool {
        // Оставляем место под нулевой заголовок-терминатор: без него скан
        // упёрся бы в конец файла вместо честного признака конца данных.
        self.remaining() >= block_len + BlockHeader::SIZE as u64
    }

    /// Дотянуть окно резерва до блока такого размера.
    ///
    /// Зовётся из [`Self::append_block`], то есть на каждой записи, и почти
    /// всегда выходит на первой строке: расширений на целый сегмент восемь.
    ///
    /// Шаг — восьмая доля **предела**, а не текущего окна. Доля окна дала бы
    /// сорок расширений на путь от первого экстента до восьми мегабайт, а
    /// удвоение — просьбу о вдвое большем месте, ровно ту, от которой окно и
    /// уводит. Доля предела не зависит ни от того, ни от другого: восемь
    /// расширений на любой `segment_bytes`.
    ///
    /// Выше предела окно шагом не поднимается — только под конкретную нужду:
    /// у миграции предел равен размеру оригинала, а насколько раздуются
    /// записи, знает только цепочка шагов. Просить там «на всякий случай»
    /// значит занять на приборе место, которого может не быть; расти по
    /// факту — значит отказать ровно тогда, когда места действительно не
    /// хватило. Лишний хвост обрежет запечатывание.
    pub fn reserve(&mut self, block_len: u64) -> Result<()> {
        let need = self
            .end
            .saturating_add(block_len)
            .saturating_add(BlockHeader::SIZE as u64);
        if need <= self.capacity {
            return Ok(());
        }
        let step = (self.limit / 8).max(FIRST_EXTENT.min(self.limit));
        let want = need.max(self.capacity.saturating_add(step).min(self.limit));
        grow_to(&self.file, want, &self.path)?;
        self.capacity = want;
        Ok(())
    }

    /// Дописать готовый блок (заголовок + тело). Возвращает его смещение.
    ///
    /// Блок обязан быть собран с номером [`Self::next_seq`].
    pub fn append_block(&mut self, bytes: &[u8]) -> Result<u64> {
        // Резерв здесь, а не у вызывающего: забыть его значило бы писать за
        // границу зарезервированного, то есть получить ENOSPC посреди блока —
        // ровно то, ради чего резерв и делается.
        self.reserve(bytes.len() as u64)?;
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
    #[cfg(test)]
    if fault::refuses(capacity) {
        return Err(Error::NoSpace(path.to_owned()));
    }
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

/// Отказ по месту — только для тестов движка.
///
/// ENOSPC — единственное, ради чего делается преаллокация, и воспроизвести его
/// на настоящей файловой системе, не будучи root'ом, нельзя: нужен свой
/// носитель или своё монтирование. Оставить этот путь непроверенным значило бы
/// проверить всё, кроме того, ради чего всё и построено, — поэтому здесь стоит
/// заглушка ровно на ту точку, где отказ приходит на самом деле.
///
/// Счётчик потоковый: writer живёт в своём потоке, и тест, управляющий его
/// циклом напрямую, не должен ронять запись в соседних тестах.
#[cfg(test)]
pub(crate) mod fault {
    use std::cell::Cell;

    thread_local! {
        static NO_SPACE: Cell<u32> = const { Cell::new(0) };
        static CEILING: Cell<u64> = const { Cell::new(u64::MAX) };
    }

    /// Следующие `n` попыток зарезервировать место отказывают.
    pub(crate) fn no_space_for(n: u32) {
        NO_SPACE.with(|c| c.set(n));
    }

    /// Носитель, на котором свободно ровно `bytes`: попытка зарезервировать
    /// больше отказывает.
    ///
    /// Нужно там, где проверяется не «отказ по месту вообще», а **сколько**
    /// места операция просит: прогон миграции, который берёт вдвое больше
    /// нужного, на настоящем приборе ляжет, а на просторной машине разработчика
    /// пройдёт незамеченным.
    pub(crate) fn free_space(bytes: u64) {
        CEILING.with(|c| c.set(bytes));
    }

    /// Снять потолок, выставленный [`free_space`].
    pub(crate) fn unlimited_space() {
        CEILING.with(|c| c.set(u64::MAX));
    }

    pub(crate) fn refuses(want: u64) -> bool {
        let over = CEILING.with(|c| want > c.get());
        let counted = NO_SPACE.with(|c| {
            let left = c.get();
            c.set(left.saturating_sub(1));
            left > 0
        });
        over || counted
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
    /// Чем кончился обход: концом данных, порчей или обрывом.
    pub stopped_by: ScanEnd,
}

impl Scan {
    /// Хвост был повреждён и отброшен.
    pub fn truncated(&self) -> bool {
        self.stopped_by.is_damage()
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
                stopped_by: end,
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
    pub size_bytes: u64,
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
        size_bytes: size,
        reclaimed: len.saturating_sub(size),
        truncated: scan.truncated(),
    }))
}

/// Сегмент, открытый на чтение.
///
/// Дескриптор держится **только на время чтения** и отпускается
/// [`SegmentReader::detach`]: всё, что стоит обращения к носителю — заголовок,
/// footer, границы данных, — разбирается один раз и живёт в памяти, а
/// открыть файл заново стоит одного вызова. Читатель заводит курсор на каждую
/// пару (неймспейс, канал), и при заявленных двадцати четырёх тысячах
/// неймспейсов постоянный дескриптор у каждого — это десятки тысяч открытых
/// файлов на один запрос.
#[derive(Debug)]
pub struct SegmentReader {
    /// `None` — дескриптор отпущен; чтение откроет файл заново.
    file: Option<File>,
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

        let (footer_bytes, data_end) = Self::probe_tail(&file, path, len)?;

        Ok(Self {
            file: Some(file),
            path: path.to_owned(),
            len,
            header,
            footer_bytes,
            data_end,
        })
    }

    /// Перечитать хвост файла, не разбирая заголовок заново.
    ///
    /// Нужно подписке: сегмент под ней растёт (новые блоки) и меняет длину
    /// (запечатывание обрезает преаллокацию и дописывает footer, отпускание по
    /// бездействию — просто обрезает). Заголовок при этом неизменен по
    /// определению — сегмент не меняет ни запуска, ни базы, — поэтому его
    /// чтение и разбор здесь не повторяются.
    pub fn refresh(&mut self) -> Result<()> {
        self.attach()?;
        let file = self.file.as_ref().expect("после attach дескриптор есть");
        let len = file.metadata().ctx_path("stat", &self.path)?.len();
        let (footer_bytes, data_end) = Self::probe_tail(file, &self.path, len)?;
        self.len = len;
        self.footer_bytes = footer_bytes;
        self.data_end = data_end;
        Ok(())
    }

    /// Двухфазное чтение footer'а: сначала трейлер фиксированного размера, из
    /// него — длина, потом сам footer. Так не читается лишнего.
    ///
    /// Возвращает байты footer'а (если сегмент запечатан) и границу данных.
    fn probe_tail(file: &File, path: &Path, len: u64) -> Result<(Option<Vec<u8>>, u64)> {
        if len < (SegmentHeader::SIZE + Trailer::SIZE) as u64 {
            return Ok((None, len));
        }
        let mut tail = [0u8; Trailer::SIZE];
        file.read_exact_at(&mut tail, len - Trailer::SIZE as u64)
            .ctx_path("чтение трейлера", path)?;
        let Ok(Some(trailer)) = Trailer::parse(&tail) else {
            return Ok((None, len));
        };
        let total = trailer.total_len();
        // Длина из трейлера управляет и размером чтения, и границей данных, а
        // сам трейлер сверяется только по сигнатуре. Поэтому footer
        // принимается лишь после проверки CRC: иначе испорченное поле длины
        // молча отрезало бы часть блоков, выдав усечённый сегмент за целый.
        // Границы две, и обе обязательны. Размер файла отсекает заведомо
        // невозможное, но у сегмента в четверть гигабайта он разрешил бы
        // четвертьгигабайтное чтение по одному числу из хвоста — а трейлер
        // сверен только по сигнатуре, и подобрать её ничего не стоит. Потолок
        // ограничивает footer тем, каким он бывает: индекс блока стоит единицы
        // байт, и даже сегмент, набитый минимальными блоками, не даёт больше
        // нескольких мегабайт.
        if total > len - SegmentHeader::SIZE as u64 || total > MAX_FOOTER {
            return Ok((None, len));
        }
        let mut buf = vec![0u8; total as usize];
        file.read_exact_at(&mut buf, len - total)
            .ctx_path("чтение footer", path)?;
        if matches!(dduroc_format::Footer::parse(&buf), Ok(Some(_))) {
            Ok((Some(buf), len - total))
        } else {
            Ok((None, len))
        }
    }

    /// Отпустить дескриптор, сохранив всё разобранное.
    ///
    /// Разбор от этого не теряется: заголовок, footer и границы данных лежат
    /// в памяти, а чтение блока откроет файл заново. Читателю это позволяет
    /// держать курсор на каждый канал, не держа на каждый по открытому файлу.
    ///
    /// Цена — одно открытие на порцию чтения (не на блок: за одно открытие
    /// курсор дочитывает столько блоков, сколько ему понадобилось). Плата за
    /// это — **окно**: файл, вытесненный ротацией между порциями, откроется
    /// уже не по этому имени. Для живого чтения это то же штатное событие, что
    /// и сегмент, исчезнувший между листингом и открытием: историю убрал сам
    /// движок. Дампу ротация не грозит вовсе.
    pub fn detach(&mut self) {
        self.file = None;
    }

    /// Убедиться, что файл открыт.
    fn attach(&mut self) -> Result<()> {
        if self.file.is_none() {
            self.file =
                Some(File::open(&self.path).ctx_path("повторное открытие сегмента", &self.path)?);
        }
        Ok(())
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

    /// Граница полезных данных: конец блоков, до footer'а.
    ///
    /// Отличается от [`SegmentReader::len`] у обоих концов: у запечатанного
    /// сегмента длина файла включает footer, у незапечатанного — непрописанный
    /// хвост преаллокации. Спрашивают об этом те, кому нужен объём **записей**,
    /// а не файла, — например, миграция, прикидывая ёмкость под переписывание.
    pub fn data_end(&self) -> u64 {
        self.data_end
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
    pub fn read_block_at(&mut self, offset: u64, buf: &mut Vec<u8>) -> Result<Option<u64>> {
        if offset >= self.data_end || self.data_end - offset < BlockHeader::SIZE as u64 {
            return Ok(None);
        }
        self.attach()?;
        let file = self.file.as_ref().expect("после attach дескриптор есть");
        let mut hdr = [0u8; BlockHeader::SIZE];
        file.read_exact_at(&mut hdr, offset)
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
        file.read_exact_at(buf, offset)
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
    pub fn block_offsets(&mut self) -> Result<Vec<u64>> {
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
    pub fn scan_block_offsets(&mut self) -> (Vec<u64>, Option<(u64, String)>) {
        let scan = self.scan_block_offsets_from(Self::first_block_offset(), 0);
        (scan.offsets, scan.stopped)
    }

    /// Продолжить скан блоков с известного места.
    ///
    /// Нужен подписке на поток: сегмент, который пишется прямо сейчас,
    /// перечитывается по мере роста, и начинать каждый раз с начала значило бы
    /// вычитывать весь файл на каждую порцию новых записей — восьмимегабайтный
    /// сегмент вместо десятка килобайт свежего хвоста.
    ///
    /// `expected_seq` продолжает нумерацию блоков: она своя у каждого сегмента
    /// и начинается с нуля, поэтому частичный скан обязан принести её с собой —
    /// иначе продолжение объявляло бы разрыв нумерации на первом же блоке.
    pub fn scan_block_offsets_from(&mut self, start: u64, expected_seq: u32) -> BlockScan {
        let mut scan = BlockScan {
            offsets: Vec::new(),
            end: start,
            next_seq: expected_seq,
            stopped: None,
        };
        let mut buf = Vec::new();
        let mut offset = start;
        loop {
            match self.read_block_at(offset, &mut buf) {
                Ok(Some(next)) => {
                    // Заголовок уже прочитан в буфер — разбор не стоит ни
                    // одного обращения к носителю.
                    if let Ok(Some(header)) = BlockHeader::parse(&buf)
                        && header.seq != scan.next_seq
                    {
                        scan.stopped = Some((
                            offset,
                            format!(
                                "разрыв нумерации блоков: ожидался {}, в файле {}",
                                scan.next_seq, header.seq
                            ),
                        ));
                        return scan;
                    }
                    scan.next_seq = scan.next_seq.saturating_add(1);
                    scan.offsets.push(offset);
                    scan.end = next;
                    offset = next;
                }
                Ok(None) => return scan,
                Err(e) => {
                    scan.stopped = Some((offset, e.to_string()));
                    return scan;
                }
            }
        }
    }
}

/// Итог скана блоков вместе с местом, где его продолжать.
#[derive(Debug, Clone)]
pub struct BlockScan {
    /// Смещения найденных блоков, от старых к новым.
    pub offsets: Vec<u64>,
    /// Смещение, с которого продолжится следующий скан: конец последнего
    /// целого блока либо место обрыва.
    pub end: u64,
    /// Номер, которого следующий скан ждёт у блока на `end`.
    pub next_seq: u32,
    /// Где и почему скан оборвался. `None` — дошёл до конца данных.
    pub stopped: Option<(u64, String)>,
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

        let w2 = SegmentWriter::reopen(&path, None, None).unwrap();
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
        assert_eq!(scan.stopped_by, ScanEnd::ZeroTail);
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
        assert_eq!(
            scan.stopped_by,
            ScanEnd::Corrupt,
            "обрыв распознан как порча"
        );
        assert_eq!(scan.data_end, good_end, "конец данных — до битого блока");

        // Продолжение записи с восстановленной позиции затирает битый хвост.
        let mut w = SegmentWriter::reopen(&path, None, None).unwrap();
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
        //
        // Восстановление добивается этого обрезкой, а не затиранием: файл
        // кончается там же, где кончаются целые данные. Байты за новым блоком
        // приносит уже расширение окна резерва, а `fallocate` выдаёт нули.
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

        let mut w = SegmentWriter::reopen(&path, None, Some(32 * 1024)).unwrap();
        assert_eq!(w.data_end(), good_end);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            good_end,
            "область прежнего длинного блока обязана быть отрезана"
        );

        append(&mut w, 2_000, 1);
        w.sync().unwrap();
        let new_end = w.data_end();
        assert!(
            new_end < long_end,
            "иначе тест пуст: блок обязан быть короче"
        );

        // Хвост, который принесло расширение окна под этот блок, — нулевой.
        let f = File::open(&path).unwrap();
        let grown = std::fs::metadata(&path).unwrap().len();
        assert!(grown > new_end, "окно раздвинулось под запись");
        let mut probe = vec![0u8; (grown - new_end) as usize];
        f.read_exact_at(&mut probe, new_end).unwrap();
        assert!(
            probe.iter().all(|&b| b == 0),
            "хвост окна обязан быть нулевым"
        );

        let scan = Scan::of_path(&path).unwrap();
        assert_eq!(scan.block_count, 2);
        assert_eq!(scan.stopped_by, ScanEnd::ZeroTail, "чистый конец данных");
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
            scan.stopped_by,
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

        let err = SegmentWriter::reopen(&path, Some(0x1111_2222_3333_4444), None).unwrap_err();
        assert!(
            matches!(err, Error::ForeignSegment { .. }),
            "сегмент чужого хранилища обязан отвергаться: {err}"
        );
        // Со своим идентификатором открывается штатно.
        SegmentWriter::reopen(&path, Some(0xAAAA_BBBB_CCCC_DDDD), None).unwrap();
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
            rec.size_bytes,
            "размер файла совпал с объявленным"
        );
        assert!(
            rec.size_bytes < 4 * 1024,
            "преаллокация возвращена: {}",
            rec.size_bytes
        );
        assert_eq!(rec.reclaimed, 32 * 1024 - rec.size_bytes);

        // Footer собран тем же обходом, которым искался конец данных.
        let mut r = SegmentReader::open(&path).unwrap();
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

        let mut r = SegmentReader::open(&path).unwrap();
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

        let mut r = SegmentReader::open(&path).unwrap();
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

        let mut r = SegmentReader::open(&path).unwrap();
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
