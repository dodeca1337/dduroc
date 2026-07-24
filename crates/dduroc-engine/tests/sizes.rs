//! Размеры структур горячего пути.
//!
//! Очереди writer'а выделяются целиком при открытии хранилища, поэтому размер
//! [`Staged`] напрямую задаёт расход памяти: при значениях по умолчанию это
//! 9216 слотов. На armv7 с сотнями мегабайт памяти рост записи на десяток
//! байт заметен, а заметить его иначе нечем — компилятор о нём не скажет.
//!
//! Тест не о «правильном» размере, а о том, чтобы изменение размера было
//! **осознанным**: если он вырос, надо посмотреть, стоило ли того новое поле.

use dduroc_engine::staged::{INLINE_PAYLOAD, OwnedValue, Payload, Staged, StagedRecord};

#[test]
fn hot_path_structs_do_not_grow_unnoticed() {
    let staged = std::mem::size_of::<Staged>();
    let record = std::mem::size_of::<StagedRecord>();
    let value = std::mem::size_of::<OwnedValue>();
    let payload = std::mem::size_of::<Payload>();

    // Ориентиры сняты на x86-64; на armv7 указатели вдвое короче, поэтому
    // проверяется потолок, а не точное равенство.
    assert!(
        staged <= 72,
        "Staged вырос до {staged} байт: очередь по умолчанию подорожала на \
         {} КБ. Новое поле того стоит?",
        staged.saturating_sub(72) * 9216 / 1024
    );
    assert!(record <= 56, "StagedRecord = {record}");
    assert!(value <= 48, "OwnedValue = {value}");

    // Payload держит inline-буфер: событие с парой чисел не должно уходить
    // в кучу, иначе горячий путь аллоцирует на каждый вызов.
    assert!(
        payload > INLINE_PAYLOAD,
        "Payload = {payload}, inline-ёмкость = {INLINE_PAYLOAD}"
    );

    // Размер задаёт вариант Message со своим payload'ом, а не отсчёт
    // телеметрии: экономить на отсчёте бессмысленно, пока Message больше.
    assert!(
        record >= payload,
        "StagedRecord ({record}) меньше Payload ({payload}) — раскладка \
         изменилась, ориентиры пора пересчитать"
    );
}
