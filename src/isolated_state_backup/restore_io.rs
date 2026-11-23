use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
};

pub(crate) fn copy(source: &mut File, destination: &mut File) -> Result<(), ()> {
    source.seek(SeekFrom::Start(0)).map_err(|_| ())?;
    let mut buffer = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let count = source.read(&mut buffer).map_err(|_| ())?;
        if count == 0 {
            break;
        }
        total = total.checked_add(count as u64).ok_or(())?;
        if total > super::MAX_FILE {
            return Err(());
        }
        destination.write_all(&buffer[..count]).map_err(|_| ())?;
    }
    Ok(())
}
