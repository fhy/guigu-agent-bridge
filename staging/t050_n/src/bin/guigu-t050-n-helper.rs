use guigu_t050_n::{Refusal, Result, secret_channel};
fn run() -> Result<()> {
    let spec = secret_channel::worker_spec(&std::env::args().collect::<Vec<_>>())?;
    let (mut channel, _provider, owner) = secret_channel::worker_channels()?;
    secret_channel::peer(&channel, owner, spec.seed().uid)?;
    use std::io::{Read, Write};
    use std::os::fd::AsRawFd;
    let timeout = libc::timeval {
        tv_sec: 10,
        tv_usec: 0,
    };
    if unsafe {
        libc::setsockopt(
            channel.as_raw_fd(),
            libc::SOL_SOCKET,
            libc::SO_RCVTIMEO,
            (&timeout as *const libc::timeval).cast(),
            std::mem::size_of_val(&timeout) as u32,
        )
    } != 0
    {
        return Err(Refusal::Channel);
    }
    let mut length = [0u8; 4];
    channel
        .read_exact(&mut length)
        .map_err(|_| Refusal::Channel)?;
    let length = u32::from_le_bytes(length) as usize;
    if length == 0 || length > 32768 {
        return Err(Refusal::Length);
    }
    let mut bytes = vec![0u8; length];
    channel
        .read_exact(&mut bytes)
        .map_err(|_| Refusal::Channel)?;
    let packet: guigu_t050_n::contract::VerificationPacket =
        serde_json::from_slice(&bytes).map_err(|_| Refusal::Contract)?;
    if packet.receipt.spec != spec {
        return Err(Refusal::Identity);
    }
    guigu_t050_n::identity::verify_namespace(&packet)?;
    channel
        .write_all(b"authenticated-files;NoOperationAuthority\n")
        .map_err(|_| Refusal::Channel)
}
fn main() {
    if let Err(error) = run() {
        eprintln!("t050-n-helper category={error}");
        std::process::exit(3);
    }
}
