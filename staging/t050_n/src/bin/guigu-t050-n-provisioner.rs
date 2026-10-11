use guigu_t050_n::{
    Refusal, Result,
    contract::{NamespaceSeed, NamespaceSpec, UUID},
};
fn run() -> Result<()> {
    let arguments: Vec<_> = std::env::args().collect();
    if arguments.len() != 4 {
        return Err(Refusal::Contract);
    }
    let spec = NamespaceSpec::derive(NamespaceSeed {
        uuid: UUID.into(),
        candidate: arguments[2].clone(),
        approval: arguments[3].clone(),
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    })?;
    match arguments[1].as_str() {
        "--derive" => println!(
            "{}",
            serde_json::to_string(&spec).map_err(|_| Refusal::Contract)?
        ),
        "--prepare" => {
            let packet: guigu_t050_n::contract::SourcePacket =
                guigu_t050_n::secret_channel::read_packet(
                    &guigu_t050_n::secret_channel::retained_input(3)?,
                )?;
            if packet.validate()? != spec {
                return Err(Refusal::Identity);
            }
            let images = (4..=6)
                .enumerate()
                .map(|(i, fd)| {
                    guigu_t050_n::identity::RetainedImage::authenticate(
                        guigu_t050_n::secret_channel::retained_input(fd)?,
                        &packet.images[i],
                        spec.seed().uid,
                        spec.seed().gid,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            let mut owner = guigu_t050_n::BoundNamespace::allocate(&packet)?;
            owner.publish(&packet, [&images[0], &images[1], &images[2]])?;
            println!("t050-n stage=publication-committed operation=not-run");
        }
        "--verify" => {
            let packet: guigu_t050_n::contract::VerificationPacket =
                guigu_t050_n::secret_channel::read_packet(
                    &guigu_t050_n::secret_channel::retained_input(3)?,
                )?;
            if packet.receipt.spec != spec {
                return Err(Refusal::Identity);
            }
            guigu_t050_n::identity::verify_namespace(&packet)?;
            println!("t050-n stage=read-only-authenticated operation=not-run");
        }
        _ => return Err(Refusal::Contract),
    }
    Ok(())
}
fn main() {
    if let Err(error) = run() {
        eprintln!("t050-n-provisioner category={error}");
        std::process::exit(3);
    }
}
