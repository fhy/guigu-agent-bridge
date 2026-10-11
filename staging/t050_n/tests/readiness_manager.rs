use guigu_t050_n::{
    Refusal,
    contract::{NamespaceSeed, NamespaceSpec, UUID},
    identity::{FileId, ObjectId},
    manager::{Connection, Manager, Value, decode},
    readiness::{ListenerFact, socket_fact, verify},
    supervision::ChildCertificate,
};
use std::{cell::Cell, collections::BTreeMap, rc::Rc};
fn spec() -> NamespaceSpec {
    NamespaceSpec::derive(NamespaceSeed {
        uuid: UUID.into(),
        candidate: "a".repeat(40),
        approval: "b".repeat(64),
        uid: unsafe { libc::geteuid() },
        gid: unsafe { libc::getegid() },
    })
    .unwrap()
}
fn child() -> ChildCertificate {
    ChildCertificate {
        pid: 42,
        uid: spec().seed().uid,
        start_ticks: 101,
        image: FileId {
            object: ObjectId {
                dev: 1,
                ino: 2,
                uid: spec().seed().uid,
                gid: spec().seed().gid,
                mode: 0o700,
                kind: libc::S_IFREG,
            },
            len: 7,
            nlink: 1,
            sha256: "a".repeat(64),
        },
        nonce: "b".repeat(64),
        argv_digest: "c".repeat(64),
        namespace: None,
    }
}
fn props() -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("Id".into(), Value::Text(spec().unit())),
        (
            "ControlGroup".into(),
            Value::Text(format!("/fixture/{}", spec().unit())),
        ),
        ("ActiveState".into(), Value::Text("active".into())),
        ("Transient".into(), Value::Bool(true)),
        ("InvocationID".into(), Value::Bytes(vec![1; 16])),
        ("PIDs".into(), Value::Pids(vec![42])),
    ])
}
struct Fake {
    owner: Rc<Cell<u32>>,
    starts: Rc<Cell<u32>>,
    stops: Rc<Cell<u32>>,
    values: Rc<std::cell::RefCell<BTreeMap<String, Value>>>,
}
impl Connection for Fake {
    fn unique_owner(&mut self) -> guigu_t050_n::Result<(String, u32)> {
        Ok((format!(":1.{}", self.owner.get()), spec().seed().uid))
    }
    fn start_scope(&mut self, unit: &str, pids: &[u32]) -> guigu_t050_n::Result<()> {
        assert_eq!(unit, spec().unit());
        assert_eq!(pids, [42]);
        self.starts.set(self.starts.get() + 1);
        Ok(())
    }
    fn query_scope(&mut self, unit: &str) -> guigu_t050_n::Result<BTreeMap<String, Value>> {
        assert_eq!(unit, spec().unit());
        Ok(self.values.borrow().clone())
    }
    fn stop_scope(&mut self, unit: &str) -> guigu_t050_n::Result<()> {
        assert_eq!(unit, spec().unit());
        self.stops.set(self.stops.get() + 1);
        Ok(())
    }
}
#[test]
fn unit_manager_invocation_drift() {
    let owner = Rc::new(Cell::new(1));
    let starts = Rc::new(Cell::new(0));
    let stops = Rc::new(Cell::new(0));
    let values = Rc::new(std::cell::RefCell::new(props()));
    let fake = Fake {
        owner: owner.clone(),
        starts: starts.clone(),
        stops: stops.clone(),
        values: values.clone(),
    };
    let mut manager = Manager::authenticate(fake, spec()).unwrap();
    let facts = manager.attach(&[child()]).unwrap();
    assert_eq!(starts.get(), 1);
    assert!(manager.attach(&[child()]).is_err());
    assert_eq!(starts.get(), 1);
    values
        .borrow_mut()
        .insert("InvocationID".into(), Value::Bytes(vec![2; 16]));
    assert_eq!(manager.retire(&facts), Err(Refusal::NoAuthority));
    assert_eq!(stops.get(), 0);
    values
        .borrow_mut()
        .insert("InvocationID".into(), Value::Bytes(vec![1; 16]));
    owner.set(2);
    assert_eq!(manager.observe(&[42]), Err(Refusal::Manager));
    assert_eq!(stops.get(), 0);
}
#[test]
fn typed_property_missing_wrong_type() {
    for key in [
        "Id",
        "ControlGroup",
        "ActiveState",
        "Transient",
        "InvocationID",
        "PIDs",
    ] {
        let mut p = props();
        p.remove(key);
        assert!(decode(p, ":1.1".into(), spec().seed().uid).is_err());
        let mut p = props();
        p.insert(key.into(), Value::Text("wrong-type".into()));
        if ["Id", "ControlGroup", "ActiveState"].contains(&key) {
            p.insert(key.into(), Value::Bool(false));
        }
        assert!(decode(p, ":1.1".into(), spec().seed().uid).is_err());
    }
    let mut p = props();
    p.insert("MainPID".into(), Value::Pids(vec![]));
    assert!(decode(p, ":1.1".into(), spec().seed().uid).is_err());
}
#[test]
fn foreign_or_missing_listener() {
    let child = child();
    let original = vec![
        ListenerFact {
            pid: 42,
            ticks: 101,
            inode: 200,
            port: 9391,
            loopback: true,
            listening: true,
        },
        ListenerFact {
            pid: 42,
            ticks: 101,
            inode: 201,
            port: 9392,
            loopback: true,
            listening: true,
        },
    ];
    verify(&spec(), &child, &original).unwrap();
    assert!(verify(&spec(), &child, &original[..1]).is_err());
    for field in 0..6 {
        let mut bad = original.clone();
        match field {
            0 => bad[0].pid += 1,
            1 => bad[0].ticks += 1,
            2 => bad[0].port = 9191,
            3 => bad[0].inode = 0,
            4 => bad[0].loopback = false,
            _ => bad[0].listening = false,
        }
        assert!(verify(&spec(), &child, &bad).is_err());
    }
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let descriptor: std::os::fd::OwnedFd = listener.into();
    let file = std::fs::File::from(descriptor);
    let fact = socket_fact(&file, std::process::id(), 1).unwrap();
    assert_eq!(fact.port, port);
    assert!(fact.loopback && fact.listening);
}
