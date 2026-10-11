use crate::{Refusal, Result, contract::NamespaceSpec, supervision::ChildCertificate};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Value {
    Text(String),
    Bool(bool),
    Bytes(Vec<u8>),
    Pids(Vec<u32>),
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScopeFacts {
    pub owner: String,
    pub uid: u32,
    pub id: String,
    pub invocation: String,
    pub cgroup: String,
    pub active: String,
    pub pids: Vec<u32>,
    pub transient: bool,
}
pub trait Connection {
    fn unique_owner(&mut self) -> Result<(String, u32)>;
    fn start_scope(&mut self, unit: &str, owned_pids: &[u32]) -> Result<()>;
    fn query_scope(&mut self, unit: &str) -> Result<BTreeMap<String, Value>>;
    fn stop_scope(&mut self, unit: &str) -> Result<()>;
}
pub struct Manager<C> {
    connection: C,
    owner: String,
    uid: u32,
    spec: NamespaceSpec,
    invoked: bool,
}
impl<C: Connection> Manager<C> {
    pub fn authenticate(mut connection: C, spec: NamespaceSpec) -> Result<Self> {
        spec.validate()?;
        let (owner, uid) = connection.unique_owner()?;
        if !owner.starts_with(':') || owner.len() > 64 || uid != spec.seed().uid {
            return Err(Refusal::Manager);
        }
        Ok(Self {
            connection,
            owner,
            uid,
            spec,
            invoked: false,
        })
    }
    fn continuity(&mut self) -> Result<()> {
        if self.connection.unique_owner()? != (self.owner.clone(), self.uid) {
            return Err(Refusal::Manager);
        }
        Ok(())
    }
    pub fn attach(&mut self, children: &[ChildCertificate]) -> Result<ScopeFacts> {
        self.continuity()?;
        if self.invoked
            || children.is_empty()
            || children.len() > 2
            || children
                .iter()
                .any(|c| c.uid != self.uid || c.pid == 0 || c.start_ticks == 0)
        {
            return Err(Refusal::NoAuthority);
        }
        let pids: Vec<_> = children.iter().map(|c| c.pid).collect();
        self.invoked = true;
        self.connection.start_scope(&self.spec.unit(), &pids)?;
        self.continuity()?;
        self.observe(&pids)
    }
    pub fn observe(&mut self, owned_pids: &[u32]) -> Result<ScopeFacts> {
        self.continuity()?;
        let properties = self.connection.query_scope(&self.spec.unit())?;
        self.continuity()?;
        let facts = decode(properties, self.owner.clone(), self.uid)?;
        let mut actual = facts.pids.clone();
        actual.sort_unstable();
        let mut expected = owned_pids.to_vec();
        expected.sort_unstable();
        if facts.id != self.spec.unit()
            || !facts.transient
            || facts.active != "active"
            || !crate::contract::hex(&facts.invocation, 32)
            || actual != expected
            || expected.windows(2).any(|p| p[0] == p[1])
            || !facts.cgroup.starts_with('/')
            || facts.cgroup.contains("..")
            || !facts.cgroup.ends_with(&format!("/{}", self.spec.unit()))
        {
            return Err(Refusal::Manager);
        }
        Ok(facts)
    }
    pub fn retire(&mut self, expected: &ScopeFacts) -> Result<()> {
        let current = self.observe(&expected.pids)?;
        if current != *expected {
            return Err(Refusal::NoAuthority);
        }
        self.continuity()?;
        self.connection.stop_scope(&self.spec.unit())
    }
}
pub fn decode(mut values: BTreeMap<String, Value>, owner: String, uid: u32) -> Result<ScopeFacts> {
    if values.contains_key("MainPID") {
        return Err(Refusal::Manager);
    }
    let id = text(&mut values, "Id")?;
    let cgroup = text(&mut values, "ControlGroup")?;
    let active = text(&mut values, "ActiveState")?;
    let Some(Value::Bool(transient)) = values.remove("Transient") else {
        return Err(Refusal::Manager);
    };
    let Some(Value::Bytes(bytes)) = values.remove("InvocationID") else {
        return Err(Refusal::Manager);
    };
    if bytes.len() != 16 || bytes.iter().all(|v| *v == 0) {
        return Err(Refusal::Manager);
    }
    let invocation = bytes.iter().map(|b| format!("{b:02x}")).collect();
    let Some(Value::Pids(pids)) = values.remove("PIDs") else {
        return Err(Refusal::Manager);
    };
    if pids.len() > 2 {
        return Err(Refusal::Manager);
    }
    Ok(ScopeFacts {
        owner,
        uid,
        id,
        invocation,
        cgroup,
        active,
        pids,
        transient,
    })
}
fn text(values: &mut BTreeMap<String, Value>, name: &str) -> Result<String> {
    match values.remove(name) {
        Some(Value::Text(v)) if !v.is_empty() && v.len() <= 512 => Ok(v),
        _ => Err(Refusal::Manager),
    }
}

/// Retained sd-bus DSO ABI ownership, separate from typed manager admission.
pub(crate) mod abi {
    use crate::{Refusal, Result};
    use std::ffi::{CStr, c_char, c_int, c_void};
    type Call = unsafe extern "C" fn(
        *mut c_void,
        *const c_char,
        *const c_char,
        *const c_char,
        *const c_char,
        *mut c_void,
        *mut *mut c_void,
        *const c_char,
        ...
    ) -> c_int;
    type Read = unsafe extern "C" fn(*mut c_void, c_char, *mut c_void) -> c_int;
    type Enter = unsafe extern "C" fn(*mut c_void, c_char, *const c_char) -> c_int;
    type Exit = unsafe extern "C" fn(*mut c_void) -> c_int;
    type Unref = unsafe extern "C" fn(*mut c_void) -> *mut c_void;
    type Append = unsafe extern "C" fn(*mut c_void, *const c_char, ...) -> c_int;
    type New = unsafe extern "C" fn(*mut *mut c_void) -> c_int;
    type Address = unsafe extern "C" fn(*mut c_void, *const c_char) -> c_int;
    type Client = unsafe extern "C" fn(*mut c_void, c_int) -> c_int;
    type Timeout = unsafe extern "C" fn(*mut c_void, u64) -> c_int;
    type NewMethod = unsafe extern "C" fn(
        *mut c_void,
        *mut *mut c_void,
        *const c_char,
        *const c_char,
        *const c_char,
        *const c_char,
    ) -> c_int;
    type BusCall =
        unsafe extern "C" fn(*mut c_void, *mut c_void, u64, *mut c_void, *mut *mut c_void) -> c_int;
    pub(crate) struct Api {
        pub(crate) library: *mut c_void,
        pub(crate) call: Call,
        pub(crate) read: Read,
        pub(crate) enter: Enter,
        pub(crate) exit: Exit,
        pub(crate) message_unref: Unref,
        pub(crate) bus_unref: Unref,
        pub(crate) append: Append,
        pub(crate) new: New,
        pub(crate) address: Address,
        pub(crate) client: Client,
        pub(crate) start: Exit,
        pub(crate) timeout: Timeout,
        pub(crate) new_method: NewMethod,
        pub(crate) open: Enter,
        pub(crate) close: Exit,
        pub(crate) bus_call: BusCall,
    }
    impl Api {
        pub(crate) fn load() -> Result<Self> {
            let library = unsafe {
                libc::dlopen(
                    c"libsystemd.so.0".as_ptr(),
                    libc::RTLD_NOW | libc::RTLD_LOCAL,
                )
            };
            if library.is_null() {
                return Err(Refusal::Unsupported);
            }
            unsafe fn symbol<T: Copy>(library: *mut c_void, name: &CStr) -> Result<T> {
                let pointer = unsafe { libc::dlsym(library, name.as_ptr()) };
                if pointer.is_null()
                    || std::mem::size_of::<T>() != std::mem::size_of::<*mut c_void>()
                {
                    return Err(Refusal::Unsupported);
                }
                Ok(unsafe { std::mem::transmute_copy(&pointer) })
            }
            // All signatures are the published sd-bus C ABI. The owning Api keeps
            // the DSO alive until after every message and the bus have been released.
            let result = (|| unsafe {
                Ok(Self {
                    library,
                    call: symbol(library, c"sd_bus_call_method")?,
                    read: symbol(library, c"sd_bus_message_read_basic")?,
                    enter: symbol(library, c"sd_bus_message_enter_container")?,
                    exit: symbol(library, c"sd_bus_message_exit_container")?,
                    message_unref: symbol(library, c"sd_bus_message_unref")?,
                    bus_unref: symbol(library, c"sd_bus_unref")?,
                    append: symbol(library, c"sd_bus_message_append")?,
                    new: symbol(library, c"sd_bus_new")?,
                    address: symbol(library, c"sd_bus_set_address")?,
                    client: symbol(library, c"sd_bus_set_bus_client")?,
                    start: symbol(library, c"sd_bus_start")?,
                    timeout: symbol(library, c"sd_bus_set_method_call_timeout")?,
                    new_method: symbol(library, c"sd_bus_message_new_method_call")?,
                    open: symbol(library, c"sd_bus_message_open_container")?,
                    close: symbol(library, c"sd_bus_message_close_container")?,
                    bus_call: symbol(library, c"sd_bus_call")?,
                })
            })();
            if result.is_err() {
                unsafe {
                    libc::dlclose(library);
                }
            }
            result
        }
    }
    impl Drop for Api {
        fn drop(&mut self) {
            unsafe {
                libc::dlclose(self.library);
            }
        }
    }
    pub(crate) struct Message<'a> {
        pub(crate) pointer: *mut c_void,
        pub(crate) api: &'a Api,
    }
    impl Drop for Message<'_> {
        fn drop(&mut self) {
            unsafe {
                (self.api.message_unref)(self.pointer);
            }
        }
    }
}
