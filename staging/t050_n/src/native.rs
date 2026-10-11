//! Bounded single-connection sd-bus backend. Never systemctl, LoadUnit or enumeration.
use crate::{
    Refusal, Result,
    manager::{Connection, Value},
};
use std::{
    collections::BTreeMap,
    ffi::{CStr, CString, c_char, c_int, c_void},
    ptr,
};

use crate::manager::abi::{Api, Message};

pub struct NativeConnection {
    bus: *mut c_void,
    api: Api,
    uid: u32,
    unit: String,
}
impl NativeConnection {
    pub fn connect(spec: &crate::contract::NamespaceSpec) -> Result<Self> {
        spec.validate()?;
        let api = Api::load()?;
        let mut bus = ptr::null_mut();
        checked(unsafe { (api.new)(&mut bus) })?;
        let address = CString::new(format!("unix:path=/run/user/{}/bus", spec.seed().uid))
            .map_err(|_| Refusal::Manager)?;
        let result = (|| {
            checked(unsafe { (api.address)(bus, address.as_ptr()) })?;
            checked(unsafe { (api.client)(bus, 1) })?;
            checked(unsafe { (api.timeout)(bus, 10_000_000) })?;
            checked(unsafe { (api.start)(bus) })
        })();
        if result.is_err() {
            unsafe {
                (api.bus_unref)(bus);
            }
            return Err(Refusal::Manager);
        }
        Ok(Self {
            bus,
            api,
            uid: spec.seed().uid,
            unit: spec.unit(),
        })
    }
    fn call_string(
        &self,
        destination: &CStr,
        path: &CStr,
        interface: &CStr,
        method: &CStr,
        argument: &CStr,
    ) -> Result<Message<'_>> {
        let mut reply = ptr::null_mut();
        checked(unsafe {
            (self.api.call)(
                self.bus,
                destination.as_ptr(),
                path.as_ptr(),
                interface.as_ptr(),
                method.as_ptr(),
                ptr::null_mut(),
                &mut reply,
                c"s".as_ptr(),
                argument.as_ptr(),
            )
        })?;
        Ok(Message {
            pointer: reply,
            api: &self.api,
        })
    }
    fn unit_path(&self, unit: &str) -> Result<CString> {
        if unit != self.unit {
            return Err(Refusal::NoAuthority);
        }
        let unit = CString::new(unit).map_err(|_| Refusal::Manager)?;
        let reply = self.call_string(
            c"org.freedesktop.systemd1",
            c"/org/freedesktop/systemd1",
            c"org.freedesktop.systemd1.Manager",
            c"GetUnit",
            &unit,
        )?;
        let value = read_text(&reply, b'o')?;
        if !value.starts_with("/org/freedesktop/systemd1/unit/") {
            return Err(Refusal::Manager);
        }
        CString::new(value).map_err(|_| Refusal::Manager)
    }
    fn property(&self, path: &CStr, property: &CStr, kind: u8) -> Result<Value> {
        let mut reply = ptr::null_mut();
        checked(unsafe {
            (self.api.call)(
                self.bus,
                c"org.freedesktop.systemd1".as_ptr(),
                path.as_ptr(),
                c"org.freedesktop.DBus.Properties".as_ptr(),
                c"Get".as_ptr(),
                ptr::null_mut(),
                &mut reply,
                c"ss".as_ptr(),
                c"org.freedesktop.systemd1.Unit".as_ptr(),
                property.as_ptr(),
            )
        })?;
        let message = Message {
            pointer: reply,
            api: &self.api,
        };
        let signature = CString::new(vec![kind]).map_err(|_| Refusal::Manager)?;
        entered(unsafe { (self.api.enter)(reply, b'v' as c_char, signature.as_ptr()) })?;
        let value = if kind == b's' {
            Value::Text(read_text(&message, b's')?)
        } else {
            let mut boolean: c_int = 0;
            entered(unsafe {
                (self.api.read)(reply, b'b' as c_char, (&mut boolean as *mut c_int).cast())
            })?;
            if ![0, 1].contains(&boolean) {
                return Err(Refusal::Manager);
            }
            Value::Bool(boolean == 1)
        };
        checked(unsafe { (self.api.exit)(reply) })?;
        Ok(value)
    }
    fn invocation(&self, path: &CStr) -> Result<Vec<u8>> {
        let mut reply = ptr::null_mut();
        checked(unsafe {
            (self.api.call)(
                self.bus,
                c"org.freedesktop.systemd1".as_ptr(),
                path.as_ptr(),
                c"org.freedesktop.DBus.Properties".as_ptr(),
                c"Get".as_ptr(),
                ptr::null_mut(),
                &mut reply,
                c"ss".as_ptr(),
                c"org.freedesktop.systemd1.Unit".as_ptr(),
                c"InvocationID".as_ptr(),
            )
        })?;
        let _message = Message {
            pointer: reply,
            api: &self.api,
        };
        entered(unsafe { (self.api.enter)(reply, b'v' as c_char, c"ay".as_ptr()) })?;
        entered(unsafe { (self.api.enter)(reply, b'a' as c_char, c"y".as_ptr()) })?;
        let mut bytes = Vec::new();
        loop {
            let mut byte = 0u8;
            let count =
                unsafe { (self.api.read)(reply, b'y' as c_char, (&mut byte as *mut u8).cast()) };
            if count == 0 {
                break;
            }
            entered(count)?;
            bytes.push(byte);
            if bytes.len() > 16 {
                return Err(Refusal::Manager);
            }
        }
        if bytes.len() != 16 {
            return Err(Refusal::Manager);
        }
        Ok(bytes)
    }
}
impl Connection for NativeConnection {
    fn unique_owner(&mut self) -> Result<(String, u32)> {
        let reply = self.call_string(
            c"org.freedesktop.DBus",
            c"/org/freedesktop/DBus",
            c"org.freedesktop.DBus",
            c"GetNameOwner",
            c"org.freedesktop.systemd1",
        )?;
        let owner = read_text(&reply, b's')?;
        let name = CString::new(owner.clone()).map_err(|_| Refusal::Manager)?;
        let reply = self.call_string(
            c"org.freedesktop.DBus",
            c"/org/freedesktop/DBus",
            c"org.freedesktop.DBus",
            c"GetConnectionUnixUser",
            &name,
        )?;
        let mut uid = 0u32;
        entered(unsafe {
            (self.api.read)(reply.pointer, b'u' as c_char, (&mut uid as *mut u32).cast())
        })?;
        if uid != self.uid {
            return Err(Refusal::Manager);
        }
        Ok((owner, uid))
    }
    fn start_scope(&mut self, unit: &str, pids: &[u32]) -> Result<()> {
        if unit != self.unit || pids.is_empty() || pids.len() > 2 || pids.contains(&0) {
            return Err(Refusal::NoAuthority);
        }
        let unit = CString::new(unit).map_err(|_| Refusal::Manager)?;
        let mut pointer = ptr::null_mut();
        checked(unsafe {
            (self.api.new_method)(
                self.bus,
                &mut pointer,
                c"org.freedesktop.systemd1".as_ptr(),
                c"/org/freedesktop/systemd1".as_ptr(),
                c"org.freedesktop.systemd1.Manager".as_ptr(),
                c"StartTransientUnit".as_ptr(),
            )
        })?;
        let message = Message {
            pointer,
            api: &self.api,
        };
        checked(unsafe {
            (self.api.append)(pointer, c"ss".as_ptr(), unit.as_ptr(), c"fail".as_ptr())
        })?;
        checked(unsafe { (self.api.open)(pointer, b'a' as c_char, c"(sv)".as_ptr()) })?;
        checked(unsafe { (self.api.open)(pointer, b'r' as c_char, c"sv".as_ptr()) })?;
        checked(unsafe { (self.api.append)(pointer, c"s".as_ptr(), c"PIDs".as_ptr()) })?;
        checked(unsafe { (self.api.open)(pointer, b'v' as c_char, c"au".as_ptr()) })?;
        checked(unsafe { (self.api.open)(pointer, b'a' as c_char, c"u".as_ptr()) })?;
        for pid in pids {
            checked(unsafe { (self.api.append)(pointer, c"u".as_ptr(), *pid) })?;
        }
        for _ in 0..4 {
            checked(unsafe { (self.api.close)(pointer) })?;
        }
        checked(unsafe { (self.api.open)(pointer, b'a' as c_char, c"(sa(sv))".as_ptr()) })?;
        checked(unsafe { (self.api.close)(pointer) })?;
        let mut reply = ptr::null_mut();
        checked(unsafe {
            (self.api.bus_call)(
                self.bus,
                message.pointer,
                10_000_000,
                ptr::null_mut(),
                &mut reply,
            )
        })?;
        let _reply = Message {
            pointer: reply,
            api: &self.api,
        };
        Ok(())
    }
    fn query_scope(&mut self, unit: &str) -> Result<BTreeMap<String, Value>> {
        let path = self.unit_path(unit)?;
        let mut properties = BTreeMap::new();
        for name in ["Id", "ControlGroup", "ActiveState"] {
            let property = CString::new(name).map_err(|_| Refusal::Manager)?;
            properties.insert(name.into(), self.property(&path, &property, b's')?);
        }
        properties.insert(
            "Transient".into(),
            self.property(&path, c"Transient", b'b')?,
        );
        properties.insert("InvocationID".into(), Value::Bytes(self.invocation(&path)?));
        let Some(Value::Text(id)) = properties.get("Id") else {
            return Err(Refusal::Manager);
        };
        let Some(Value::Text(cgroup)) = properties.get("ControlGroup") else {
            return Err(Refusal::Manager);
        };
        if id != unit
            || !cgroup.starts_with(&format!("/user.slice/user-{}.slice/", self.uid))
            || cgroup.contains("..")
            || cgroup.contains("//")
            || !cgroup.ends_with(&format!("/{unit}"))
        {
            return Err(Refusal::Manager);
        }
        let text = std::fs::read_to_string(format!("/sys/fs/cgroup{cgroup}/cgroup.procs"))
            .map_err(|_| Refusal::Manager)?;
        if text.len() > 128 {
            return Err(Refusal::Manager);
        }
        let pids = text
            .lines()
            .map(|v| v.parse::<u32>().map_err(|_| Refusal::Manager))
            .collect::<Result<Vec<_>>>()?;
        properties.insert("PIDs".into(), Value::Pids(pids));
        Ok(properties)
    }
    fn stop_scope(&mut self, unit: &str) -> Result<()> {
        if unit != self.unit {
            return Err(Refusal::NoAuthority);
        }
        let unit = CString::new(unit).map_err(|_| Refusal::Manager)?;
        let mut reply = ptr::null_mut();
        checked(unsafe {
            (self.api.call)(
                self.bus,
                c"org.freedesktop.systemd1".as_ptr(),
                c"/org/freedesktop/systemd1".as_ptr(),
                c"org.freedesktop.systemd1.Manager".as_ptr(),
                c"StopUnit".as_ptr(),
                ptr::null_mut(),
                &mut reply,
                c"ss".as_ptr(),
                unit.as_ptr(),
                c"fail".as_ptr(),
            )
        })?;
        let _reply = Message {
            pointer: reply,
            api: &self.api,
        };
        Ok(())
    }
}
impl Drop for NativeConnection {
    fn drop(&mut self) {
        unsafe {
            (self.api.bus_unref)(self.bus);
        }
    }
}
fn checked(value: c_int) -> Result<()> {
    if value < 0 {
        Err(Refusal::Manager)
    } else {
        Ok(())
    }
}
fn entered(value: c_int) -> Result<()> {
    if value <= 0 {
        Err(Refusal::Manager)
    } else {
        Ok(())
    }
}
fn read_text(message: &Message<'_>, kind: u8) -> Result<String> {
    let mut pointer: *const c_char = ptr::null();
    entered(unsafe {
        (message.api.read)(
            message.pointer,
            kind as c_char,
            (&mut pointer as *mut *const c_char).cast(),
        )
    })?;
    if pointer.is_null() {
        return Err(Refusal::Manager);
    }
    let bytes = unsafe { CStr::from_ptr(pointer) }.to_bytes();
    if bytes.is_empty() || bytes.len() > 512 {
        return Err(Refusal::Manager);
    }
    String::from_utf8(bytes.to_vec()).map_err(|_| Refusal::Manager)
}
