use std::io::{self, Write};
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub(crate) struct Capture(Arc<Mutex<Vec<u8>>>);

pub(crate) struct Writer(Arc<Mutex<Vec<u8>>>);

impl Capture {
    pub(crate) fn subscriber(&self) -> impl tracing::Subscriber + Send + Sync + use<> {
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_env_filter("off,guigu_agent_bridge=debug")
            .with_writer(self.clone())
            .finish()
    }

    pub(crate) fn output(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }
}

impl<'writer> tracing_subscriber::fmt::MakeWriter<'writer> for Capture {
    type Writer = Writer;

    fn make_writer(&'writer self) -> Writer {
        Writer(self.0.clone())
    }
}

impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn secondary_finalization_error_retains_both_safe_categories() {
    let task_id = crate::models::TaskId::generate();
    let error = crate::bus::WorkerError::DeliveryFinalization {
        task_id,
        seq: 2,
        delivery_class: "not-accepted",
        finalization_class: "recovery-needed",
    };
    let output = error.to_string();
    assert!(output.contains(&task_id.to_string()));
    assert!(output.contains("not-accepted/recovery-needed"));
    assert_eq!(error.category(), "delivery-finalization");
}
