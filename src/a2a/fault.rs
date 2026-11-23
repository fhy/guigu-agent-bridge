#[cfg(feature = "test-support")]
use std::sync::Arc;

use super::A2aError;
#[cfg(feature = "test-support")]
use super::{IngressCause, IngressStage, Retryability};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FaultPoint {
    BeforeReservation,
    AfterReservation,
    BeforeContent,
    AfterContent,
    BeforePrerequisites,
    AfterPrerequisites,
    BeforeAdmission,
    AfterAdmission,
    AdmissionBusy,
    BeforeBinding,
    AfterBinding,
    BeforeSnapshot,
}

pub trait IngressFaultInjector: Send + Sync {
    fn inject(&self, point: FaultPoint) -> Result<(), A2aError>;
}

#[derive(Debug, Default)]
pub struct NoFault;

impl IngressFaultInjector for NoFault {
    fn inject(&self, _point: FaultPoint) -> Result<(), A2aError> {
        Ok(())
    }
}

#[cfg(feature = "test-support")]
#[derive(Debug, Clone)]
pub struct FaultPlan(Arc<std::sync::Mutex<Option<FaultPoint>>>);

#[cfg(feature = "test-support")]
impl FaultPlan {
    pub fn once(point: FaultPoint) -> Self {
        Self(Arc::new(std::sync::Mutex::new(Some(point))))
    }
}

#[cfg(feature = "test-support")]
impl IngressFaultInjector for FaultPlan {
    fn inject(&self, point: FaultPoint) -> Result<(), A2aError> {
        let mut pending = self.0.lock().unwrap_or_else(|value| value.into_inner());
        if *pending != Some(point) {
            return Ok(());
        }
        *pending = None;
        let (stage, cause, retryability) = match point {
            FaultPoint::AdmissionBusy => (
                IngressStage::Admission,
                IngressCause::AdmissionBusy,
                Retryability::Backoff,
            ),
            FaultPoint::BeforeBinding | FaultPoint::AfterBinding => (
                IngressStage::Binding,
                IngressCause::BindingIncomplete,
                Retryability::Never,
            ),
            FaultPoint::BeforeSnapshot => (
                IngressStage::Snapshot,
                IngressCause::Internal,
                Retryability::Never,
            ),
            FaultPoint::BeforeReservation | FaultPoint::AfterReservation => (
                IngressStage::IdempotencyReserve,
                IngressCause::Storage,
                Retryability::SameRequest,
            ),
            FaultPoint::BeforeContent | FaultPoint::AfterContent => (
                IngressStage::ContentPersist,
                IngressCause::Storage,
                Retryability::SameRequest,
            ),
            FaultPoint::BeforePrerequisites | FaultPoint::AfterPrerequisites => (
                IngressStage::Conversation,
                IngressCause::Storage,
                Retryability::SameRequest,
            ),
            FaultPoint::BeforeAdmission | FaultPoint::AfterAdmission => (
                IngressStage::Admission,
                IngressCause::Storage,
                Retryability::SameRequest,
            ),
        };
        Err(A2aError::ingress(stage, cause, retryability))
    }
}
