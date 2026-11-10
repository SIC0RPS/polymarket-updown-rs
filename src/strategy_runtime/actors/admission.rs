use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct CentralCycleAdmissionState {
    remaining_new_exposure: usize,
    next_reservation_id: u64,
    reservations: BTreeSet<u64>,
}

#[derive(Clone)]
pub struct CentralCycleAdmission {
    inner: Arc<Mutex<CentralCycleAdmissionState>>,
}

#[derive(Clone, Debug)]
pub struct CentralAdmissionReservation {
    inner: Arc<Mutex<CentralCycleAdmissionState>>,
    id: u64,
}

impl CentralAdmissionReservation {
    pub fn release(&self) {
        let mut state = self.inner.lock().expect("central admission lock poisoned");
        if state.reservations.remove(&self.id) {
            state.remaining_new_exposure = state.remaining_new_exposure.saturating_add(1);
        }
    }
}

impl CentralCycleAdmission {
    pub(super) fn new(max_orders_per_cycle: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CentralCycleAdmissionState {
                remaining_new_exposure: max_orders_per_cycle,
                next_reservation_id: 0,
                reservations: BTreeSet::new(),
            })),
        }
    }

    pub(super) fn reconcile(&self, max_orders_per_cycle: usize) {
        let mut state = self.inner.lock().expect("central admission lock poisoned");
        state.reservations.clear();
        state.remaining_new_exposure = max_orders_per_cycle;
    }

    pub fn reserve(
        &mut self,
        action: &crate::strategy::StrategyAction,
    ) -> Result<Option<CentralAdmissionReservation>, ()> {
        let reserves_new_exposure = match action {
            crate::strategy::StrategyAction::Order(order)
                if order.side() == crate::types::Side::Buy =>
            {
                true
            }
            _ => false,
        };
        if !reserves_new_exposure {
            return Ok(None);
        }
        let mut state = self.inner.lock().expect("central admission lock poisoned");
        if state.remaining_new_exposure == 0 {
            return Err(());
        }
        state.remaining_new_exposure = state.remaining_new_exposure.saturating_sub(1);
        state.next_reservation_id = state.next_reservation_id.wrapping_add(1);
        let id = state.next_reservation_id;
        state.reservations.insert(id);
        Ok(Some(CentralAdmissionReservation {
            inner: self.inner.clone(),
            id,
        }))
    }
}
