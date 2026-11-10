use std::time::Duration;

const NATIVE_TIMER_GRANULARITY: Duration = Duration::from_millis(1);

#[derive(Default)]
pub(in crate::polymarket_ws) struct PingResponseEstimator {
    estimate: Option<ResponseEstimate>,
}

#[derive(Clone, Copy)]
struct ResponseEstimate {
    srtt: Duration,
    rttvar: Duration,
}

impl PingResponseEstimator {
    pub(in crate::polymarket_ws) fn observe(&mut self, response: Duration) {
        self.estimate = Some(match self.estimate {
            None => ResponseEstimate {
                srtt: response,
                rttvar: response / 2,
            },
            Some(previous) => {
                let deviation = previous.srtt.abs_diff(response);
                ResponseEstimate {
                    rttvar: weighted_duration(previous.rttvar, 3, deviation, 1, 4),
                    srtt: weighted_duration(previous.srtt, 7, response, 1, 8),
                }
            }
        });
    }

    pub(in crate::polymarket_ws) fn allowance(&self) -> Option<Duration> {
        self.estimate.map(|estimate| {
            let variability = estimate.rttvar.saturating_mul(4);
            estimate
                .srtt
                .saturating_add(variability.max(NATIVE_TIMER_GRANULARITY))
        })
    }
}

fn weighted_duration(
    first: Duration,
    first_weight: u128,
    second: Duration,
    second_weight: u128,
    divisor: u128,
) -> Duration {
    let nanos = first
        .as_nanos()
        .saturating_mul(first_weight)
        .saturating_add(second.as_nanos().saturating_mul(second_weight))
        / divisor;
    duration_from_nanos(nanos)
}

fn duration_from_nanos(nanos: u128) -> Duration {
    let seconds = nanos / 1_000_000_000;
    if seconds > u128::from(u64::MAX) {
        return Duration::MAX;
    }
    Duration::new(seconds as u64, (nanos % 1_000_000_000) as u32)
}
