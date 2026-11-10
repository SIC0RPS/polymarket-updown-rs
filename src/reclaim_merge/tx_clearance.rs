use super::*;

pub(super) fn reclaim_retry_remaining(
    last_attempt_at: Option<&Instant>,
    retry_cooldown: Duration,
) -> Option<Duration> {
    let last_attempt_at = last_attempt_at?;
    retry_cooldown.checked_sub(last_attempt_at.elapsed())
}

pub(super) fn schedule_reclaim_retry(
    reclaim_trigger: &std::sync::Arc<tokio::sync::Notify>,
    delay: Duration,
) {
    let reclaim_trigger = reclaim_trigger.clone();
    tokio::spawn(async move {
        tokio::time::sleep(delay.max(Duration::from_millis(1))).await;
        reclaim_trigger.notify_one();
    });
}
