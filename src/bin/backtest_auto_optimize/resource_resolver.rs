use super::*;

const MIN_RESERVE_CPUS: usize = 2;

#[derive(Clone, Debug)]
pub(super) struct ResourcePolicy {
    pub configured_jobs: usize,
    pub online_cpus: usize,
    pub effective_jobs: usize,
}

pub(super) fn resolve_resource_policy(configured_jobs: usize) -> Result<ResourcePolicy> {
    let online_cpus = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(1)
        .max(1);
    let reserve_cpus = MIN_RESERVE_CPUS.min(online_cpus.saturating_sub(1));
    let cpu_slots = online_cpus.saturating_sub(reserve_cpus).max(1);
    let effective_jobs = if configured_jobs == 0 {
        cpu_slots
    } else {
        configured_jobs.min(cpu_slots).max(1)
    };
    crate::summary::publish_progress(format_args!(
        "AUTO_OPTIMIZE_RESOURCES online_cpus={} configured_jobs={} effective_jobs={} reserve_cpus={} source={}",
        online_cpus,
        configured_jobs,
        effective_jobs,
        reserve_cpus,
        if configured_jobs == 0 {
            "online_cpus_minus_two"
        } else {
            "explicit_override_capped_to_online_cpus_minus_two"
        }
    ));
    Ok(ResourcePolicy {
        configured_jobs,
        online_cpus,
        effective_jobs,
    })
}
