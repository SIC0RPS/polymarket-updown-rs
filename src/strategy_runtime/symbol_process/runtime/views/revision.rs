use anyhow::Result;

pub(super) fn accept_revision<K: std::hash::Hash + Eq + Copy + std::fmt::Debug>(
    revisions: &mut std::collections::HashMap<K, u64>,
    identity: K,
    revision: u64,
    owner: &str,
) -> Result<()> {
    anyhow::ensure!(
        revision > 0,
        "{owner} central revision is zero identity={identity:?}"
    );
    anyhow::ensure!(
        revisions
            .get(&identity)
            .is_none_or(|previous| revision > *previous),
        "{owner} central revision duplicate/regression identity={identity:?} previous={:?} actual={revision}",
        revisions.get(&identity)
    );
    revisions.insert(identity, revision);
    Ok(())
}
