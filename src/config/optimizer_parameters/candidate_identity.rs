use super::grid::update_string;
use super::*;

impl OptimizerParameterSchema {
    pub fn candidate_sha256(
        &self,
        base: &MakerRebateMmStrategyConfig,
        values: &BTreeMap<String, OptimizerParameterValue>,
    ) -> Result<[u8; 32]> {
        let ordered = self.ordered_values(values)?;
        let mut base = base.clone();
        base.optimizer_symbol_configs.clear();
        let base_bytes = serde_json::to_vec(&base).context("serialize optimizer base config")?;
        let mut hasher = Sha256::new();
        hasher.update(b"PMCAND01");
        hasher.update(self.sha256);
        hasher.update(Sha256::digest(base_bytes));
        for value in ordered {
            match value {
                OptimizerParameterValue::Int(value) => {
                    hasher.update([0]);
                    hasher.update(value.to_le_bytes());
                }
                OptimizerParameterValue::Float(value) => {
                    hasher.update([1]);
                    hasher.update(value.to_bits().to_le_bytes());
                }
            }
        }
        Ok(hasher.finalize().into())
    }

    pub fn symbol_candidate_id(
        &self,
        symbol: &str,
        base: &MakerRebateMmStrategyConfig,
        values: &BTreeMap<String, OptimizerParameterValue>,
    ) -> Result<String> {
        let symbol = symbol.trim().to_ascii_uppercase();
        anyhow::ensure!(!symbol.is_empty(), "optimizer candidate symbol is empty");
        let mut applied = self.apply_symbol_map(base, values, &symbol)?;
        applied.optimizer_symbol_configs.clear();
        applied.enabled_assets = vec![symbol.clone()];
        let applied_bytes =
            serde_json::to_vec(&applied).context("serialize applied symbol optimizer config")?;
        let mut hasher = Sha256::new();
        hasher.update(b"PMCSYM02");
        update_string(&mut hasher, &symbol)?;
        hasher.update(self.sha256);
        hasher.update(Sha256::digest(applied_bytes));
        Ok(format!("{:x}", hasher.finalize()))
    }
}
