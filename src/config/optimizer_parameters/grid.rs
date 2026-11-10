use super::*;

pub(super) fn grid_index(spec: &OptimizerParameterSpec, value: f64) -> Result<i64> {
    let raw = (value - spec.low) / spec.step;
    let rounded = raw.round();
    let tolerance = 1e-9_f64.max(raw.abs() * 1e-12);
    anyhow::ensure!(
        (raw - rounded).abs() <= tolerance
            && rounded >= -(MAX_EXACT_INTEGER_F64 as f64)
            && rounded <= MAX_EXACT_INTEGER_F64 as f64,
        "optimizer parameter {} value={} is not aligned to low={} step={}",
        spec.name,
        value,
        spec.low,
        spec.step
    );
    exact_i64(rounded)
}

pub(super) fn exact_i64(value: f64) -> Result<i64> {
    anyhow::ensure!(
        value.is_finite()
            && value.fract() == 0.0
            && value >= -(MAX_EXACT_INTEGER_F64 as f64)
            && value <= MAX_EXACT_INTEGER_F64 as f64,
        "integer is not exactly representable in ordered f64 transport: {value}"
    );
    Ok(value as i64)
}

pub(super) fn schema_sha256(specs: &[OptimizerParameterSpec]) -> Result<[u8; 32]> {
    let count = u32::try_from(specs.len()).context("optimizer parameter count exceeds u32")?;
    let mut hasher = Sha256::new();
    hasher.update(OPTIMIZER_PARAMETER_SCHEMA);
    hasher.update(count.to_le_bytes());
    for spec in specs {
        update_string(&mut hasher, &spec.name)?;
        hasher.update([match spec.kind {
            OptimizerParameterKind::Int => 0,
            OptimizerParameterKind::Float => 1,
        }]);
        hasher.update(spec.low.to_bits().to_le_bytes());
        hasher.update(spec.high.to_bits().to_le_bytes());
        hasher.update(spec.step.to_bits().to_le_bytes());
    }
    Ok(hasher.finalize().into())
}

pub(super) fn update_string(hasher: &mut Sha256, value: &str) -> Result<()> {
    let len = u32::try_from(value.len()).context("optimizer schema string exceeds u32")?;
    hasher.update(len.to_le_bytes());
    hasher.update(value.as_bytes());
    Ok(())
}
