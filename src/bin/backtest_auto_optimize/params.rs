use super::*;

pub(super) fn initial_mean_for_params(
    specs: &[ParamSpec],
    params: &BTreeMap<String, ParamValue>,
) -> Result<Vec<f64>> {
    ensure!(
        params.len() == specs.len(),
        "optimizer parameter dimension {} != spec dimension {}",
        params.len(),
        specs.len()
    );
    specs
        .iter()
        .map(|spec| {
            let value = params
                .get(&spec.name)
                .with_context(|| format!("missing optimizer parameter {}", spec.name))?;
            spec.unit_for_value(value)
        })
        .collect()
}

pub(super) fn params_from_vector(
    specs: &[ParamSpec],
    x: &DVector<f64>,
) -> Result<BTreeMap<String, ParamValue>> {
    ensure!(
        x.len() == specs.len(),
        "optimizer vector dimension {} != spec dimension {}",
        x.len(),
        specs.len()
    );
    specs
        .iter()
        .enumerate()
        .map(|(index, spec)| Ok((spec.name.clone(), spec.value_at_unit(x[index])?)))
        .collect()
}

pub(super) fn default_params(specs: &[ParamSpec]) -> Result<BTreeMap<String, ParamValue>> {
    specs
        .iter()
        .map(|spec| Ok((spec.name.clone(), spec.default_value()?)))
        .collect()
}
