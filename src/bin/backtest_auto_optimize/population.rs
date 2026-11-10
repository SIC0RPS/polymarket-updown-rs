use super::*;

pub(crate) fn candidate_id_for_params(
    ctx: &RunContext,
    params_by_symbol: &BTreeMap<String, BTreeMap<String, ParamValue>>,
) -> Result<String> {
    joint_candidate_id(
        &ctx.cfg,
        &ctx.parameter_schemas,
        &ctx.symbols,
        params_by_symbol,
    )
}

pub(crate) fn joint_candidate_id(
    cfg: &AppConfig,
    schemas: &BTreeMap<String, OptimizerParameterSchema>,
    expected_symbols: &[String],
    params_by_symbol: &BTreeMap<String, BTreeMap<String, ParamValue>>,
) -> Result<String> {
    ensure!(
        params_by_symbol.len() == expected_symbols.len(),
        "joint optimizer candidate symbol count mismatch"
    );
    let mut symbol_ids = BTreeMap::new();
    for symbol in expected_symbols {
        let schema = schemas
            .get(symbol)
            .with_context(|| format!("optimizer schema missing symbol={symbol}"))?;
        let params = params_by_symbol
            .get(symbol)
            .with_context(|| format!("optimizer params missing symbol={symbol}"))?;
        let id = schema
            .symbol_candidate_id(symbol, &symbol_base_config(cfg, symbol), params)
            .with_context(|| format!("hash {symbol} optimizer candidate"))?;
        ensure!(
            symbol_ids.insert(symbol.clone(), id).is_none(),
            "duplicate joint optimizer symbol={symbol}"
        );
    }
    let mut hasher = Sha256::new();
    hasher.update(b"PMJOINT01");
    hasher.update(serde_json::to_vec(&symbol_ids).context("serialize joint candidate identity")?);
    Ok(format!("{:x}", hasher.finalize()))
}

fn symbol_base_config(cfg: &AppConfig, symbol: &str) -> MakerRebateMmStrategyConfig {
    let mut base = cfg
        .strategy
        .maker_rebate_mm
        .config_for_symbol(symbol)
        .clone();
    base.optimizer_symbol_configs.clear();
    base.enabled_assets = vec![symbol.to_string()];
    base
}

pub(crate) fn strategy_configs_for_params(
    ctx: &RunContext,
    params_by_symbol: &BTreeMap<String, BTreeMap<String, ParamValue>>,
) -> Result<(
    MakerRebateMmStrategyConfig,
    BTreeMap<String, MakerRebateMmStrategyConfig>,
)> {
    let mut configs = BTreeMap::new();
    for symbol in &ctx.symbols {
        let schema = ctx
            .parameter_schemas
            .get(symbol)
            .with_context(|| format!("optimizer schema missing symbol={symbol}"))?;
        let params = params_by_symbol
            .get(symbol)
            .with_context(|| format!("optimizer params missing symbol={symbol}"))?;
        let config = schema
            .apply_symbol_map(&symbol_base_config(&ctx.cfg, symbol), params, symbol)
            .with_context(|| format!("build {symbol} strategy knobs"))?;
        ensure!(
            configs.insert(symbol.clone(), config).is_none(),
            "duplicate joint optimizer symbol={symbol}"
        );
    }
    Ok((ctx.cfg.strategy.maker_rebate_mm.clone(), configs))
}

pub(crate) fn default_params_by_symbol(
    ctx: &RunContext,
) -> Result<BTreeMap<String, BTreeMap<String, ParamValue>>> {
    ctx.symbols
        .iter()
        .map(|symbol| {
            let schema = ctx
                .parameter_schemas
                .get(symbol)
                .with_context(|| format!("optimizer schema missing symbol={symbol}"))?;
            Ok((symbol.clone(), default_params(schema.specs())?))
        })
        .collect()
}

pub(crate) fn initial_mean_for_joint_params(
    ctx: &RunContext,
    params_by_symbol: &BTreeMap<String, BTreeMap<String, ParamValue>>,
) -> Result<Vec<f64>> {
    let mut mean = Vec::with_capacity(ctx.parameter_count());
    for symbol in &ctx.symbols {
        let schema = ctx
            .parameter_schemas
            .get(symbol)
            .with_context(|| format!("optimizer schema missing symbol={symbol}"))?;
        let params = params_by_symbol
            .get(symbol)
            .with_context(|| format!("optimizer params missing symbol={symbol}"))?;
        mean.extend(params::initial_mean_for_params(schema.specs(), params)?);
    }
    Ok(mean)
}

fn params_by_symbol_from_vector(
    ctx: &RunContext,
    vector: &DVector<f64>,
) -> Result<BTreeMap<String, BTreeMap<String, ParamValue>>> {
    ensure!(
        vector.len() == ctx.parameter_count(),
        "joint optimizer vector dimension mismatch"
    );
    let mut offset = 0usize;
    let mut params_by_symbol = BTreeMap::new();
    for symbol in &ctx.symbols {
        let schema = ctx
            .parameter_schemas
            .get(symbol)
            .with_context(|| format!("optimizer schema missing symbol={symbol}"))?;
        let len = schema.specs().len();
        let symbol_vector =
            DVector::from_iterator(len, vector.iter().skip(offset).take(len).copied());
        let params = params_from_vector(schema.specs(), &symbol_vector)?;
        ensure!(
            params_by_symbol.insert(symbol.clone(), params).is_none(),
            "duplicate joint optimizer symbol={symbol}"
        );
        offset = offset
            .checked_add(len)
            .context("joint optimizer vector offset overflow")?;
    }
    ensure!(
        offset == vector.len(),
        "joint optimizer vector was not consumed"
    );
    Ok(params_by_symbol)
}

pub(crate) fn evaluate_population(ctx: &RunContext, vectors: &[DVector<f64>]) -> Result<Vec<f64>> {
    let requests = vectors
        .iter()
        .map(|vector| {
            params_by_symbol_from_vector(ctx, vector)
                .map(|params| CandidateRequest::new(params, TrialOrigin::Search))
        })
        .collect::<Result<Vec<_>>>()?;
    evaluate::evaluate_params_batch(ctx, requests, false)
}
