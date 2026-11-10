use crate::strategy::fair_value::sigmoid;

pub(crate) const ONE_SIDED_95_Z: f64 = 1.644_853_626_951_472_2;

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ProbabilityInterval {
    lo: f64,
    hi: f64,
}

impl ProbabilityInterval {
    pub(crate) fn new(lo: f64, hi: f64) -> Option<Self> {
        (lo.is_finite() && hi.is_finite() && 0.0 <= lo && lo <= hi && hi <= 1.0)
            .then_some(Self { lo, hi })
    }

    pub(crate) fn from_logit(mean: f64, std: f64, z: f64) -> Option<Self> {
        if !(mean.is_finite() && std.is_finite() && std >= 0.0 && z.is_finite() && z > 0.0) {
            return None;
        }
        let radius = outward_up(std * z);
        Self::new(
            sigmoid(outward_down(mean - radius)),
            sigmoid(outward_up(mean + radius)),
        )
    }

    pub(crate) fn lo(self) -> f64 {
        self.lo
    }

    pub(crate) fn hi(self) -> f64 {
        self.hi
    }

    pub(crate) fn contains(self, value: f64) -> bool {
        value.is_finite() && self.lo <= value && value <= self.hi
    }

    pub(crate) fn affine(self, scale: f64, offset: f64) -> Option<Self> {
        if !(scale.is_finite() && offset.is_finite()) {
            return None;
        }
        let (lo, hi) = if scale >= 0.0 {
            (
                outward_down(scale * self.lo + offset),
                outward_up(scale * self.hi + offset),
            )
        } else {
            (
                outward_down(scale * self.hi + offset),
                outward_up(scale * self.lo + offset),
            )
        };
        Self::new(lo.max(0.0), hi.min(1.0))
    }
}

pub(crate) fn monotone_probability_hull(
    input: ProbabilityInterval,
    mean: f64,
    map: impl Fn(f64) -> f64,
) -> Option<ProbabilityInterval> {
    let interval = ProbabilityInterval::new(
        outward_down(map(input.lo())).max(0.0),
        outward_up(map(input.hi())).min(1.0),
    )?;
    interval.contains(mean).then_some(interval)
}

pub(crate) fn probability_hull(
    mean: f64,
    values: impl IntoIterator<Item = f64>,
) -> Option<ProbabilityInterval> {
    let mut lo = f64::INFINITY;
    let mut hi = f64::NEG_INFINITY;
    for value in values {
        if !value.is_finite() {
            return None;
        }
        lo = lo.min(value);
        hi = hi.max(value);
    }
    let interval = ProbabilityInterval::new(outward_down(lo).max(0.0), outward_up(hi).min(1.0))?;
    interval.contains(mean).then_some(interval)
}

pub(crate) fn toward_midpoint_hull(
    input: ProbabilityInterval,
    mean: f64,
) -> Option<ProbabilityInterval> {
    let interval = ProbabilityInterval::new(input.lo().min(0.5), input.hi().max(0.5))?;
    interval.contains(mean).then_some(interval)
}

fn outward_down(value: f64) -> f64 {
    if !value.is_finite() || value == f64::NEG_INFINITY {
        return value;
    }
    if value == 0.0 {
        return -f64::from_bits(1);
    }
    let bits = value.to_bits();
    f64::from_bits(if value > 0.0 { bits - 1 } else { bits + 1 })
}

fn outward_up(value: f64) -> f64 {
    if !value.is_finite() || value == f64::INFINITY {
        return value;
    }
    if value == 0.0 {
        return f64::from_bits(1);
    }
    let bits = value.to_bits();
    f64::from_bits(if value > 0.0 { bits + 1 } else { bits - 1 })
}
