use anyhow::{Context, Result};

const SCALE: u128 = 1_000_000;
const MAX_PROJECTABLE_UNITS: u128 = 1_u128 << 52;

#[derive(Clone, Copy, Default, PartialEq, PartialOrd, Hash)]
pub struct RiskFixed6(u64);

impl RiskFixed6 {
    pub fn capacity(value: f64, owner: &str) -> Result<Self> {
        Self::from_nonnegative_f64(value, owner, false)
    }

    pub fn consumption(value: f64, owner: &str) -> Result<Self> {
        Self::from_nonnegative_f64(value, owner, true)
    }

    pub fn capacity_product(left: f64, right: f64, owner: &str) -> Result<Self> {
        Self::from_nonnegative_product(left, right, owner, false)
    }

    pub fn consumption_product(left: f64, right: f64, owner: &str) -> Result<Self> {
        Self::from_nonnegative_product(left, right, owner, true)
    }

    pub fn from_units(units: u128, owner: &str) -> Result<Self> {
        anyhow::ensure!(
            units <= MAX_PROJECTABLE_UNITS,
            "{owner} exceeds unique fixed-6dp strategy projection units={units}"
        );
        Ok(Self(
            u64::try_from(units).context("fixed-6dp units exceed u64")?,
        ))
    }

    pub const fn units(self) -> u64 {
        self.0
    }

    pub fn as_f64(self) -> f64 {
        self.0 as f64 / SCALE as f64
    }

    pub fn checked_add(self, other: Self, owner: &str) -> Result<Self> {
        let units = u128::from(self.0)
            .checked_add(u128::from(other.0))
            .with_context(|| format!("{owner} fixed-6dp sum overflow"))?;
        Self::from_units(units, owner)
    }

    pub const fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }

    pub const fn max(self, other: Self) -> Self {
        if self.0 >= other.0 {
            self
        } else {
            other
        }
    }

    fn from_nonnegative_f64(value: f64, owner: &str, round_up: bool) -> Result<Self> {
        let (mantissa, exponent) = Self::nonnegative_binary_parts(value, owner)?;
        Self::from_scaled_binary(mantissa, exponent, owner, round_up)
    }

    fn from_nonnegative_product(
        left: f64,
        right: f64,
        owner: &str,
        round_up: bool,
    ) -> Result<Self> {
        let (left_mantissa, left_exponent) = Self::nonnegative_binary_parts(left, owner)?;
        let (right_mantissa, right_exponent) = Self::nonnegative_binary_parts(right, owner)?;
        let mantissa = left_mantissa
            .checked_mul(right_mantissa)
            .with_context(|| format!("{owner} fixed-6dp product overflow"))?;
        let exponent = left_exponent
            .checked_add(right_exponent)
            .with_context(|| format!("{owner} fixed-6dp product exponent overflow"))?;
        Self::from_scaled_binary(mantissa, exponent, owner, round_up)
    }

    fn nonnegative_binary_parts(value: f64, owner: &str) -> Result<(u128, i32)> {
        anyhow::ensure!(
            value.is_finite() && value >= 0.0 && !value.is_sign_negative(),
            "{owner} is not a finite nonnegative risk fact"
        );
        let bits = value.to_bits();
        let exponent_bits = ((bits >> 52) & 0x7ff) as i32;
        let fraction = bits & ((1_u64 << 52) - 1);
        Ok(if exponent_bits == 0 {
            (u128::from(fraction), 1 - 1023 - 52)
        } else {
            (
                u128::from((1_u64 << 52) | fraction),
                exponent_bits - 1023 - 52,
            )
        })
    }

    fn from_scaled_binary(
        mantissa: u128,
        exponent: i32,
        owner: &str,
        round_up: bool,
    ) -> Result<Self> {
        if mantissa == 0 {
            return Ok(Self::default());
        }
        let scaled = mantissa
            .checked_mul(SCALE)
            .with_context(|| format!("{owner} fixed-6dp scale overflow"))?;
        let units = if exponent >= 0 {
            let shift = exponent as u32;
            anyhow::ensure!(
                shift < u128::BITS && scaled <= (u128::MAX >> shift),
                "{owner} fixed-6dp shift overflow"
            );
            scaled << shift
        } else {
            let shift = exponent.unsigned_abs();
            if shift >= 128 {
                u128::from(round_up)
            } else {
                let quotient = scaled >> shift;
                let remainder_mask = (1_u128 << shift) - 1;
                let has_remainder = scaled & remainder_mask != 0;
                quotient
                    .checked_add(u128::from(round_up && has_remainder))
                    .with_context(|| format!("{owner} fixed-6dp rounding overflow"))?
            }
        };
        Self::from_units(units, owner)
    }
}
