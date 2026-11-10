use anyhow::{Context, Result};

use crate::ctf_inventory::CTF_COHORT_ID_MAX_BYTES;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct CohortIdentityRecord {
    len: u8,
    bytes: [u8; CTF_COHORT_ID_MAX_BYTES],
}

impl Default for CohortIdentityRecord {
    fn default() -> Self {
        Self::empty()
    }
}

impl CohortIdentityRecord {
    pub(crate) const fn empty() -> Self {
        Self {
            len: 0,
            bytes: [0; CTF_COHORT_ID_MAX_BYTES],
        }
    }

    pub(crate) fn optional(value: Option<&str>) -> Result<Self> {
        match value {
            Some(value) => Self::required(value),
            None => Ok(Self::empty()),
        }
    }

    pub(crate) fn required(value: &str) -> Result<Self> {
        anyhow::ensure!(!value.is_empty(), "cohort identity is empty");
        anyhow::ensure!(
            value.len() <= CTF_COHORT_ID_MAX_BYTES,
            "cohort identity exceeds wire limit bytes={} limit={CTF_COHORT_ID_MAX_BYTES}",
            value.len()
        );
        let mut bytes = [0; CTF_COHORT_ID_MAX_BYTES];
        bytes[..value.len()].copy_from_slice(value.as_bytes());
        Ok(Self {
            len: value.len() as u8,
            bytes,
        })
    }

    pub(crate) const fn is_empty(self) -> bool {
        self.len == 0
    }

    pub(crate) fn optional_str(&self) -> Result<Option<&str>> {
        if self.is_empty() {
            return Ok(None);
        }
        Ok(Some(self.required_str()?))
    }

    pub(crate) fn required_str(&self) -> Result<&str> {
        anyhow::ensure!(!self.is_empty(), "cohort identity is empty");
        std::str::from_utf8(&self.bytes[..usize::from(self.len)])
            .context("cohort identity is not UTF-8")
    }
}
