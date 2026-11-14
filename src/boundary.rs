#![forbid(unsafe_code)]

use crate::error::SyncError;
use crate::sqlite::SqliteStore;

#[derive(Clone, Debug)]
pub struct SyncBoundary {
    pub store: SqliteStore,
    pub enabled: bool,
}

/// Maximum accepted size for a message or whole batch.
pub const MAX_ENVELOPE_BYTES: usize = 64 * 1024;

impl SyncBoundary {
    /// Check the whole batch before allowing its members into the sync boundary.
    pub fn envelope_batch<'a, I>(&self, messages: I) -> Result<usize, SyncError>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        if !self.enabled {
            return Err(SyncError::Disabled);
        }
        let mut total = 0usize;
        for bytes in messages {
            total = total.checked_add(self.envelope(bytes)?)
                .ok_or(SyncError::TooLarge)?;
            if total > MAX_ENVELOPE_BYTES {
                return Err(SyncError::TooLarge);
            }
        }
        Ok(total)
    }

    pub fn envelope(&self, bytes: &[u8]) -> Result<usize, SyncError> {
        if !self.enabled {
            return Err(SyncError::Disabled);
        }
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(SyncError::TooLarge);
        }
        Ok(bytes.len())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    fn boundary(enabled: bool) -> SyncBoundary {
        SyncBoundary { store: SqliteStore::default(), enabled }
    }

    #[test]
    fn batch_checks_aggregate_budget() {
        let b = boundary(true);
        assert!(matches!(b.envelope_batch([&[0u8; 32_768][..], &[0u8; 32_769][..]]), Err(SyncError::TooLarge)));
        assert_eq!(b.envelope_batch([&[0u8; 32_768][..], &[0u8; 32_768][..]]).unwrap(), MAX_ENVELOPE_BYTES);
        assert!(matches!(b.envelope_batch([&[0u8; 65_537][..]]), Err(SyncError::TooLarge)));
    }

    #[test]
    fn disabled_batch_is_denied_even_when_empty() {
        let b = boundary(false);
        assert!(matches!(b.envelope_batch(std::iter::empty::<&[u8]>()), Err(SyncError::Disabled)));
    }
}
