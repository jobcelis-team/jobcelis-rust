//! Helpers for automatic retries: which failures are retried and how long to
//! wait between attempts.

use std::time::{Duration, SystemTime};

/// Number of retries used by [`crate::JobcelisClient::new`]. With the default
/// backoff (1s, 2s, 4s, 8s, 16s, 30s) it covers about 60 seconds of downtime.
pub const DEFAULT_MAX_RETRIES: u32 = 6;

pub(crate) const DEFAULT_RETRY_BASE_DELAY: Duration = Duration::from_secs(1);
const MAX_DELAY_FACTOR: u32 = 30;
const MAX_RETRY_AFTER: Duration = Duration::from_secs(60);

/// Header that carries the idempotency key of a single event.
pub(crate) const IDEMPOTENCY_KEY_HEADER: &str = "x-idempotency-key";

/// HTTP statuses that mean the server did not process the request.
pub(crate) fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 502 | 503 | 504)
}

/// Delay before retry number `attempt` (1-based): `base * 2^(attempt-1)`,
/// capped at 30 times the base (30s by default), with +/-20% jitter.
pub(crate) fn backoff_delay(attempt: u32, base: Duration) -> Duration {
    let limit = base * MAX_DELAY_FACTOR;
    let exp = attempt.saturating_sub(1).min(16);
    let delay = base.saturating_mul(1u32 << exp).min(limit);
    let jitter = 0.8 + 0.4 * random_unit();
    delay.mul_f64(jitter)
}

/// Parses a `Retry-After` value (seconds or an HTTP date), capped at 60s.
pub(crate) fn parse_retry_after(value: &str) -> Option<Duration> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }
    let delay = if let Ok(secs) = value.parse::<f64>() {
        if !secs.is_finite() {
            return None;
        }
        Duration::from_secs_f64(secs.max(0.0).min(MAX_RETRY_AFTER.as_secs_f64()))
    } else {
        let date = httpdate::parse_http_date(value).ok()?;
        date.duration_since(SystemTime::now()).unwrap_or(Duration::ZERO)
    };
    Some(delay.min(MAX_RETRY_AFTER))
}

/// A new random UUID v4, as used for idempotency keys.
pub(crate) fn new_idempotency_key() -> String {
    uuid::Uuid::new_v4().to_string()
}

/// A random number in `[0, 1)`.
fn random_unit() -> f64 {
    let bits = uuid::Uuid::new_v4().as_u128() as u64 >> 11;
    bits as f64 / (1u64 << 53) as f64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_up_to_30_seconds_with_jitter() {
        let base = Duration::from_secs(1);
        for (attempt, secs) in [(1, 1.0), (2, 2.0), (3, 4.0), (4, 8.0), (5, 16.0), (6, 30.0), (7, 30.0), (40, 30.0)] {
            let d = backoff_delay(attempt, base).as_secs_f64();
            assert!(d >= secs * 0.8 - 1e-9 && d <= secs * 1.2 + 1e-9, "attempt {attempt}: {d}s");
        }
    }

    #[test]
    fn retry_after_seconds_and_dates() {
        assert_eq!(parse_retry_after("0"), Some(Duration::ZERO));
        assert_eq!(parse_retry_after("5"), Some(Duration::from_secs(5)));
        assert_eq!(parse_retry_after("120"), Some(Duration::from_secs(60)));
        assert_eq!(parse_retry_after("soon"), None);
        let date = httpdate::fmt_http_date(SystemTime::now() + Duration::from_secs(10));
        let d = parse_retry_after(&date).unwrap();
        assert!(d > Duration::ZERO && d <= Duration::from_secs(10), "{d:?}");
    }

    #[test]
    fn idempotency_keys_are_uuid_v4() {
        let key = new_idempotency_key();
        assert_eq!(key.len(), 36);
        assert_eq!(&key[14..15], "4");
        assert_ne!(key, new_idempotency_key());
    }
}
