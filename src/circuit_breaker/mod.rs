use std::sync::Mutex;
use std::time::{Duration, Instant};

use tracing::{info, warn};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum CircuitState {
    Closed,
    Open,
    HalfOpen,
}

impl CircuitState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Closed => "closed",
            Self::Open => "open",
            Self::HalfOpen => "half_open",
        }
    }
}

pub struct CircuitBreaker {
    name: &'static str,
    failure_threshold: usize,
    reset_timeout: Duration,
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    state: CircuitState,
    failures: usize,
    opened_at: Option<Instant>,
}

impl CircuitBreaker {
    pub fn new(name: &'static str, failure_threshold: usize, reset_timeout: Duration) -> Self {
        Self {
            name,
            failure_threshold,
            reset_timeout,
            inner: Mutex::new(Inner {
                state: CircuitState::Closed,
                failures: 0,
                opened_at: None,
            }),
        }
    }

    pub fn allow_request(&self) -> bool {
        let mut inner = self.inner.lock().expect("circuit breaker poisoned");

        if inner.state == CircuitState::Open {
            if inner
                .opened_at
                .map(|opened_at| opened_at.elapsed() >= self.reset_timeout)
                .unwrap_or(false)
            {
                inner.state = CircuitState::HalfOpen;
                inner.failures = 0;
                info!(
                    breaker = self.name,
                    state = inner.state.as_str(),
                    "circuit breaker transitioned"
                );
                return true;
            }

            return false;
        }

        true
    }

    pub fn record_success(&self) {
        let mut inner = self.inner.lock().expect("circuit breaker poisoned");
        let previous = inner.state;

        inner.state = CircuitState::Closed;
        inner.failures = 0;
        inner.opened_at = None;

        if previous != inner.state {
            info!(
                breaker = self.name,
                state = inner.state.as_str(),
                "circuit breaker transitioned"
            );
        }
    }

    pub fn record_failure(&self) {
        let mut inner = self.inner.lock().expect("circuit breaker poisoned");

        match inner.state {
            CircuitState::Closed => {
                inner.failures += 1;
                if inner.failures >= self.failure_threshold {
                    inner.state = CircuitState::Open;
                    inner.opened_at = Some(Instant::now());
                    warn!(
                        breaker = self.name,
                        state = inner.state.as_str(),
                        failures = inner.failures,
                        "circuit breaker transitioned"
                    );
                }
            }
            CircuitState::HalfOpen => {
                inner.state = CircuitState::Open;
                inner.failures = self.failure_threshold;
                inner.opened_at = Some(Instant::now());
                warn!(
                    breaker = self.name,
                    state = inner.state.as_str(),
                    failures = inner.failures,
                    "circuit breaker transitioned"
                );
            }
            CircuitState::Open => {}
        }
    }

    pub fn state(&self) -> CircuitState {
        let mut inner = self.inner.lock().expect("circuit breaker poisoned");

        if inner.state == CircuitState::Open
            && inner
                .opened_at
                .map(|opened_at| opened_at.elapsed() >= self.reset_timeout)
                .unwrap_or(false)
        {
            inner.state = CircuitState::HalfOpen;
            inner.failures = 0;
            info!(
                breaker = self.name,
                state = inner.state.as_str(),
                "circuit breaker transitioned"
            );
        }

        inner.state
    }
}
