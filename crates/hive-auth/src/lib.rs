//! Authentication and the capability tokens that carry authorization between services. The gate checks these on every request and the comb checks them again, so the code lives here once.
//!
//! The design is in `spec/10_security.md`. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
