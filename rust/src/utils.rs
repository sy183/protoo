use rand::Rng;

/// Generates a random positive integer compatible with the original
/// JavaScript implementation.
pub(crate) fn generate_random_number() -> u64 {
    rand::thread_rng().gen_range(0..=10_000_000)
}
