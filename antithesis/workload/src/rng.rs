pub fn u64_in(beginning: u64, end: u64) -> u64 {
    beginning + antithesis_sdk::random::get_random() % (end - beginning + 1)
}
pub fn random_item<T>(slice: &[T]) -> Option<&T> {
    if slice.is_empty() {
        None
    } else {
        slice.get((antithesis_sdk::random::get_random() as usize) % slice.len())
    }
}
