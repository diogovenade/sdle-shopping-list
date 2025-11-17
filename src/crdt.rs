use std::cmp;
use std::collections::HashMap;
use uuid::Uuid;

// Item
pub struct Item {
    id: Uuid,
    name: String,
    amount: u32,
    acquired: bool,
}

// PNCounter
pub struct PNCounter {
    p: GCounter,
    n: GCounter,
}

impl PNCounter {
    pub fn new(id: Uuid) -> Self {
        PNCounter {
            p: GCounter::new(id),
            n: GCounter::new(id),
        }
    }

    pub fn inc(&mut self) {
        self.p.inc();
    }

    pub fn dec(&mut self) {
        self.n.inc();
    }

    pub fn value_local(&self) -> u64 {
        self.p.value_local() - self.n.value_local()
    }

    pub fn value_total(&self) -> u64 {
        self.p.value_total() - self.n.value_total()
    }

    pub fn merge(&mut self, other: &PNCounter) {
        self.p.merge(&other.p);
        self.n.merge(&other.n);
    }
}

// GCounter
#[derive(Clone)]
pub struct GCounter {
    counter: HashMap<Uuid, u64>,
    id: Uuid,
}

impl GCounter {
    pub fn new(id: Uuid) -> Self {
        GCounter {
            counter: HashMap::new(),
            id,
        }
    }

    pub fn inc(&mut self) {
        *self.counter.entry(self.id).or_insert(0) += 1;
    }

    pub fn value_local(&self) -> u64 {
        *self.counter.get(&self.id).unwrap_or(&0)
    }

    pub fn value_total(&self) -> u64 {
        self.counter.values().sum()
    }

    pub fn merge(&mut self, other: &GCounter) {
        for (id, count) in &other.counter {
            let max_count = *cmp::max(count, self.counter.get(id).unwrap_or(&0));
            self.counter.insert(id.to_owned(), max_count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    // GCounter
    #[test]
    fn test_gcounter_local_increment() {
        let id = Uuid::new_v4();
        let mut c = GCounter::new(id);

        assert_eq!(c.value_local(), 0);
        assert_eq!(c.value_total(), 0);

        c.inc();
        c.inc();

        assert_eq!(c.value_local(), 2);
        assert_eq!(c.value_total(), 2);
    }

    #[test]
    fn test_gcounter_merge() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let mut c1 = GCounter::new(id1);
        let mut c2 = GCounter::new(id2);

        c1.inc();
        c1.inc();
        c2.inc();

        c1.merge(&c2);

        assert_eq!(c1.value_total(), 3);
        assert_eq!(c1.value_local(), 2);
        assert_eq!(c2.value_local(), 1);
    }

    #[test]
    fn test_gcounter_merge_commutative() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let mut a = GCounter::new(id1);
        let mut b = GCounter::new(id2);

        a.inc();
        a.inc();
        b.inc();

        let mut a_then_b = a.clone();
        let mut b_then_a = b.clone();

        a_then_b.merge(&b);
        b_then_a.merge(&a);

        assert_eq!(a_then_b.value_total(), b_then_a.value_total());
    }

    #[test]
    fn test_gcounter_merge_is_idempotent() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let mut a = GCounter::new(id1);
        let mut b = GCounter::new(id2);

        a.inc();
        b.inc();

        a.merge(&b);
        let first_merge_value = a.value_total();

        a.merge(&b);
        let second_merge_value = a.value_total();

        assert_eq!(first_merge_value, second_merge_value);
    }

    #[test]
    fn test_gcounter_merge_is_associative() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        let mut a = GCounter::new(id1);
        let mut b = GCounter::new(id2);
        let mut c = GCounter::new(id3);

        a.inc(); // 1
        b.inc(); // 1
        b.inc(); // 2
        c.inc(); // 1

        let mut ab = a.clone();
        ab.merge(&b);
        ab.merge(&c);

        let mut bc = b.clone();
        bc.merge(&c);
        let mut a_bc = a.clone();
        a_bc.merge(&bc);

        assert_eq!(ab.value_total(), a_bc.value_total());
    }

    // PNCounter
    #[test]
    fn test_pncounter_local_value() {
        let id = Uuid::new_v4();
        let mut counter = PNCounter::new(id);

        assert_eq!(counter.value_local(), 0);

        counter.inc();
        assert_eq!(counter.value_local(), 1);

        counter.dec();
        assert_eq!(counter.value_local(), 0);

        counter.inc();
        counter.inc();
        counter.dec();
        assert_eq!(counter.value_local(), 1);
    }

    #[test]
    fn test_pncounter_merge_counters() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let mut c1 = PNCounter::new(id1);
        let mut c2 = PNCounter::new(id2);

        c1.inc();
        c1.inc();
        c2.inc();
        c2.dec();

        assert_eq!(c1.value_total(), 2);
        assert_eq!(c2.value_total(), 0);

        c1.merge(&c2);
        c2.merge(&c1);

        assert_eq!(c1.value_total(), 2);
        assert_eq!(c2.value_total(), 2);
    }

    #[test]
    fn test_pncounter_convergence() {
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let mut c1 = PNCounter::new(id1);
        let mut c2 = PNCounter::new(id2);

        for _ in 0..5 {
            c1.inc();
        }
        for _ in 0..3 {
            c2.inc();
        }
        for _ in 0..2 {
            c1.dec();
        }
        for _ in 0..1 {
            c2.dec();
        }

        c1.merge(&c2);
        c2.merge(&c1);

        assert_eq!(c1.value_total(), 5 + 3 - 2 - 1);
        assert_eq!(c2.value_total(), 5);
    }

    #[test]
    fn test_local_values_are_correct() {
        let id = Uuid::new_v4();
        let mut c = PNCounter::new(id);

        for _ in 0..4 {
            c.inc();
        }
        for _ in 0..2 {
            c.dec();
        }

        assert_eq!(c.value_local(), 2);
    }
}
