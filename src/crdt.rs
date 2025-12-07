#![allow(dead_code)]
use std::cmp::{self, Ordering};
use std::collections::{BTreeMap, HashMap};
use std::fmt::{self, Display};
use uuid::Uuid;

pub trait Mergeable<V> {
    fn merge(&mut self, other: &V);
}

// ShoppingList
pub struct ShoppingList {
    id: Uuid,
    list: AWORMap,
}

// Item
#[derive(Clone)]
pub struct Item {
    amount: PNCounter,
    acquired: LWWReg<Uuid>,
}

impl Mergeable<Item> for Item {
    fn merge(&mut self, other: &Item) {
        self.amount.merge(&other.amount);
        self.acquired.merge(&other.acquired);
    }
}

// AWORMap
#[derive(Clone)]
pub struct AWORMap {
    items: HashMap<String, Item>,
}

impl AWORMap {
    pub fn new() -> Self {
        Self {
            items: HashMap::new(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn keys(&self) -> Vec<String> {
        // WARNING: not currently filtering for deleted items
        self.items.iter().map(|(k, _)| k.clone()).collect()
    }

    pub fn values(&self) -> Vec<Item> {
        // WARNING: not currently filtering for deleted items
        self.items.values().map(|v| v.clone()).collect()
    }

    pub fn insert(&mut self, name: String, item: Item) {
        if self.items.contains_key(&name) {
            self.items.get_mut(&name).unwrap().merge(&item);
        } else {
            self.items.insert(name, item);
        }
    }
}

impl Mergeable<AWORMap> for AWORMap {
    fn merge(&mut self, other: &AWORMap) {
        for (key, other_item) in &other.items {
            match self.items.get_mut(key) {
                Some(self_item) => {
                    self_item.merge(other_item);
                }
                None => {
                    self.items.insert(key.clone(), other_item.clone());
                }
            }
        }
    }
}

/*
// DeltaAWORMap Metadata
pub struct Metadata {
    pub clock: VClock<Uuid>,
    pub item: Item,
}

impl Metadata {
    pub fn new(item: Item, clock: VClock<Uuid>) -> Self {
        Self { clock, item }
    }
}


// DeltaAWORMap
pub struct DeltaAWORMap {
    entries: HashMap<String, Metadata>,
    actor: Uuid,
}

impl DeltaAWORMap {
    pub fn new(actor: Uuid) -> Self {
        Self {
            entries: HashMap::new(),
            actor,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn keys(&self) -> Vec<String> {
        self.entries
            .iter()
            .filter(|(_, m)| !m.is_deleted)
            .map(|(k, _)| k.clone())
            .collect()
    }

    pub fn values(&self) -> Vec<Item> {
        self.entries
            .values()
            .filter(|m| !m.is_deleted)
            .map(|m| m.item.clone())
            .collect()
    }

    pub fn get_item_ref(&self, name: &String) -> Option<&Item> {
        if let Some(metadata) = self.entries.get(name) {
            if !metadata.is_deleted {
                return Some(&metadata.item);
            }
        }
        None
    }

    pub fn insert(&mut self, name: String, item: Item) {
        let mut clock = self
            .entries
            .get(&name)
            .map(|meta| meta.clock.clone())
            .unwrap_or_default();

        let _ = clock.inc(self.actor);

        let entry = self
            .entries
            .entry(name.clone())
            .or_insert_with(|| Metadata::new(item.clone(), clock.clone()));

        match clock.partial_cmp(&entry.clock) {
            Some(Ordering::Greater) => {
                entry.item = item;
                entry.clock = clock;
                entry.is_deleted = false;
            }
            None => {
                entry.item.merge(&item);
                entry.clock.merge(&clock);
                entry.is_deleted = false;
            }
            _ => (), //TODO: investigate this scenario
                     //se for igual desempate pelo Uuid
        }
    }

    pub fn remove(&mut self, name: &String) {
        if let Some(metadata) = self.entries.get_mut(name) {
            metadata.clock.inc(self.actor);
            metadata.is_deleted = true;
        }
    }
}


// Vector Clock
#[derive(PartialEq, Eq, Hash, Clone)]
pub struct VClock<A: Ord + Copy> {
    pub dots: BTreeMap<A, u64>,
}

impl<A: Ord + Copy> VClock<A> {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn get(&self, actor: &A) -> u64 {
        *self.dots.get(actor).unwrap_or(&0)
    }

    pub fn get_tuple(&self, actor: A) -> (A, u64) {
        let counter = self.get(&actor);
        (actor, counter)
    }

    pub fn inc(&mut self, actor: A) -> u64 {
        let entry = self.dots.entry(actor).or_insert(0);
        *entry += 1;
        *entry
    }

    pub fn is_empty(&self) -> bool {
        self.dots.is_empty()
    }

    pub fn diverged(&self, other: &VClock<A>) -> bool {
        self.partial_cmp(other).is_none()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&A, u64)> {
        self.dots.iter().map(|(a, c)| (a, *c))
    }

    pub fn common(left: &VClock<A>, right: &VClock<A>) -> VClock<A> {
        let mut dots = BTreeMap::new();
        for (left_actor, left_counter) in left.dots.iter() {
            let right_counter = right.get(left_actor);
            if right_counter == *left_counter {
                dots.insert(*left_actor, *left_counter);
            }
        }
        Self { dots }
    }
}

impl<A: Ord + Copy> Mergeable<VClock<A>> for VClock<A> {
    fn merge(&mut self, other: &VClock<A>) {
        for (a, c) in other.dots.iter() {
            let entry = self.dots.entry(*a).or_insert(0);
            if *entry < *c {
                *entry = *c;
            }
        }
    }
}

impl<A: Ord + Display + Copy> Display for VClock<A> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "<")?;
        for (i, (actor, count)) in self.dots.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}:{}", actor, count)?;
        }
        write!(f, ">")
    }
}

impl<A: Ord + Copy> Default for VClock<A> {
    fn default() -> Self {
        Self {
            dots: BTreeMap::new(),
        }
    }
}

impl<A: Ord + Copy> PartialOrd for VClock<A> {
    fn partial_cmp(&self, other: &VClock<A>) -> Option<Ordering> {
        if self == other {
            Some(Ordering::Equal)
        } else if other.dots.iter().all(|(w, c)| self.get(w) >= *c) {
            Some(Ordering::Greater)
        } else if self.dots.iter().all(|(w, c)| other.get(w) >= *c) {
            Some(Ordering::Less)
        } else {
            None
        }
    }
}

*/

// LLWReg
#[derive(Clone)]
pub struct LWWReg<A> {
    val: u32,
    clock: u32, // monotonic value
    actor: A,   // per actor
}

impl<A: Ord + Copy> LWWReg<A> {
    pub fn new(val: u32, clock: u32, actor: A) -> Self {
        Self { val, clock, actor }
    }

    fn should_update(&self, clock: u32, actor: &A) -> bool {
        match clock.cmp(&self.clock) {
            Ordering::Greater => true,
            Ordering::Less => false,
            Ordering::Equal => actor > &self.actor,
        }
    }

    pub fn update(&mut self, val: u32, clock: u32, actor: A) {
        if self.should_update(clock, &actor) {
            self.val = val;
            self.clock = clock;
            self.actor = actor;
        }
    }
}

impl<A: Ord + Copy> Mergeable<LWWReg<A>> for LWWReg<A> {
    fn merge(&mut self, other: &LWWReg<A>) {
        if self.should_update(other.clock, &other.actor) {
            self.val = other.val;
            self.clock = other.clock;
            self.actor = other.actor;
        }
    }
}

impl<A: Ord + Default> Default for LWWReg<A> {
    fn default() -> Self {
        Self {
            val: u32::default(),
            clock: u32::default(),
            actor: A::default(),
        }
    }
}

// PNCounter
#[derive(Clone)]
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

    pub fn value_local(&self) -> i64 {
        self.p.value_local() as i64 - self.n.value_local() as i64
    }

    pub fn value_total(&self) -> i64 {
        self.p.value_total() as i64 - self.n.value_total() as i64
    }
}

impl Mergeable<PNCounter> for PNCounter {
    fn merge(&mut self, other: &PNCounter) {
        self.p.merge(&other.p);
        self.n.merge(&other.n);
    }
}

// GCounter
#[derive(Clone)]
struct GCounter {
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
}

impl Mergeable<GCounter> for GCounter {
    fn merge(&mut self, other: &GCounter) {
        for (id, count) in &other.counter {
            let max_count = *cmp::max(count, self.counter.get(id).unwrap_or(&0));
            self.counter.insert(*id, max_count);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn mk_item(amount: i64, clock: u32, actor: Uuid, acquired: bool) -> Item {
        let mut pn = PNCounter::new(actor);
        if amount > 0 {
            for _ in 0..amount {
                pn.inc();
            }
        } else {
            for _ in 0..(-amount) {
                pn.inc();
            }
        }

        let reg = LWWReg::new(acquired as u32, clock, actor);

        Item {
            amount: pn,
            acquired: reg,
        }
    }

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

    // LWWReg
    #[test]
    fn test_lwwreg_update_newer_clock() {
        let mut reg = LWWReg::new(10, 1, 1u32);
        reg.update(20, 2, 2);

        assert_eq!(reg.val, 20);
        assert_eq!(reg.clock, 2);
        assert_eq!(reg.actor, 2);
    }

    #[test]
    fn test_lwwreg_update_ignores_older_clock() {
        let mut reg = LWWReg::new(10, 2, 1u32);
        reg.update(20, 1, 2);

        assert_eq!(reg.val, 10);
        assert_eq!(reg.clock, 2);
        assert_eq!(reg.actor, 1);
    }

    #[test]
    fn test_lwwreg_update_tiebreaker() {
        let mut reg = LWWReg::new(10, 5, 1u32);
        reg.update(20, 5, 2);

        assert_eq!(reg.val, 20);
        assert_eq!(reg.actor, 2);
    }

    #[test]
    fn test_lwwreg_merge() {
        let mut a = LWWReg::new(1, 1, 1u32);
        let b = LWWReg::new(2, 3, 2u32);

        a.merge(&b);

        assert_eq!(a.val, 2);
        assert_eq!(a.clock, 3);
        assert_eq!(a.actor, 2);
    }

    #[test]
    fn test_lwwreg_merge_tiebreaker() {
        let mut a = LWWReg::new(10, 7, 1u32);
        let b = LWWReg::new(20, 7, 2u32);

        a.merge(&b);

        assert_eq!(a.val, 20);
        assert_eq!(a.actor, 2);
    }

    #[test]
    fn test_lwwreg_merge_is_idempotent() {
        let mut a = LWWReg::new(10, 5, 3u32);
        let clone = a.clone();

        a.merge(&clone);

        assert_eq!(a.val, clone.val);
        assert_eq!(a.clock, clone.clock);
        assert_eq!(a.actor, clone.actor);
    }

    #[test]
    fn test_lwwreg_merge_is_commutative() {
        let a = LWWReg::new(10, 5, 1u32);
        let b = LWWReg::new(20, 7, 2u32);

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.val, ba.val);
        assert_eq!(ab.clock, ba.clock);
        assert_eq!(ab.actor, ba.actor);
    }

    #[test]
    fn test_lwwreg_merge_is_associative() {
        let a = LWWReg::new(10, 1, 1u32);
        let b = LWWReg::new(20, 2, 2u32);
        let c = LWWReg::new(30, 3, 3u32);

        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut a_bc = a.clone();
        let mut bc = b.clone();
        bc.merge(&c);
        a_bc.merge(&bc);

        assert_eq!(ab_c.val, a_bc.val);
        assert_eq!(ab_c.clock, a_bc.clock);
        assert_eq!(ab_c.actor, a_bc.actor);
    }

    // AWORMap
    #[test]
    fn test_awormap_insert_item() {
        let mut map = AWORMap::new();
        let id = Uuid::new_v4();

        let item = mk_item(3, 1, id, false);
        map.insert("apple".into(), item.clone());

        assert!(!map.is_empty());
        assert_eq!(map.keys(), vec!["apple".to_string()]);
        assert_eq!(
            map.values()[0].amount.value_local(),
            item.amount.value_local()
        );
    }

    #[test]
    fn test_awormap_insert_item_merges() {
        let mut map = AWORMap::new();
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        let a = mk_item(2, 1, id1, false);
        let b = mk_item(5, 3, id2, false);

        map.insert("apple".into(), a.clone());
        map.insert("apple".into(), b.clone());

        let result = map.values()[0].clone();

        assert_eq!(
            result.amount.value_total(),
            a.amount.value_local() + b.amount.value_local()
        );
    }

    #[test]
    fn test_awormap_merge_combines_keys() {
        let mut m1 = AWORMap::new();
        let mut m2 = AWORMap::new();

        let id = Uuid::new_v4();

        m1.insert("apple".into(), mk_item(3, 1, id, false));
        m2.insert("banana".into(), mk_item(7, 1, id, false));

        m1.merge(&m2);

        let keys = m1.keys();
        assert!(keys.contains(&"apple".to_string()));
        assert!(keys.contains(&"banana".to_string()));
    }

    #[test]
    fn test_awormap_acquired() {
        let mut m1 = AWORMap::new();
        let mut m2 = AWORMap::new();

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();

        m1.insert("apple".into(), mk_item(2, 2, id1, false));
        m2.insert("apple".into(), mk_item(2, 1, id2, true));

        m1.merge(&m2);

        let result = m1.items.get("apple").unwrap();

        assert_eq!(result.acquired.val, 0u32);
    }

    #[test]
    fn test_awormap_merge_idempotent() {
        let mut m1 = AWORMap::new();

        let id = Uuid::new_v4();
        m1.insert("apple".into(), mk_item(1, 1, id, false));

        let clone = m1.clone();
        m1.merge(&clone);

        assert_eq!(m1.keys(), clone.keys());
        assert_eq!(
            m1.items["apple"].amount.value_local(),
            clone.items["apple"].amount.value_local()
        );
        assert_eq!(
            m1.items["apple"].acquired.val,
            clone.items["apple"].acquired.val
        );
    }

    #[test]
    fn test_awormap_merge_is_commutative() {
        let id1 = Uuid::nil();
        let id2 = Uuid::from_u128(2);

        let mut a = AWORMap::new();
        let mut b = AWORMap::new();

        a.insert("apple".into(), mk_item(3, 2, id1, false));
        b.insert("apple".into(), mk_item(5, 4, id2, false));
        b.insert("banana".into(), mk_item(1, 1, id1, false));

        let mut ab = a.clone();
        ab.merge(&b);

        let mut ba = b.clone();
        ba.merge(&a);

        assert_eq!(ab.keys().len(), ba.keys().len());
        assert_eq!(
            ab.items["apple"].amount.value_total(),
            ba.items["apple"].amount.value_total()
        );
        assert_eq!(
            ab.items["apple"].acquired.clock,
            ba.items["apple"].acquired.clock
        );
        assert_eq!(
            ab.items["banana"].amount.value_total(),
            ba.items["banana"].amount.value_total()
        );
    }

    #[test]
    fn test_awormap_merge_is_associative() {
        let id1 = Uuid::nil();
        let id2 = Uuid::from_u128(2);
        let id3 = Uuid::from_u128(3);

        let mut a = AWORMap::new();
        let mut b = AWORMap::new();
        let mut c = AWORMap::new();

        a.insert("apple".into(), mk_item(1, 1, id1, false));
        b.insert("apple".into(), mk_item(2, 2, id2, false));
        c.insert("banana".into(), mk_item(3, 3, id3, false));

        let mut ab_c = a.clone();
        ab_c.merge(&b);
        ab_c.merge(&c);

        let mut a_bc = a.clone();
        let mut bc = b.clone();
        bc.merge(&c);
        a_bc.merge(&bc);

        assert_eq!(ab_c.keys().len(), a_bc.keys().len());

        for key in ab_c.keys() {
            let i1 = ab_c.items.get(&key).unwrap();
            let i2 = a_bc.items.get(&key).unwrap();

            assert_eq!(i1.amount.value_total(), i2.amount.value_total());
            assert_eq!(i1.acquired.clock, i2.acquired.clock);
            assert_eq!(i1.acquired.actor, i2.acquired.actor);
        }
    }

    #[test]
    fn test_awormap_replicas_converge_after_merge() {
        let id1 = Uuid::nil();
        let id2 = Uuid::from_u128(2);

        let mut r1 = AWORMap::new();
        let mut r2 = AWORMap::new();

        r1.insert("apple".into(), mk_item(2, 1, id1, false));
        r2.insert("apple".into(), mk_item(5, 3, id2, false));
        r2.insert("banana".into(), mk_item(1, 1, id1, false));

        let mut m1 = r1.clone();
        let mut m2 = r2.clone();

        m1.merge(&r2);
        m2.merge(&r1);

        assert_eq!(m1.keys(), m2.keys());

        for key in m1.keys() {
            let i1 = m1.items.get(&key).unwrap();
            let i2 = m2.items.get(&key).unwrap();

            assert_eq!(i1.amount.value_total(), i2.amount.value_total());
            assert_eq!(i1.acquired.clock, i2.acquired.clock);
        }
    }
}
