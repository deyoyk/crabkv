use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use parking_lot::RwLock;

#[derive(Debug, Clone)]
pub enum EntryValue {
    String(Vec<u8>),
    List(Vec<Vec<u8>>),
    Hash(HashMap<Vec<u8>, Vec<u8>>),
    Set(HashSet<Vec<u8>>),
    SortedSet(BTreeMap<OrderedF64, Vec<Vec<u8>>>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct OrderedF64(u64);

impl OrderedF64 {
    fn from_f64(v: f64) -> Self {
        Self(v.to_bits())
    }

    fn to_f64(self) -> f64 {
        f64::from_bits(self.0)
    }
}

struct ExpiryEntry {
    expires_at: Instant,
}

pub struct StorageEngine {
    entries: DashMap<Vec<u8>, EntryValue>,
    expiries: DashMap<Vec<u8>, ExpiryEntry>,
    pub total_keys: AtomicU64,
    shard_count: usize,
    max_memory_bytes: usize,
}

impl StorageEngine {
    pub fn new(shard_count: usize, max_memory_bytes: usize) -> Self {
        Self {
            entries: DashMap::with_shard_amount(shard_count),
            expiries: DashMap::with_shard_amount(shard_count),
            total_keys: AtomicU64::new(0),
            shard_count,
            max_memory_bytes,
        }
    }

    fn is_expired(&self, key: &[u8]) -> bool {
        if let Some(entry) = self.expiries.get(key) {
            if Instant::now() >= entry.expires_at {
                return true;
            }
        }
        false
    }

    fn remove_if_expired(&self, key: &[u8]) -> bool {
        if self.is_expired(key) {
            self.entries.remove(key);
            self.expiries.remove(key);
            self.total_keys.fetch_sub(1, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    // --- String operations ---

    pub fn set(&self, key: &[u8], value: &[u8], expiry: Option<Duration>) {
        if self.remove_if_expired(key) || self.entries.get(key).is_none() {
            self.total_keys.fetch_add(1, Ordering::Relaxed);
        }
        self.entries
            .insert(key.to_vec(), EntryValue::String(value.to_vec()));
        if let Some(ttl) = expiry {
            self.expiries.insert(
                key.to_vec(),
                ExpiryEntry {
                    expires_at: Instant::now() + ttl,
                },
            );
        } else {
            self.expiries.remove(key);
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        if self.remove_if_expired(key) {
            return None;
        }
        self.entries.get(key).and_then(|v| match v.value() {
            EntryValue::String(s) => Some(s.clone()),
            _ => None,
        })
    }

    pub fn del(&self, key: &[u8]) -> bool {
        let existed = self.entries.remove(key).is_some();
        self.expiries.remove(key);
        if existed {
            self.total_keys.fetch_sub(1, Ordering::Relaxed);
        }
        existed
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        if self.remove_if_expired(key) {
            return false;
        }
        self.entries.contains_key(key)
    }

    pub fn incr(&self, key: &[u8]) -> Result<i64, String> {
        if self.remove_if_expired(key) {
            self.total_keys.fetch_add(1, Ordering::Relaxed);
            self.entries
                .insert(key.to_vec(), EntryValue::String(b"1".to_vec()));
            return Ok(1);
        }
        match self.entries.get(key) {
            Some(entry) => match entry.value() {
                EntryValue::String(s) => {
                    let n: i64 = std::str::from_utf8(s)
                        .map_err(|_| "ERR not an integer".to_string())?
                        .parse()
                        .map_err(|_| "ERR not an integer".to_string())?;
                    let new_val = n + 1;
                    drop(entry);
                    self.entries.insert(
                        key.to_vec(),
                        EntryValue::String(new_val.to_string().into_bytes()),
                    );
                    Ok(new_val)
                }
                _ => Err("ERR wrong type".to_string()),
            },
            None => {
                self.total_keys.fetch_add(1, Ordering::Relaxed);
                self.entries
                    .insert(key.to_vec(), EntryValue::String(b"1".to_vec()));
                Ok(1)
            }
        }
    }

    pub fn decr(&self, key: &[u8]) -> Result<i64, String> {
        if self.remove_if_expired(key) {
            self.total_keys.fetch_add(1, Ordering::Relaxed);
            self.entries
                .insert(key.to_vec(), EntryValue::String(b"-1".to_vec()));
            return Ok(-1);
        }
        match self.entries.get(key) {
            Some(entry) => match entry.value() {
                EntryValue::String(s) => {
                    let n: i64 = std::str::from_utf8(s)
                        .map_err(|_| "ERR not an integer".to_string())?
                        .parse()
                        .map_err(|_| "ERR not an integer".to_string())?;
                    let new_val = n - 1;
                    drop(entry);
                    self.entries.insert(
                        key.to_vec(),
                        EntryValue::String(new_val.to_string().into_bytes()),
                    );
                    Ok(new_val)
                }
                _ => Err("ERR wrong type".to_string()),
            },
            None => {
                self.total_keys.fetch_add(1, Ordering::Relaxed);
                self.entries.insert(
                    key.to_vec(),
                    EntryValue::String(b"-1".to_vec()),
                );
                Ok(-1)
            }
        }
    }

    pub fn ttl(&self, key: &[u8]) -> Option<Duration> {
        if !self.exists(key) {
            return None;
        }
        self.expiries.get(key).and_then(|e| {
            let now = Instant::now();
            if e.expires_at > now {
                Some(e.expires_at - now)
            } else {
                None
            }
        })
    }

    pub fn persist(&self, key: &[u8]) -> bool {
        self.expiries.remove(key).is_some()
    }

    // --- MSET / MGET ---

    pub fn mset(&self, pairs: &[(&[u8], &[u8])]) {
        for (key, value) in pairs {
            self.set(key, value, None);
        }
    }

    pub fn mget(&self, keys: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    // --- List operations ---

    pub fn lpush(&self, key: &[u8], values: &[&[u8]]) -> Result<usize, String> {
        self.remove_if_expired(key);
        match self.entries.get_mut(key) {
            Some(mut entry) => match entry.value_mut() {
                EntryValue::List(list) => {
                    for v in values.iter().rev() {
                        list.insert(0, v.to_vec());
                    }
                    Ok(list.len())
                }
                _ => Err("ERR wrong type".to_string()),
            },
            None => {
                let mut list = Vec::new();
                for v in values.iter().rev() {
                    list.insert(0, v.to_vec());
                }
                let len = list.len();
                self.entries
                    .insert(key.to_vec(), EntryValue::List(list));
                self.total_keys.fetch_add(1, Ordering::Relaxed);
                Ok(len)
            }
        }
    }

    pub fn rpush(&self, key: &[u8], values: &[&[u8]]) -> Result<usize, String> {
        self.remove_if_expired(key);
        match self.entries.get_mut(key) {
            Some(mut entry) => match entry.value_mut() {
                EntryValue::List(list) => {
                    for v in values {
                        list.push(v.to_vec());
                    }
                    Ok(list.len())
                }
                _ => Err("ERR wrong type".to_string()),
            },
            None => {
                let list: Vec<Vec<u8>> = values.iter().map(|v| v.to_vec()).collect();
                let len = list.len();
                self.entries
                    .insert(key.to_vec(), EntryValue::List(list));
                self.total_keys.fetch_add(1, Ordering::Relaxed);
                Ok(len)
            }
        }
    }

    pub fn lpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.remove_if_expired(key);
        self.entries.get_mut(key).and_then(|mut entry| {
            match entry.value_mut() {
                EntryValue::List(list) => {
                    let v = list.remove(0);
                    if list.is_empty() {
                        drop(entry);
                        self.entries.remove(key);
                        self.total_keys.fetch_sub(1, Ordering::Relaxed);
                    }
                    Some(v)
                }
                _ => None,
            }
        })
    }

    pub fn rpop(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.remove_if_expired(key);
        self.entries.get_mut(key).and_then(|mut entry| {
            match entry.value_mut() {
                EntryValue::List(list) => {
                    let v = list.pop()?;
                    if list.is_empty() {
                        drop(entry);
                        self.entries.remove(key);
                        self.total_keys.fetch_sub(1, Ordering::Relaxed);
                    }
                    Some(v)
                }
                _ => None,
            }
        })
    }

    pub fn lrange(&self, key: &[u8], start: isize, stop: isize) -> Vec<Vec<u8>> {
        self.remove_if_expired(key);
        self.entries.get(key).map(|entry| {
            match entry.value() {
                EntryValue::List(list) => {
                    let len = list.len() as isize;
                    let s = if start < 0 { start + len } else { start }.max(0) as usize;
                    let e = if stop < 0 { stop + len } else { stop };
                    let e = if e >= len { len - 1 } else { e as usize };
                    if s > e || s >= list.len() {
                        Vec::new()
                    } else {
                        list[s..=e].to_vec()
                    }
                }
                _ => Vec::new(),
            }
        }).unwrap_or_default()
    }

    pub fn llen(&self, key: &[u8]) -> usize {
        self.remove_if_expired(key);
        self.entries.get(key).map(|entry| {
            match entry.value() {
                EntryValue::List(list) => list.len(),
                _ => 0,
            }
        }).unwrap_or(0)
    }

    // --- Hash operations ---

    pub fn hset(&self, key: &[u8], field: &[u8], value: &[u8]) -> Result<bool, String> {
        self.remove_if_expired(key);
        match self.entries.get_mut(key) {
            Some(mut entry) => match entry.value_mut() {
                EntryValue::Hash(map) => {
                    let existed = map.insert(field.to_vec(), value.to_vec()).is_some();
                    Ok(!existed)
                }
                _ => Err("ERR wrong type".to_string()),
            },
            None => {
                let mut map = HashMap::new();
                map.insert(field.to_vec(), value.to_vec());
                self.entries.insert(key.to_vec(), EntryValue::Hash(map));
                self.total_keys.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
        }
    }

    pub fn hget(&self, key: &[u8], field: &[u8]) -> Option<Vec<u8>> {
        self.remove_if_expired(key);
        self.entries.get(key).and_then(|entry| {
            match entry.value() {
                EntryValue::Hash(map) => map.get(field).cloned(),
                _ => None,
            }
        })
    }

    pub fn hdel(&self, key: &[u8], field: &[u8]) -> bool {
        self.remove_if_expired(key);
        self.entries.get_mut(key).map(|mut entry| {
            match entry.value_mut() {
                EntryValue::Hash(map) => {
                    let existed = map.remove(field).is_some();
                    if map.is_empty() {
                        drop(entry);
                        self.entries.remove(key);
                        self.total_keys.fetch_sub(1, Ordering::Relaxed);
                    }
                    existed
                }
                _ => false,
            }
        }).unwrap_or(false)
    }

    pub fn hgetall(&self, key: &[u8]) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.remove_if_expired(key);
        self.entries.get(key).map(|entry| {
            match entry.value() {
                EntryValue::Hash(map) => map.iter().map(|(k, v)| (k.clone(), v.clone())).collect(),
                _ => Vec::new(),
            }
        }).unwrap_or_default()
    }

    // --- Set operations ---

    pub fn sadd(&self, key: &[u8], members: &[&[u8]]) -> Result<usize, String> {
        self.remove_if_expired(key);
        match self.entries.get_mut(key) {
            Some(mut entry) => match entry.value_mut() {
                EntryValue::Set(set) => {
                    let mut added = 0;
                    for m in members {
                        if set.insert(m.to_vec()) {
                            added += 1;
                        }
                    }
                    Ok(added)
                }
                _ => Err("ERR wrong type".to_string()),
            },
            None => {
                let mut set = HashSet::new();
                for m in members {
                    set.insert(m.to_vec());
                }
                let len = set.len();
                self.entries.insert(key.to_vec(), EntryValue::Set(set));
                self.total_keys.fetch_add(1, Ordering::Relaxed);
                Ok(len)
            }
        }
    }

    pub fn srem(&self, key: &[u8], members: &[&[u8]]) -> Result<usize, String> {
        self.remove_if_expired(key);
        self.entries.get_mut(key).map(|mut entry| {
            match entry.value_mut() {
                EntryValue::Set(set) => {
                    let mut removed = 0;
                    for m in members {
                        if set.remove(*m) {
                            removed += 1;
                        }
                    }
                    if set.is_empty() {
                        drop(entry);
                        self.entries.remove(key);
                        self.total_keys.fetch_sub(1, Ordering::Relaxed);
                    }
                    Ok(removed)
                }
                _ => Err("ERR wrong type".to_string()),
            }
        }).unwrap_or(Ok(0))
    }

    pub fn smembers(&self, key: &[u8]) -> Vec<Vec<u8>> {
        self.remove_if_expired(key);
        self.entries.get(key).map(|entry| {
            match entry.value() {
                EntryValue::Set(set) => set.iter().cloned().collect(),
                _ => Vec::new(),
            }
        }).unwrap_or_default()
    }

    pub fn sismember(&self, key: &[u8], member: &[u8]) -> bool {
        self.remove_if_expired(key);
        self.entries.get(key).map(|entry| {
            match entry.value() {
                EntryValue::Set(set) => set.contains(member),
                _ => false,
            }
        }).unwrap_or(false)
    }

    // --- Sorted Set operations ---

    pub fn zadd(&self, key: &[u8], score: f64, member: &[u8]) -> Result<bool, String> {
        self.remove_if_expired(key);
        match self.entries.get_mut(key) {
            Some(mut entry) => match entry.value_mut() {
                EntryValue::SortedSet(zset) => {
                    // Remove old entry if exists (we need to find by member)
                    let old_score = zset.values().flatten().position(|m| m == member);
                    if let Some(pos) = old_score {
                        // Find and remove old score entry
                        let keys_to_fix: Vec<(OrderedF64, Vec<Vec<u8>>)> = zset.iter()
                            .map(|(k, v)| (*k, v.clone()))
                            .collect();
                        for (k, mut v) in keys_to_fix {
                            if let Some(idx) = v.iter().position(|m| m == member) {
                                v.remove(idx);
                                if v.is_empty() {
                                    zset.remove(&k);
                                } else {
                                    zset.insert(k, v);
                                }
                                break;
                            }
                        }
                    }
                    let ordered = OrderedF64::from_f64(score);
                    zset.entry(ordered)
                        .or_insert_with(Vec::new)
                        .push(member.to_vec());
                    Ok(old_score.is_none())
                }
                _ => Err("ERR wrong type".to_string()),
            },
            None => {
                let mut zset = BTreeMap::new();
                let ordered = OrderedF64::from_f64(score);
                zset.insert(ordered, vec![member.to_vec()]);
                self.entries.insert(key.to_vec(), EntryValue::SortedSet(zset));
                self.total_keys.fetch_add(1, Ordering::Relaxed);
                Ok(true)
            }
        }
    }

    pub fn zrem(&self, key: &[u8], member: &[u8]) -> Result<bool, String> {
        self.remove_if_expired(key);
        self.entries.get_mut(key).map(|mut entry| {
            match entry.value_mut() {
                EntryValue::SortedSet(zset) => {
                    let keys_to_fix: Vec<(OrderedF64, Vec<Vec<u8>>)> = zset.iter()
                        .map(|(k, v)| (*k, v.clone()))
                        .collect();
                    for (k, mut v) in keys_to_fix {
                        if let Some(idx) = v.iter().position(|m| m == member) {
                            v.remove(idx);
                            if v.is_empty() {
                                zset.remove(&k);
                            } else {
                                zset.insert(k, v);
                            }
                            if zset.is_empty() {
                                drop(entry);
                                self.entries.remove(key);
                                self.total_keys.fetch_sub(1, Ordering::Relaxed);
                            }
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                _ => Err("ERR wrong type".to_string()),
            }
        }).unwrap_or(Ok(false))
    }

    pub fn zrange(&self, key: &[u8], start: isize, stop: isize) -> Vec<(Vec<u8>, f64)> {
        self.remove_if_expired(key);
        self.entries.get(key).map(|entry| {
            match entry.value() {
                EntryValue::SortedSet(zset) => {
                    let items: Vec<_> = zset.iter()
                        .flat_map(|(score, members)| {
                            members.iter().map(move |m| (m.clone(), score.to_f64()))
                        })
                        .collect();
                    let len = items.len() as isize;
                    let s = if start < 0 { start + len } else { start }.max(0) as usize;
                    let e = if stop < 0 { stop + len } else { stop };
                    let e = if e >= len { len - 1 } else { e as usize };
                    if s > e || s >= items.len() {
                        Vec::new()
                    } else {
                        items[s..=e].to_vec()
                    }
                }
                _ => Vec::new(),
            }
        }).unwrap_or_default()
    }

    pub fn zrank(&self, key: &[u8], member: &[u8]) -> Option<usize> {
        self.remove_if_expired(key);
        self.entries.get(key).and_then(|entry| {
            match entry.value() {
                EntryValue::SortedSet(zset) => {
                    let items: Vec<_> = zset.iter()
                        .flat_map(|(score, members)| {
                            members.iter().map(move |m| (m.clone(), score.to_f64()))
                        })
                        .collect();
                    items.iter().position(|(m, _)| m == member)
                }
                _ => None,
            }
        })
    }

    pub fn zscore(&self, key: &[u8], member: &[u8]) -> Option<f64> {
        self.remove_if_expired(key);
        self.entries.get(key).and_then(|entry| {
            match entry.value() {
                EntryValue::SortedSet(zset) => {
                    for (score, members) in zset.iter() {
                        if members.iter().any(|m| m == member) {
                            return Some(score.to_f64());
                        }
                    }
                    None
                }
                _ => None,
            }
        })
    }

    pub fn key_count(&self) -> usize {
        self.entries.len()
    }
}
