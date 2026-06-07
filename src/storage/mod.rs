use std::collections::{BTreeMap, VecDeque};
use std::collections::hash_map::Entry as HashMapEntry;
use std::hash::Hasher;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ahash::{AHashMap, AHashSet, AHasher};
use bytes::Bytes;
use crossbeam_skiplist::SkipSet;
use crossbeam_utils::CachePadded;
use parking_lot::RwLock;
use rand::Rng;

const DEFAULT_NUM_SHARDS: usize = 256;
const DEFAULT_MAX_MEMORY: usize = 256 * 1024 * 1024;
const CLEANUP_INTERVAL: Duration = Duration::from_secs(10);
const LFU_DECAY_WINDOW: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Core data types
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq)]
pub enum StorageValue {
    String(Bytes),
    List(VecDeque<Vec<u8>>),
    Hash(AHashMap<Vec<u8>, Vec<u8>>),
    Set(AHashSet<Vec<u8>>),
    SortedSet(SortedSetData),
    Stream(StreamData),
}

impl Clone for StorageValue {
    fn clone(&self) -> Self {
        match self {
            StorageValue::String(v) => StorageValue::String(v.clone()),
            StorageValue::List(v) => StorageValue::List(v.clone()),
            StorageValue::Hash(v) => StorageValue::Hash(v.clone()),
            StorageValue::Set(v) => StorageValue::Set(v.clone()),
            StorageValue::SortedSet(data) => {
                let mut new_data = SortedSetData::default();
                for entry in data.tree.iter() {
                    let (score, member) = entry.value().clone();
                    new_data.tree.insert((score, member.clone()));
                    new_data.member_index.insert(member, score);
                }
                StorageValue::SortedSet(new_data)
            }
            StorageValue::Stream(v) => StorageValue::Stream(v.clone()),
        }
    }
}

#[derive(Debug, Default)]
pub struct SortedSetData {
    pub tree: SkipSet<(i64, Vec<u8>)>,
    pub member_index: AHashMap<Vec<u8>, i64>,
}

impl PartialEq for SortedSetData {
    fn eq(&self, other: &Self) -> bool {
        if self.tree.len() != other.tree.len() {
            return false;
        }
        self.tree.iter().zip(other.tree.iter()).all(|(a, b)| a.value() == b.value())
            && self.member_index == other.member_index
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct StreamData {
    pub entries: BTreeMap<u64, StreamEntry>,
    pub last_entry_id: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamEntry {
    pub id: u64,
    pub fields: AHashMap<Vec<u8>, Vec<u8>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    String,
    List,
    Hash,
    Set,
    SortedSet,
    Stream,
}

impl StorageValue {
    pub fn data_type(&self) -> DataType {
        match self {
            StorageValue::String(_) => DataType::String,
            StorageValue::List(_) => DataType::List,
            StorageValue::Hash(_) => DataType::Hash,
            StorageValue::Set(_) => DataType::Set,
            StorageValue::SortedSet(_) => DataType::SortedSet,
            StorageValue::Stream(_) => DataType::Stream,
        }
    }

    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + match self {
                StorageValue::String(v) => v.len(),
                StorageValue::List(v) => {
                    v.iter().map(|e| e.len()).sum::<usize>()
                        + v.len() * std::mem::size_of::<Vec<u8>>()
                }
                StorageValue::Hash(m) => {
                    m.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
                        + m.len() * (std::mem::size_of::<Vec<u8>>() * 2)
                }
                StorageValue::Set(s) => {
                    s.iter().map(|e| e.len()).sum::<usize>()
                        + s.len() * std::mem::size_of::<Vec<u8>>()
                }
                StorageValue::SortedSet(s) => {
                    s.tree
                        .iter()
                        .map(|entry| {
                            let (_score, member) = entry.value();
                            std::mem::size_of::<i64>() + member.len()
                        })
                        .sum::<usize>()
                        + s.tree.len() * (std::mem::size_of::<i64>() + std::mem::size_of::<Vec<u8>>())
                        + s.member_index.len() * (std::mem::size_of::<Vec<u8>>() + std::mem::size_of::<i64>())
                }
                StorageValue::Stream(s) => {
                    s.entries
                        .iter()
                        .map(|(_id, e)| {
                            std::mem::size_of::<u64>()
                                + e.fields
                                    .iter()
                                    .map(|(k, v)| k.len() + v.len())
                                    .sum::<usize>()
                        })
                        .sum::<usize>()
                }
            }
    }
}

// ---------------------------------------------------------------------------
// Eviction
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionPolicy {
    Lfu,
    Lru,
}

struct AccessInfo {
    count: u64,
    last_access: Instant,
}

// ---------------------------------------------------------------------------
// Expiry tracking
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct Expiry {
    pub expires_at: Instant,
    pub key: Vec<u8>,
}

// ---------------------------------------------------------------------------
// Shard
// ---------------------------------------------------------------------------

pub struct Shard {
    data: AHashMap<Vec<u8>, StorageValue>,
    ttl: AHashMap<Vec<u8>, Instant>,
    access_info: AHashMap<Vec<u8>, AccessInfo>,
    size_bytes: usize,
}

impl Shard {
    fn new() -> Self {
        Self {
            data: AHashMap::new(),
            ttl: AHashMap::new(),
            access_info: AHashMap::new(),
            size_bytes: 0,
        }
    }

    fn insert(&mut self, key: Vec<u8>, value: StorageValue) -> usize {
        let new_usage = value.memory_usage();
        match self.data.entry(key) {
            HashMapEntry::Occupied(mut e) => {
                let old_usage = e.get().memory_usage();
                self.size_bytes -= old_usage;
                self.size_bytes += new_usage;
                e.insert(value);
                old_usage
            }
            HashMapEntry::Vacant(e) => {
                self.size_bytes += new_usage;
                e.insert(value);
                0
            }
        }
    }

    fn remove(&mut self, key: &[u8]) -> Option<StorageValue> {
        if let Some(val) = self.data.remove(key) {
            self.size_bytes -= val.memory_usage();
            self.ttl.remove(key);
            self.access_info.remove(key);
            Some(val)
        } else {
            None
        }
    }

    fn is_expired(&self, key: &[u8]) -> bool {
        if self.ttl.is_empty() {
            return false;
        }
        self.ttl
            .get(key)
            .map(|exp| Instant::now() >= *exp)
            .unwrap_or(false)
    }

    fn cleanup_expired(&mut self) -> Vec<Vec<u8>> {
        let now = Instant::now();
        let expired: Vec<Vec<u8>> = self
            .ttl
            .iter()
            .filter(|(_, exp)| now >= **exp)
            .map(|(k, _)| k.clone())
            .collect();

        for key in &expired {
            if let Some(val) = self.data.remove(key) {
                self.size_bytes -= val.memory_usage();
            }
            self.ttl.remove(key);
            self.access_info.remove(key);
        }

        expired
    }


    fn pick_victim_lfu(&self) -> Option<Vec<u8>> {
        let now = Instant::now();
        self.data
            .keys()
            .filter(|k| !self.is_expired(k))
            .min_by_key(|k| {
                self.access_info
                    .get(*k)
                    .map(|info| {
                        let elapsed = now.duration_since(info.last_access).as_secs();
                        let decay = elapsed / LFU_DECAY_WINDOW.as_secs().max(1);
                        info.count.saturating_sub(decay)
                    })
                    .unwrap_or(0)
            })
            .cloned()
    }

    fn pick_victim_lru(&self) -> Option<Vec<u8>> {
        let now = Instant::now();
        self.data
            .keys()
            .filter(|k| !self.is_expired(k))
            .min_by_key(|k| {
                self.access_info
                    .get(*k)
                    .map(|info| now.duration_since(info.last_access))
                    .unwrap_or(Duration::MAX)
            })
            .cloned()
    }
}

// ---------------------------------------------------------------------------
// StorageEngine
// ---------------------------------------------------------------------------

pub struct StorageEngine {
    shards: Vec<RwLock<Shard>>,
    num_shards: usize,
    max_memory: AtomicUsize,
    current_memory: CachePadded<AtomicUsize>,
    eviction_policy: RwLock<EvictionPolicy>,
}

impl StorageEngine {
    pub fn new() -> Self {
        Self::with_config(DEFAULT_NUM_SHARDS, DEFAULT_MAX_MEMORY, EvictionPolicy::Lru)
    }

    pub fn with_config(num_shards: usize, max_memory: usize, eviction_policy: EvictionPolicy) -> Self {
        let num_shards = num_shards.max(1);
        let mut shards = Vec::with_capacity(num_shards);
        for _ in 0..num_shards {
            shards.push(RwLock::new(Shard::new()));
        }
        Self {
            shards,
            num_shards,
            max_memory: AtomicUsize::new(max_memory),
            current_memory: CachePadded::new(AtomicUsize::new(0)),
            eviction_policy: RwLock::new(eviction_policy),
        }
    }

    fn shard_index(&self, key: &[u8]) -> usize {
        let mut hasher = AHasher::default();
        hasher.write(key);
        hasher.finish() as usize % self.num_shards
    }

    fn get_shard(&self, key: &[u8]) -> &RwLock<Shard> {
        &self.shards[self.shard_index(key)]
    }

    fn evict_if_needed(&self) {
        let current = self.current_memory.load(Ordering::Relaxed);
        let max = self.max_memory.load(Ordering::Relaxed);
        if current <= max {
            return;
        }

        let policy = *self.eviction_policy.read();
        let target = max * 80 / 100;

        for shard_lock in &self.shards {
            if self.current_memory.load(Ordering::Relaxed) <= target {
                break;
            }
            let mut shard = shard_lock.write();
            let victim = match policy {
                EvictionPolicy::Lfu => shard.pick_victim_lfu(),
                EvictionPolicy::Lru => shard.pick_victim_lru(),
            };
            if let Some(vk) = victim {
                if let Some(val) = shard.remove(&vk) {
                    let freed = val.memory_usage();
                    self.current_memory.fetch_sub(freed, Ordering::Relaxed);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Public API: key/value basics
    // -----------------------------------------------------------------------

    pub fn get(&self, key: &[u8]) -> Option<StorageValue> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return None;
        }
        shard.data.get(key).cloned()
    }

    pub fn set(&self, key: Vec<u8>, value: StorageValue) -> Option<StorageValue> {
        let shard_lock = self.get_shard(&key);
        let mut shard = shard_lock.write();
        let new_size = value.memory_usage();
        let old_size = shard.insert(key, value);
        drop(shard);

        let delta = new_size as isize - old_size as isize;
        if delta > 0 {
            self.current_memory.fetch_add(delta as usize, Ordering::Relaxed);
        } else if delta < 0 {
            self.current_memory.fetch_sub((-delta) as usize, Ordering::Relaxed);
        }
        self.evict_if_needed();

        None
    }

    pub fn set_with_ttl(
        &self,
        key: Vec<u8>,
        value: StorageValue,
        ttl: Duration,
    ) -> Option<StorageValue> {
        let shard_lock = self.get_shard(&key);
        let mut shard = shard_lock.write();
        let new_size = value.memory_usage();
        let expires_at = Instant::now() + ttl;
        let old_size = shard.insert(key.clone(), value);
        shard.ttl.insert(key, expires_at);
        drop(shard);

        let delta = new_size as isize - old_size as isize;
        if delta > 0 {
            self.current_memory.fetch_add(delta as usize, Ordering::Relaxed);
        } else if delta < 0 {
            self.current_memory.fetch_sub((-delta) as usize, Ordering::Relaxed);
        }
        self.evict_if_needed();

        None
    }

    pub fn del(&self, key: &[u8]) -> bool {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();
        if let Some(val) = shard.remove(key) {
            let size = val.memory_usage();
            drop(shard);
            self.current_memory.fetch_sub(size, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    pub fn exists(&self, key: &[u8]) -> bool {
        let shard = self.get_shard(key).read();
        shard.data.contains_key(key) && !shard.is_expired(key)
    }

    pub fn mget(&self, keys: &[Vec<u8>]) -> Vec<Option<StorageValue>> {
        keys.iter().map(|k| self.get(k)).collect()
    }

    pub fn mset(&self, pairs: Vec<(Vec<u8>, StorageValue)>) {
        for (key, value) in pairs {
            self.set(key, value);
        }
    }

    // -----------------------------------------------------------------------
    // Numeric operations
    // -----------------------------------------------------------------------

    pub fn incr(&self, key: &[u8], amount: i64) -> Result<i64, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            let result = amount;
            let mut itoa_buf = itoa::Buffer::new();
            let new_bytes = itoa_buf.format(result).as_bytes().to_vec();
            let new_val = StorageValue::String(Bytes::from(new_bytes));
            let new_size = new_val.memory_usage();
            shard.insert(key.to_vec(), new_val);
            shard.ttl.remove(key);
            drop(shard);
            self.current_memory.fetch_add(new_size, Ordering::Relaxed);
            self.evict_if_needed();
            return Ok(result);
        }

        let (current, old_size) = match shard.data.get(key) {
            Some(StorageValue::String(s)) => {
                let str_val = std::str::from_utf8(s).map_err(|_| "value is not an integer")?;
                let val = str_val
                    .parse::<i64>()
                    .map_err(|_| "value is not an integer")?;
                (val, s.len() + std::mem::size_of::<StorageValue>())
            }
            Some(_) => return Err("wrong type for key".into()),
            None => {
                let result = amount;
                let mut itoa_buf = itoa::Buffer::new();
                let new_bytes = itoa_buf.format(result).as_bytes().to_vec();
                let new_val = StorageValue::String(Bytes::from(new_bytes));
                let new_size = new_val.memory_usage();
                shard.insert(key.to_vec(), new_val);
                drop(shard);
                self.current_memory.fetch_add(new_size, Ordering::Relaxed);
                self.evict_if_needed();
                return Ok(result);
            }
        };

        let result = current.checked_add(amount).ok_or("increment would overflow")?;
        let mut itoa_buf = itoa::Buffer::new();
        let new_bytes = itoa_buf.format(result).as_bytes().to_vec();
        let new_val = StorageValue::String(Bytes::from(new_bytes));
        let new_size = new_val.memory_usage();
        shard.insert(key.to_vec(), new_val);
        drop(shard);

        let delta = new_size as isize - old_size as isize;
        if delta > 0 {
            self.current_memory.fetch_add(delta as usize, Ordering::Relaxed);
        } else if delta < 0 {
            self.current_memory.fetch_sub((-delta) as usize, Ordering::Relaxed);
        }

        Ok(result)
    }

    pub fn decr(&self, key: &[u8], amount: i64) -> Result<i64, String> {
        self.incr(key, -amount)
    }

    // -----------------------------------------------------------------------
    // TTL operations
    // -----------------------------------------------------------------------

    pub fn expire(&self, key: &[u8], ttl: Duration) -> bool {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();
        if shard.data.contains_key(key) && !shard.is_expired(key) {
            shard
                .ttl
                .insert(key.to_vec(), Instant::now() + ttl);
            true
        } else {
            false
        }
    }

    pub fn ttl(&self, key: &[u8]) -> Option<Duration> {
        let shard = self.get_shard(key).read();
        if !shard.data.contains_key(key) {
            return None;
        }
        if shard.is_expired(key) {
            return Some(Duration::ZERO);
        }
        shard.ttl.get(key).map(|exp| {
            let now = Instant::now();
            if now >= *exp {
                Duration::ZERO
            } else {
                *exp - now
            }
        })
    }

    pub fn persist(&self, key: &[u8]) -> bool {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();
        if shard.data.contains_key(key) {
            shard.ttl.remove(key);
            true
        } else {
            false
        }
    }

    // -----------------------------------------------------------------------
    // String operations
    // -----------------------------------------------------------------------

    pub fn append(&self, key: &[u8], data: &[u8]) -> Result<usize, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::String(s)) => {
                let old_len = s.len();
                let mut v = s.to_vec();
                v.extend_from_slice(data);
                let new_len = v.len();
                *s = Bytes::from(v);
                let diff = (new_len as isize) - (old_len as isize);
                drop(shard);
                if diff > 0 {
                    self.current_memory
                        .fetch_add(diff as usize, Ordering::Relaxed);
                }
                Ok(new_len)
            }
            Some(_) => Err("wrong type for key".into()),
            None => {
                let val = StorageValue::String(Bytes::from(data.to_vec()));
                let new_size = val.memory_usage();
                shard.insert(key.to_vec(), val);
                drop(shard);
                self.current_memory
                    .fetch_add(new_size, Ordering::Relaxed);
                self.evict_if_needed();
                Ok(data.len())
            }
        }
    }

    pub fn strlen(&self, key: &[u8]) -> Result<usize, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::String(s)) => Ok(s.len()),
            Some(_) => Err("wrong type for key".into()),
            None => Err("key does not exist".into()),
        }
    }

    pub fn getrange(&self, key: &[u8], start: i64, end: i64) -> Result<Vec<u8>, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::String(s)) => {
                let len = s.len() as i64;
                let start = if start < 0 { (len + start).max(0) } else { start };
                let end = if end < 0 { len + end } else { end.min(len - 1) };
                if start > end || start >= len {
                    return Ok(Vec::new());
                }
                Ok(s[start as usize..=end as usize].to_vec())
            }
            Some(_) => Err("wrong type for key".into()),
            None => Err("key does not exist".into()),
        }
    }

    pub fn setrange(&self, key: &[u8], offset: usize, data: &[u8]) -> Result<usize, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::String(s)) => {
                let old_len = s.len();
                let mut v = s.to_vec();
                let needed = offset + data.len();
                if needed > old_len {
                    v.resize(needed, 0);
                }
                v[offset..offset + data.len()].copy_from_slice(data);
                let new_len = v.len();
                *s = Bytes::from(v);
                let diff = (new_len as isize) - (old_len as isize);
                let final_len = new_len;
                drop(shard);
                if diff > 0 {
                    self.current_memory
                        .fetch_add(diff as usize, Ordering::Relaxed);
                }
                Ok(final_len)
            }
            Some(_) => Err("wrong type for key".into()),
            None => Err("key does not exist".into()),
        }
    }

    // -----------------------------------------------------------------------
    // Type command
    // -----------------------------------------------------------------------

    pub fn r#type(&self, key: &[u8]) -> Option<DataType> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return None;
        }
        shard.data.get(key).map(|v| v.data_type())
    }

    // -----------------------------------------------------------------------
    // List operations
    // -----------------------------------------------------------------------

    pub fn lpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> Result<usize, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::List(list)) => {
                let added_bytes: usize = values.iter().map(|v| v.len() + std::mem::size_of::<Vec<u8>>()).sum();
                for v in values.into_iter().rev() {
                    list.push_front(v);
                }
                let len = list.len();
                drop(shard);
                self.current_memory.fetch_add(added_bytes, Ordering::Relaxed);
                Ok(len)
            }
            Some(_) => Err("wrong type for key".into()),
            None => {
                let list: VecDeque<Vec<u8>> = values.into_iter().rev().collect();
                let len = list.len();
                let val = StorageValue::List(list);
                let mem = val.memory_usage();
                shard.insert(key.to_vec(), val);
                drop(shard);
                self.current_memory
                    .fetch_add(mem, Ordering::Relaxed);
                self.evict_if_needed();
                Ok(len)
            }
        }
    }

    pub fn rpush(&self, key: &[u8], values: Vec<Vec<u8>>) -> Result<usize, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::List(list)) => {
                let added_bytes: usize = values.iter().map(|v| v.len() + std::mem::size_of::<Vec<u8>>()).sum();
                for v in values {
                    list.push_back(v);
                }
                let len = list.len();
                drop(shard);
                self.current_memory.fetch_add(added_bytes, Ordering::Relaxed);
                Ok(len)
            }
            Some(_) => Err("wrong type for key".into()),
            None => {
                let list: VecDeque<Vec<u8>> = values.into_iter().collect();
                let len = list.len();
                let val = StorageValue::List(list);
                let mem = val.memory_usage();
                shard.insert(key.to_vec(), val);
                drop(shard);
                self.current_memory
                    .fetch_add(mem, Ordering::Relaxed);
                self.evict_if_needed();
                Ok(len)
            }
        }
    }

    pub fn lpop(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::List(list)) => {
                if list.is_empty() {
                    return Ok(None);
                }
                let item = list.pop_front().unwrap();
                let removed_size = item.len() + std::mem::size_of::<Vec<u8>>();
                drop(shard);
                self.current_memory
                    .fetch_sub(removed_size, Ordering::Relaxed);
                Ok(Some(item))
            }
            Some(_) => Err("wrong type for key".into()),
            None => Ok(None),
        }
    }

    pub fn rpop(&self, key: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::List(list)) => {
                if list.is_empty() {
                    return Ok(None);
                }
                let item = list.pop_back().unwrap();
                let removed_size = item.len() + std::mem::size_of::<Vec<u8>>();
                drop(shard);
                self.current_memory
                    .fetch_sub(removed_size, Ordering::Relaxed);
                Ok(Some(item))
            }
            Some(_) => Err("wrong type for key".into()),
            None => Ok(None),
        }
    }

    pub fn lrange(&self, key: &[u8], start: i64, stop: i64) -> Result<Vec<Vec<u8>>, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::List(list)) => {
                let len = list.len() as i64;
                let start = if start < 0 { (len + start).max(0) } else { start };
                let stop = if stop < 0 { len + stop } else { stop.min(len - 1) };
                if start > stop || start >= len {
                    return Ok(Vec::new());
                }
                Ok(list.range(start as usize..=stop as usize).cloned().collect())
            }
            Some(_) => Err("wrong type for key".into()),
            None => Ok(Vec::new()),
        }
    }

    pub fn llen(&self, key: &[u8]) -> usize {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return 0;
        }
        match shard.data.get(key) {
            Some(StorageValue::List(list)) => list.len(),
            _ => 0,
        }
    }

    pub fn lindex(&self, key: &[u8], index: i64) -> Result<Option<Vec<u8>>, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::List(list)) => {
                let actual = if index < 0 {
                    (list.len() as i64 + index) as usize
                } else {
                    index as usize
                };
                Ok(list.get(actual).cloned())
            }
            Some(_) => Err("wrong type for key".into()),
            None => Ok(None),
        }
    }

    pub fn lset(&self, key: &[u8], index: i64, value: Vec<u8>) -> Result<(), String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::List(list)) => {
                let actual = if index < 0 {
                    (list.len() as i64 + index) as usize
                } else {
                    index as usize
                };
                if actual >= list.len() {
                    return Err("index out of range".into());
                }
                let old_size = list[actual].len();
                list[actual] = value.clone();
                let new_size = value.len();
                let diff = (new_size as isize) - (old_size as isize);
                drop(shard);
                if diff > 0 {
                    self.current_memory
                        .fetch_add(diff as usize, Ordering::Relaxed);
                } else if diff < 0 {
                    self.current_memory
                        .fetch_sub((-diff) as usize, Ordering::Relaxed);
                }
                Ok(())
            }
            Some(_) => Err("wrong type for key".into()),
            None => Err("key does not exist".into()),
        }
    }

    // -----------------------------------------------------------------------
    // Hash operations
    // -----------------------------------------------------------------------

    pub fn hset(&self, key: &[u8], field: Vec<u8>, value: Vec<u8>) -> Result<bool, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::Hash(map)) => {
                let old_field_val_size = map.get(&field).map_or(0, |v| field.len() + v.len());
                let new_field_val_size = field.len() + value.len();
                let existed = map.insert(field, value).is_some();
                let diff = (new_field_val_size as isize) - (old_field_val_size as isize);
                drop(shard);
                if diff > 0 {
                    self.current_memory.fetch_add(diff as usize, Ordering::Relaxed);
                } else if diff < 0 {
                    self.current_memory.fetch_sub((-diff) as usize, Ordering::Relaxed);
                }
                Ok(!existed)
            }
            Some(_) => Err("wrong type for key".into()),
            None => {
                let mut map = AHashMap::new();
                map.insert(field, value);
                let val = StorageValue::Hash(map);
                let mem = val.memory_usage();
                shard.insert(key.to_vec(), val);
                drop(shard);
                self.current_memory
                    .fetch_add(mem, Ordering::Relaxed);
                self.evict_if_needed();
                Ok(true)
            }
        }
    }

    pub fn hget(&self, key: &[u8], field: &[u8]) -> Result<Option<Vec<u8>>, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::Hash(map)) => Ok(map.get(field).cloned()),
            Some(_) => Err("wrong type for key".into()),
            None => Ok(None),
        }
    }

    pub fn hdel(&self, key: &[u8], field: &[u8]) -> Result<bool, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::Hash(map)) => {
                if let Some(val) = map.remove(field) {
                    let removed_size = field.len() + val.len();
                    drop(shard);
                    self.current_memory
                        .fetch_sub(removed_size, Ordering::Relaxed);
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            Some(_) => Err("wrong type for key".into()),
            None => Ok(false),
        }
    }

    pub fn hgetall(&self, key: &[u8]) -> Result<AHashMap<Vec<u8>, Vec<u8>>, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::Hash(map)) => Ok(map.clone()),
            Some(_) => Err("wrong type for key".into()),
            None => Ok(AHashMap::new()),
        }
    }

    // -----------------------------------------------------------------------
    // Set operations
    // -----------------------------------------------------------------------

    pub fn sadd(&self, key: &[u8], members: Vec<Vec<u8>>) -> Result<usize, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::Set(set)) => {
                let mut added = 0;
                let mut added_bytes = 0usize;
                for m in members {
                    let m_len = m.len() + std::mem::size_of::<Vec<u8>>();
                    if set.insert(m) {
                        added += 1;
                        added_bytes += m_len;
                    }
                }
                if added_bytes > 0 {
                    self.current_memory.fetch_add(added_bytes, Ordering::Relaxed);
                }
                Ok(added)
            }
            Some(_) => Err("wrong type for key".into()),
            None => {
                let mut set = AHashSet::new();
                let mut added = 0;
                for m in members {
                    if set.insert(m) {
                        added += 1;
                    }
                }
                let val = StorageValue::Set(set);
                let mem = val.memory_usage();
                shard.insert(key.to_vec(), val);
                drop(shard);
                self.current_memory
                    .fetch_add(mem, Ordering::Relaxed);
                self.evict_if_needed();
                Ok(added)
            }
        }
    }

    pub fn sismember(&self, key: &[u8], member: &[u8]) -> Result<bool, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::Set(set)) => Ok(set.contains(member)),
            Some(_) => Err("wrong type for key".into()),
            None => Ok(false),
        }
    }

    pub fn smembers(&self, key: &[u8]) -> Result<AHashSet<Vec<u8>>, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::Set(set)) => Ok(set.clone()),
            Some(_) => Err("wrong type for key".into()),
            None => Ok(AHashSet::new()),
        }
    }

    // -----------------------------------------------------------------------
    // Sorted set operations
    // -----------------------------------------------------------------------

    pub fn zadd(&self, key: &[u8], score: i64, member: Vec<u8>) -> Result<bool, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::SortedSet(data)) => {
                let existed = if let Some(old_score) = data.member_index.get(&member).copied() {
                    data.tree.remove(&(old_score, member.clone()));
                    true
                } else {
                    false
                };
                data.member_index.insert(member.clone(), score);
                data.tree.insert((score, member));
                Ok(!existed)
            }
            Some(_) => Err("wrong type for key".into()),
            None => {
                let mut data = SortedSetData::default();
                data.member_index.insert(member.clone(), score);
                data.tree.insert((score, member));
                let val = StorageValue::SortedSet(data);
                let mem = val.memory_usage();
                shard.insert(key.to_vec(), val);
                drop(shard);
                self.current_memory
                    .fetch_add(mem, Ordering::Relaxed);
                self.evict_if_needed();
                Ok(true)
            }
        }
    }

    pub fn zscore(&self, key: &[u8], member: &[u8]) -> Result<Option<i64>, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::SortedSet(data)) => {
                Ok(data.member_index.get(member).copied())
            }
            Some(_) => Err("wrong type for key".into()),
            None => Ok(None),
        }
    }

    pub fn zrange(
        &self,
        key: &[u8],
        start: usize,
        stop: usize,
    ) -> Result<Vec<(i64, Vec<u8>)>, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::SortedSet(data)) => {
                let len = data.tree.len();
                if len == 0 || start >= len {
                    return Ok(Vec::new());
                }
                let end = stop.min(len - 1);
                if start > end {
                    return Ok(Vec::new());
                }
                let count = end - start + 1;
                let items: Vec<(i64, Vec<u8>)> = data.tree.iter()
                    .skip(start)
                    .take(count)
                    .map(|e| e.value().clone())
                    .collect();
                Ok(items)
            }
            Some(_) => Err("wrong type for key".into()),
            None => Ok(Vec::new()),
        }
    }

    pub fn zlen(&self, key: &[u8]) -> usize {
        let shard = match self.get_shard(key).read() {
            shard => shard,
        };
        if shard.is_expired(key) {
            return 0;
        }
        match shard.data.get(key) {
            Some(StorageValue::SortedSet(data)) => data.tree.len(),
            _ => 0,
        }
    }

    // -----------------------------------------------------------------------
    // Stream operations
    // -----------------------------------------------------------------------

    pub fn xadd(&self, key: &[u8], fields: AHashMap<Vec<u8>, Vec<u8>>) -> Result<u64, String> {
        let shard_lock = self.get_shard(key);
        let mut shard = shard_lock.write();

        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }

        match shard.data.get_mut(key) {
            Some(StorageValue::Stream(stream)) => {
                stream.last_entry_id += 1;
                let id = stream.last_entry_id;
                stream.entries.insert(id, StreamEntry { id, fields });
                Ok(id)
            }
            Some(_) => Err("wrong type for key".into()),
            None => {
                let mut stream = StreamData::default();
                stream.last_entry_id = 1;
                stream.entries.insert(1, StreamEntry { id: 1, fields });
                let val = StorageValue::Stream(stream);
                let mem = val.memory_usage();
                shard.insert(key.to_vec(), val);
                drop(shard);
                self.current_memory
                    .fetch_add(mem, Ordering::Relaxed);
                self.evict_if_needed();
                Ok(1)
            }
        }
    }

    pub fn xrange(
        &self,
        key: &[u8],
        start_id: u64,
        end_id: u64,
    ) -> Result<Vec<StreamEntry>, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::Stream(stream)) => Ok(stream
                .entries
                .range(start_id..=end_id)
                .map(|(_id, entry)| entry.clone())
                .collect()),
            Some(_) => Err("wrong type for key".into()),
            None => Ok(Vec::new()),
        }
    }

    pub fn xlen(&self, key: &[u8]) -> Result<usize, String> {
        let shard = self.get_shard(key).read();
        if shard.is_expired(key) {
            return Err("key does not exist".into());
        }
        match shard.data.get(key) {
            Some(StorageValue::Stream(stream)) => Ok(stream.entries.len()),
            Some(_) => Err("wrong type for key".into()),
            None => Ok(0),
        }
    }

    // -----------------------------------------------------------------------
    // Random key sampling
    // -----------------------------------------------------------------------

    pub fn random_key(&self) -> Option<Vec<u8>> {
        let mut rng = rand::thread_rng();
        let mut attempts = 0;
        loop {
            if attempts > self.num_shards * 3 {
                return None;
            }
            let idx = rng.gen_range(0..self.num_shards);
            let shard = self.shards[idx].read();
            let keys: Vec<&Vec<u8>> = shard
                .data
                .keys()
                .filter(|k| !shard.is_expired(k))
                .collect();
            if !keys.is_empty() {
                let chosen = keys[rng.gen_range(0..keys.len())];
                return Some(chosen.clone());
            }
            drop(shard);
            attempts += 1;
        }
    }

    // -----------------------------------------------------------------------
    // Cleanup / maintenance
    // -----------------------------------------------------------------------

    pub fn cleanup_expired(&self) -> usize {
        let mut total_cleaned = 0usize;
        for shard_lock in &self.shards {
            let mut shard = shard_lock.write();
            let expired = shard.cleanup_expired();
            total_cleaned += expired.len();
        }
        total_cleaned
    }

    /// Spawn a background Tokio task that periodically cleans up expired keys.
    pub fn start_cleanup_task(self: &Arc<Self>) {
        let engine = Arc::clone(self);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(CLEANUP_INTERVAL);
            loop {
                interval.tick().await;
                engine.cleanup_expired();
            }
        });
    }

    /// Total number of keys across all shards.
    pub fn dbsize(&self) -> usize {
        self.shards.iter().map(|s| s.read().data.len()).sum()
    }

    /// Current memory usage in bytes.
    pub fn memory_usage(&self) -> usize {
        self.current_memory.load(Ordering::Relaxed)
    }

    /// Set maximum memory limit.
    pub fn set_max_memory(&self, max: usize) {
        self.max_memory.store(max, Ordering::Relaxed);
        self.evict_if_needed();
    }

    /// Set eviction policy.
    pub fn set_eviction_policy(&self, policy: EvictionPolicy) {
        *self.eviction_policy.write() = policy;
    }

    /// Flush all keys.
    pub fn flush_all(&self) {
        for shard_lock in &self.shards {
            let mut shard = shard_lock.write();
            shard.data.clear();
            shard.ttl.clear();
            shard.access_info.clear();
            shard.size_bytes = 0;
        }
        self.current_memory.store(0, Ordering::Relaxed);
    }
}

impl Default for StorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn engine() -> StorageEngine {
        StorageEngine::with_config(16, 1024 * 1024, EvictionPolicy::Lru)
    }

    #[test]
    fn test_set_get_del() {
        let e = engine();
        let key = b"foo".to_vec();
        let val = StorageValue::String(b"bar".to_vec().into());

        e.set(key.clone(), val.clone());
        assert_eq!(e.get(&key), Some(val.clone()));
        assert!(e.exists(&key));

        assert!(e.del(&key));
        assert_eq!(e.get(&key), None);
        assert!(!e.exists(&key));
        assert!(!e.del(&key));
    }

    #[test]
    fn test_mget_mset() {
        let e = engine();
        let pairs: Vec<(Vec<u8>, StorageValue)> = vec![
            (b"a".to_vec(), StorageValue::String(b"1".to_vec().into())),
            (b"b".to_vec(), StorageValue::String(b"2".to_vec().into())),
            (b"c".to_vec(), StorageValue::String(b"3".to_vec().into())),
        ];
        e.mset(pairs);

        let keys: Vec<Vec<u8>> = vec![
            b"a".to_vec(),
            b"b".to_vec(),
            b"c".to_vec(),
            b"d".to_vec(),
        ];
        let results = e.mget(&keys);
        assert_eq!(results[0], Some(StorageValue::String(b"1".to_vec().into())));
        assert_eq!(results[1], Some(StorageValue::String(b"2".to_vec().into())));
        assert_eq!(results[2], Some(StorageValue::String(b"3".to_vec().into())));
        assert_eq!(results[3], None);
    }

    #[test]
    fn test_incr_decr() {
        let e = engine();
        let key = b"counter".to_vec();
        e.set(key.clone(), StorageValue::String(b"10".to_vec().into()));

        assert_eq!(e.incr(&key, 5).unwrap(), 15);
        assert_eq!(e.decr(&key, 3).unwrap(), 12);
        assert_eq!(e.incr(&key, -2).unwrap(), 10);
        assert_eq!(e.decr(&key, -10).unwrap(), 20);

        e.set(
            key.clone(),
            StorageValue::String(i64::MAX.to_string().into_bytes().into()),
        );
        assert!(e.incr(&key, 1).is_err());
    }

    #[test]
    fn test_ttl_expire_persist() {
        let e = engine();
        let key = b"ttl_key".to_vec();
        e.set(key.clone(), StorageValue::String(b"v".to_vec().into()));

        assert!(e.ttl(&key).is_none());
        assert!(e.expire(&key, Duration::from_secs(10)));
        assert!(e.ttl(&key).is_some());
        let remaining = e.ttl(&key).unwrap();
        assert!(remaining <= Duration::from_secs(10));
        assert!(remaining > Duration::from_secs(0));

        assert!(e.persist(&key));
        assert!(e.ttl(&key).is_none());

        assert!(!e.expire(b"noexist", Duration::from_secs(1)));
    }

    #[test]
    fn test_lazy_expiry() {
        let e = engine();
        let key = b"short".to_vec();
        e.set(key.clone(), StorageValue::String(b"v".to_vec().into()));
        e.expire(&key, Duration::from_millis(1));

        assert!(e.get(&key).is_some());

        std::thread::sleep(Duration::from_millis(50));
        assert!(e.get(&key).is_none());
    }

    #[test]
    fn test_cleanup_expired() {
        let e = engine();
        for i in 0..10 {
            let key = format!("k{}", i).into_bytes();
            e.set(key.clone(), StorageValue::String(b"v".to_vec().into()));
            e.expire(&key, Duration::from_millis(1));
        }
        std::thread::sleep(Duration::from_millis(50));
        let cleaned = e.cleanup_expired();
        assert_eq!(cleaned, 10);
        assert_eq!(e.dbsize(), 0);
    }

    #[test]
    fn test_append() {
        let e = engine();
        let key = b"app".to_vec();
        e.set(key.clone(), StorageValue::String(b"hello".to_vec().into()));

        let new_len = e.append(&key, b" world").unwrap();
        assert_eq!(new_len, 11);
        assert_eq!(
            e.get(&key),
            Some(StorageValue::String(b"hello world".to_vec().into()))
        );
    }

    #[test]
    fn test_strlen() {
        let e = engine();
        let key = b"len".to_vec();
        e.set(key.clone(), StorageValue::String(b"hello".to_vec().into()));
        assert_eq!(e.strlen(&key).unwrap(), 5);

        assert!(e.strlen(b"noexist").is_err());
    }

    #[test]
    fn test_getrange_setrange() {
        let e = engine();
        let key = b"range".to_vec();
        e.set(
            key.clone(),
            StorageValue::String(b"Hello, World!".to_vec().into()),
        );

        assert_eq!(e.getrange(&key, 0, 4).unwrap(), b"Hello");
        assert_eq!(e.getrange(&key, -6, -2).unwrap(), b"World");
        assert_eq!(e.getrange(&key, 20, 25).unwrap(), b"");

        e.setrange(&key, 7, b"Rust").unwrap();
        assert_eq!(
            e.get(&key),
            Some(StorageValue::String(b"Hello, Rustd!".to_vec().into()))
        );
    }

    #[test]
    fn test_type_command() {
        let e = engine();
        e.set(b"s".to_vec(), StorageValue::String(b"".to_vec().into()));
        e.lpush(b"l", vec![b"1".to_vec()]).unwrap();
        e.hset(b"h", b"f".to_vec(), b"v".to_vec())
            .unwrap();
        e.sadd(b"se", vec![b"m".to_vec()]).unwrap();
        e.zadd(b"z", 1, b"m".to_vec()).unwrap();

        assert_eq!(e.r#type(b"s"), Some(DataType::String));
        assert_eq!(e.r#type(b"l"), Some(DataType::List));
        assert_eq!(e.r#type(b"h"), Some(DataType::Hash));
        assert_eq!(e.r#type(b"se"), Some(DataType::Set));
        assert_eq!(e.r#type(b"z"), Some(DataType::SortedSet));
        assert_eq!(e.r#type(b"missing"), None);
    }

    #[test]
    fn test_list_ops() {
        let e = engine();
        let key = b"mylist".to_vec();

        let len = e
            .rpush(&key, vec![b"a".to_vec(), b"b".to_vec()])
            .unwrap();
        assert_eq!(len, 2);
        let len = e.lpush(&key, vec![b"c".to_vec()]).unwrap();
        assert_eq!(len, 3);

        assert_eq!(
            e.lrange(&key, 0, -1).unwrap(),
            vec![b"c".to_vec(), b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(e.lindex(&key, 0).unwrap(), Some(b"c".to_vec()));
        assert_eq!(e.lindex(&key, -1).unwrap(), Some(b"b".to_vec()));

        assert_eq!(e.lpop(&key).unwrap(), Some(b"c".to_vec()));
        assert_eq!(e.rpop(&key).unwrap(), Some(b"b".to_vec()));

        e.lset(&key, 0, b"z".to_vec()).unwrap();
        assert_eq!(e.lindex(&key, 0).unwrap(), Some(b"z".to_vec()));
    }

    #[test]
    fn test_hash_ops() {
        let e = engine();
        let key = b"myhash".to_vec();

        assert!(e
            .hset(&key, b"f1".to_vec(), b"v1".to_vec())
            .unwrap());
        assert!(!e
            .hset(&key, b"f1".to_vec(), b"v2".to_vec())
            .unwrap());
        assert_eq!(
            e.hget(&key, b"f1").unwrap(),
            Some(b"v2".to_vec())
        );

        assert!(e.hdel(&key, b"f1").unwrap());
        assert!(!e.hdel(&key, b"f1").unwrap());

        e.hset(&key, b"a".to_vec(), b"1".to_vec()).unwrap();
        e.hset(&key, b"b".to_vec(), b"2".to_vec()).unwrap();
        let all = e.hgetall(&key).unwrap();
        assert_eq!(all.len(), 2);
    }

    #[test]
    fn test_set_ops() {
        let e = engine();
        let key = b"myset".to_vec();

        let added = e
            .sadd(&key, vec![b"a".to_vec(), b"b".to_vec(), b"a".to_vec()])
            .unwrap();
        assert_eq!(added, 2);

        assert!(e.sismember(&key, b"a").unwrap());
        assert!(!e.sismember(&key, b"z").unwrap());

        let members = e.smembers(&key).unwrap();
        assert_eq!(members.len(), 2);
    }

    #[test]
    fn test_sorted_set_ops() {
        let e = engine();
        let key = b"myzset".to_vec();

        e.zadd(&key, 10, b"alice".to_vec()).unwrap();
        e.zadd(&key, 5, b"bob".to_vec()).unwrap();
        e.zadd(&key, 20, b"charlie".to_vec()).unwrap();

        assert_eq!(e.zscore(&key, b"bob").unwrap(), Some(5));
        assert_eq!(e.zscore(&key, b"alice").unwrap(), Some(10));
        assert_eq!(e.zscore(&key, b"nobody").unwrap(), None);

        let range = e.zrange(&key, 0, 1).unwrap();
        assert_eq!(range.len(), 2);
        assert_eq!(range[0].0, 5);
        assert_eq!(range[1].0, 10);

        e.zadd(&key, 100, b"bob".to_vec()).unwrap();
        assert_eq!(e.zscore(&key, b"bob").unwrap(), Some(100));
    }

    #[test]
    fn test_stream_ops() {
        let e = engine();
        let key = b"mystream".to_vec();
        let mut fields = AHashMap::new();
        fields.insert(b"field1".to_vec(), b"val1".to_vec());

        let id1 = e.xadd(&key, fields.clone()).unwrap();
        assert_eq!(id1, 1);

        fields.insert(b"field2".to_vec(), b"val2".to_vec());
        let id2 = e.xadd(&key, fields).unwrap();
        assert_eq!(id2, 2);

        let entries = e.xrange(&key, 1, 2).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].id, 1);
        assert_eq!(entries[1].id, 2);
    }

    #[test]
    fn test_wrong_type_errors() {
        let e = engine();
        e.set(b"str".to_vec(), StorageValue::String(b"val".to_vec().into()));

        assert!(e.incr(b"str", 1).is_err());
        assert!(e.lrange(b"str", 0, -1).is_err());
        assert!(e.hget(b"str", b"f").is_err());
        assert!(e.sismember(b"str", b"m").is_err());
        assert!(e.zscore(b"str", b"m").is_err());
        assert!(e.xadd(b"str", AHashMap::new()).is_err());
    }

    #[test]
    fn test_eviction() {
        let e = StorageEngine::with_config(4, 200, EvictionPolicy::Lru);

        for i in 0..50 {
            let key = format!("key_{}", i).into_bytes();
            let val = StorageValue::String(vec![b'x'; 50].into());
            e.set(key, val);
        }

        assert!(e.memory_usage() < 200);
    }

    #[test]
    fn test_random_key() {
        let e = engine();
        e.set(b"k1".to_vec(), StorageValue::String(b"v".to_vec().into()));
        e.set(b"k2".to_vec(), StorageValue::String(b"v".to_vec().into()));

        let key = e.random_key();
        assert!(key.is_some());
        let k = key.unwrap();
        assert!(k == b"k1" || k == b"k2");
    }

    #[test]
    fn test_flush_all() {
        let e = engine();
        e.set(b"a".to_vec(), StorageValue::String(b"1".to_vec().into()));
        e.set(b"b".to_vec(), StorageValue::String(b"2".to_vec().into()));
        assert_eq!(e.dbsize(), 2);

        e.flush_all();
        assert_eq!(e.dbsize(), 0);
        assert_eq!(e.memory_usage(), 0);
    }

    #[test]
    fn test_set_with_ttl() {
        let e = engine();
        let key = b"ttlset".to_vec();
        e.set_with_ttl(
            key.clone(),
            StorageValue::String(b"v".to_vec().into()),
            Duration::from_secs(60),
        );

        assert!(e.ttl(&key).is_some());
        let remaining = e.ttl(&key).unwrap();
        assert!(remaining <= Duration::from_secs(60));
        assert!(remaining > Duration::from_secs(50));
    }

    #[test]
    fn test_overwrite_key() {
        let e = engine();
        let key = b"ow".to_vec();
        e.set(key.clone(), StorageValue::String(b"first".to_vec().into()));
        assert_eq!(
            e.get(&key),
            Some(StorageValue::String(b"first".to_vec().into()))
        );

        e.set(key.clone(), StorageValue::String(b"second".to_vec().into()));
        assert_eq!(
            e.get(&key),
            Some(StorageValue::String(b"second".to_vec().into()))
        );
    }

    #[test]
    fn test_dbsize_memory() {
        let e = engine();
        assert_eq!(e.dbsize(), 0);
        assert_eq!(e.memory_usage(), 0);

        e.set(b"a".to_vec(), StorageValue::String(b"hello".to_vec().into()));
        e.set(b"b".to_vec(), StorageValue::String(b"world".to_vec().into()));
        assert_eq!(e.dbsize(), 2);
        assert!(e.memory_usage() > 0);
    }
}
