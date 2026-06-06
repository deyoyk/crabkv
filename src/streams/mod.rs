use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Auto-generated id in the form `"millisecondsSinceEpoch-sequence"`.
    pub id: String,
    /// Arbitrary field-value pairs.
    pub fields: Vec<(Vec<u8>, Vec<u8>)>,
    /// Wall-clock time when the entry was created.
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct ConsumerGroup {
    /// User-chosen group name.
    pub name: Vec<u8>,
    /// The id of the last entry that was delivered to this group.
    pub last_delivered_id: String,
    /// Entries that have been delivered but not yet acknowledged.
    pub pending: HashMap<String, PendingEntry>,
}

#[derive(Debug, Clone)]
pub struct PendingEntry {
    pub id: String,
    /// The consumer within the group that received this entry.
    pub consumer: Vec<u8>,
    /// How many times this entry has been delivered (for redelivery).
    pub delivery_count: u64,
    pub last_delivery: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct StreamData {
    pub entries: VecDeque<StreamEntry>,
    pub groups: HashMap<Vec<u8>, ConsumerGroup>,
    /// Optional maximum length; when set the oldest entries are trimmed.
    pub max_len: Option<usize>,
}

/// A Kafka-lite streaming engine backed by a concurrent hash-map of streams.
///
/// Each stream is an append-only deque of [`StreamEntry`] values with
/// auto-generated ids (`"<ms>-<seq>"`).  Consumer groups track per-group
/// offsets and pending (unacknowledged) entries.
pub struct StreamEngine {
    streams: RwLock<HashMap<Vec<u8>, StreamData>>,
}

impl StreamEngine {
    pub fn new() -> Self {
        Self {
            streams: RwLock::new(HashMap::new()),
        }
    }

    /// Appends a new entry to the stream at `key`.
    ///
    /// Returns the auto-generated entry id.
    ///
    /// If the stream does not exist it is created.  When `max_len` is
    /// provided the stream is trimmed to at most that many entries (oldest
    /// removed first).
    pub fn xadd(
        &self,
        key: &[u8],
        fields: Vec<(Vec<u8>, Vec<u8>)>,
        max_len: Option<usize>,
    ) -> String {
        let id = Self::generate_id();

        let mut streams = self.streams.write();
        let entry = StreamEntry {
            id: id.clone(),
            fields,
            timestamp: Utc::now(),
        };

        let stream = streams.entry(key.to_vec()).or_insert_with(|| StreamData {
            entries: VecDeque::new(),
            groups: HashMap::new(),
            max_len,
        });

        // Apply max_len from the call if the stream doesn't already have one,
        // or honour the stricter of the two.
        if let Some(limit) = max_len {
            stream.max_len = Some(limit.min(stream.max_len.unwrap_or(usize::MAX)));
        }

        stream.entries.push_back(entry);

        // Trim if necessary.
        if let Some(limit) = stream.max_len {
            while stream.entries.len() > limit {
                stream.entries.pop_front();
            }
        }

        id
    }

    /// Reads entries from one or more streams.
    ///
    /// `ids` must have the same length as `keys`.  An id of `"$"` means
    /// "only new entries after now"; any other value is treated as a
    /// minimum entry id (inclusive).
    ///
    /// Returns at most `count` entries per stream (if `count` is `Some`).
    pub fn xread(
        &self,
        keys: &[Vec<u8>],
        ids: &[Vec<u8>],
        count: Option<usize>,
    ) -> Vec<(Vec<u8>, StreamEntry)> {
        assert_eq!(keys.len(), ids.len(), "keys and ids must have the same length");

        let streams = self.streams.read();
        let mut results = Vec::new();

        for (key, id) in keys.iter().zip(ids.iter()) {
            if let Some(stream) = streams.get(key.as_slice()) {
                let after_id = if id == b"$" {
                    // "$" means only new entries – in a blocking read this
                    // would wait, but for a non-blocking snapshot we return
                    // nothing.
                    continue;
                } else {
                    std::str::from_utf8(id).unwrap_or("0-0")
                };

                let limit = count.unwrap_or(usize::MAX);
                let mut taken = 0;

                for entry in &stream.entries {
                    if taken >= limit {
                        break;
                    }
                    if entry.id.as_str() > after_id || entry.id.as_str() == after_id {
                        results.push((key.clone(), entry.clone()));
                        taken += 1;
                    }
                }
            }
        }

        results
    }

    /// Creates a new consumer group for `key` with initial offset `"0-0"`.
    ///
    /// Returns an error if the group already exists.
    pub fn xgroup_create(&self, key: &[u8], group: &[u8]) -> Result<(), String> {
        let mut streams = self.streams.write();
        let stream = streams
            .entry(key.to_vec())
            .or_insert_with(|| StreamData {
                entries: VecDeque::new(),
                groups: HashMap::new(),
                max_len: None,
            });

        if stream.groups.contains_key(group) {
            return Err(format!(
                "BUSYGROUP Consumer Group name already exists"
            ));
        }

        stream.groups.insert(
            group.to_vec(),
            ConsumerGroup {
                name: group.to_vec(),
                last_delivered_id: "0-0".to_vec(),
                pending: HashMap::new(),
            },
        );

        Ok(())
    }

    /// Reads entries for a consumer group.
    ///
    /// Unlike `xread`, the id is always `">"` (new entries only) and
    /// entries are tracked as pending for the given consumer.
    ///
    /// Returns at most `count` entries total across the requested streams.
    pub fn xreadgroup(
        &self,
        group: &[u8],
        consumer: &[u8],
        keys: &[Vec<u8>],
        count: Option<usize>,
    ) -> Vec<(Vec<u8>, StreamEntry)> {
        let mut streams = self.streams.write();
        let mut results = Vec::new();
        let limit = count.unwrap_or(usize::MAX);

        for key in keys {
            let stream = match streams.get_mut(key.as_slice()) {
                Some(s) => s,
                None => continue,
            };

            let cg = match stream.groups.get_mut(group) {
                Some(g) => g,
                None => continue,
            };

            let last_id = cg.last_delivered_id.clone();

            for entry in &stream.entries {
                if results.len() >= limit {
                    break;
                }
                if entry.id.as_str() > last_id.as_str() {
                    // Track as pending.
                    cg.pending.insert(
                        entry.id.clone(),
                        PendingEntry {
                            id: entry.id.clone(),
                            consumer: consumer.to_vec(),
                            delivery_count: 1,
                            last_delivery: Utc::now(),
                        },
                    );
                    cg.last_delivered_id = entry.id.clone();
                    results.push((key.clone(), entry.clone()));
                }
            }
        }

        results
    }

    /// Acknowledges receipt of a single entry.
    ///
    /// Returns `true` if the entry was pending and has been removed.
    pub fn xack(&self, key: &[u8], group: &[u8], id: &str) -> bool {
        let mut streams = self.streams.write();
        if let Some(stream) = streams.get_mut(key.as_slice()) {
            if let Some(cg) = stream.groups.get_mut(group) {
                return cg.pending.remove(id).is_some();
            }
        }
        false
    }

    /// Returns all pending entries for a consumer group.
    pub fn xpending(&self, key: &[u8], group: &[u8]) -> Vec<PendingEntry> {
        let streams = self.streams.read();
        if let Some(stream) = streams.get(key.as_slice()) {
            if let Some(cg) = stream.groups.get(group) {
                return cg.pending.values().cloned().collect();
            }
        }
        Vec::new()
    }

    /// Returns entries whose ids fall within `[start, end]` (inclusive).
    ///
    /// Both `start` and `end` are string-compared against entry ids, so the
    /// `"<ms>-<seq>"` format naturally sorts chronologically.
    pub fn xrange(&self, key: &[u8], start: &str, end: &str) -> Vec<StreamEntry> {
        let streams = self.streams.read();
        if let Some(stream) = streams.get(key.as_slice()) {
            stream
                .entries
                .iter()
                .filter(|e| e.id.as_str() >= start && e.id.as_str() <= end)
                .cloned()
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Returns the number of entries in a stream.
    pub fn xlen(&self, key: &[u8]) -> usize {
        let streams = self.streams.read();
        streams
            .get(key.as_slice())
            .map(|s| s.entries.len())
            .unwrap_or(0)
    }

    /// Generates a stream entry id: `"<ms_since_epoch>-<seq>"`.
    fn generate_id() -> String {
        let ts = Utc::now().timestamp_millis();
        let seq: u32 = rand::random();
        format!("{}-{}", ts, seq)
    }
}

impl Default for StreamEngine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xadd_and_xlen() {
        let engine = StreamEngine::new();
        assert_eq!(engine.xlen(b"mykey"), 0);

        engine.xadd(b"mykey", vec![(b"f1".to_vec(), b"v1".to_vec())], None);
        assert_eq!(engine.xlen(b"mykey"), 1);

        engine.xadd(b"mykey", vec![(b"f2".to_vec(), b"v2".to_vec())], None);
        assert_eq!(engine.xlen(b"mykey"), 2);
    }

    #[test]
    fn xadd_generates_unique_ids() {
        let engine = StreamEngine::new();
        let id1 = engine.xadd(b"k", vec![], None);
        let id2 = engine.xadd(b"k", vec![], None);
        assert_ne!(id1, id2);
    }

    #[test]
    fn xadd_max_len_trims() {
        let engine = StreamEngine::new();
        for i in 0..10 {
            engine.xadd(b"trim", vec![(b"i".to_vec(), i.to_string().into_bytes())], Some(5));
        }
        assert_eq!(engine.xlen(b"trim"), 5);
    }

    #[test]
    fn xread_after_xadd() {
        let engine = StreamEngine::new();
        engine.xadd(b"s1", vec![(b"a".to_vec(), b"1".to_vec())], None);
        engine.xadd(b"s1", vec![(b"a".to_vec(), b"2".to_vec())], None);

        let results = engine.xread(&[b"s1".to_vec()], &[b"0-0".to_vec()], None);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].1.fields[0].1, b"1".to_vec());
        assert_eq!(results[1].1.fields[0].1, b"2".to_vec());
    }

    #[test]
    fn xread_with_count() {
        let engine = StreamEngine::new();
        for i in 0..5 {
            engine.xadd(b"c", vec![(b"v".to_vec(), i.to_string().into_bytes())], None);
        }

        let results = engine.xread(&[b"c".to_vec()], &[b"0-0".to_vec()], Some(3));
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn xread_dollar_returns_empty() {
        let engine = StreamEngine::new();
        engine.xadd(b"s", vec![], None);
        let results = engine.xread(&[b"s".to_vec()], &[b"$".to_vec()], None);
        assert!(results.is_empty());
    }

    #[test]
    fn xgroup_create_and_xreadgroup() {
        let engine = StreamEngine::new();
        engine.xadd(b"g1", vec![(b"f".to_vec(), b"val".to_vec())], None);

        assert!(engine.xgroup_create(b"g1", b"mygroup").is_ok());

        let results =
            engine.xreadgroup(b"mygroup", b"consumer1", &[b"g1".to_vec()], None);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].1.fields[0].1, b"val".to_vec());
    }

    #[test]
    fn xgroup_create_duplicate_errors() {
        let engine = StreamEngine::new();
        assert!(engine.xgroup_create(b"k", b"g").is_ok());
        let err = engine.xgroup_create(b"k", b"g");
        assert!(err.is_err());
        assert!(err.unwrap_err().contains("BUSYGROUP"));
    }

    #[test]
    fn xack_removes_pending() {
        let engine = StreamEngine::new();
        engine.xadd(b"a", vec![], None);
        engine.xgroup_create(b"a", b"grp").unwrap();
        engine.xreadgroup(b"grp", b"c1", &[b"a".to_vec()], None);

        let pending = engine.xpending(b"a", b"grp");
        assert_eq!(pending.len(), 1);

        let id = pending[0].id.clone();
        assert!(engine.xack(b"a", b"grp", &id));
        assert!(engine.xpending(b"a", b"grp").is_empty());
    }

    #[test]
    fn xack_nonexistent_returns_false() {
        let engine = StreamEngine::new();
        assert!(!engine.xack(b"x", b"y", "no-such-id"));
    }

    #[test]
    fn xrange_filters_by_id() {
        let engine = StreamEngine::new();
        engine.xadd(b"r", vec![(b"x".to_vec(), b"1".to_vec())], None);
        engine.xadd(b"r", vec![(b"x".to_vec(), b"2".to_vec())], None);
        engine.xadd(b"r", vec![(b"x".to_vec(), b"3".to_vec())], None);

        let entries = engine.xrange(b"r", "0-0", "9999999999999-0");
        assert_eq!(entries.len(), 3);
    }

    #[test]
    fn xrange_empty_key_returns_empty() {
        let engine = StreamEngine::new();
        assert!(engine.xrange(b"nope", "0-0", "9-0").is_empty());
    }

    #[test]
    fn xreadgroup_only_returns_new_entries() {
        let engine = StreamEngine::new();
        engine.xadd(b"n", vec![], None);
        engine.xgroup_create(b"n", b"g").unwrap();

        // First read delivers the one entry.
        let r1 = engine.xreadgroup(b"g", b"c", &[b"n".to_vec()], None);
        assert_eq!(r1.len(), 1);

        // Second read returns nothing (no new entries since last delivered id).
        let r2 = engine.xreadgroup(b"g", b"c", &[b"n".to_vec()], None);
        assert!(r2.is_empty());

        // Add a new entry.
        engine.xadd(b"n", vec![], None);
        let r3 = engine.xreadgroup(b"g", b"c", &[b"n".to_vec()], None);
        assert_eq!(r3.len(), 1);
    }

    #[test]
    fn xpending_tracks_delivery_count() {
        let engine = StreamEngine::new();
        engine.xadd(b"p", vec![], None);
        engine.xgroup_create(b"p", b"grp").unwrap();
        engine.xreadgroup(b"grp", b"cons", &[b"p".to_vec()], None);

        let pending = engine.xpending(b"p", b"grp");
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].delivery_count, 1);
        assert_eq!(pending[0].consumer, b"cons".to_vec());
    }

    #[test]
    fn default_creates_empty_engine() {
        let engine = StreamEngine::default();
        assert_eq!(engine.xlen(b"anything"), 0);
    }

    #[test]
    fn xreadgroup_nonexistent_group_returns_empty() {
        let engine = StreamEngine::new();
        engine.xadd(b"x", vec![], None);
        let results = engine.xreadgroup(b"nope", b"c", &[b"x".to_vec()], None);
        assert!(results.is_empty());
    }

    #[test]
    fn multiple_streams_xread() {
        let engine = StreamEngine::new();
        engine.xadd(b"s1", vec![(b"k".to_vec(), b"v1".to_vec())], None);
        engine.xadd(b"s2", vec![(b"k".to_vec(), b"v2".to_vec())], None);

        let results = engine.xread(
            &[b"s1".to_vec(), b"s2".to_vec()],
            &[b"0-0".to_vec(), b"0-0".to_vec()],
            None,
        );
        assert_eq!(results.len(), 2);
    }
}
