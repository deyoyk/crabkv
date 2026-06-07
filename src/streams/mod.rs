use ahash::AHashMap;

use crate::storage::StorageEngine;

pub struct StreamEntry {
    pub id: u64,
    pub fields: AHashMap<Vec<u8>, Vec<u8>>,
}

pub struct StreamEngine;

impl StreamEngine {
    pub fn new() -> Self {
        Self
    }

    pub fn xadd(
        &self,
        storage: &StorageEngine,
        stream: &[u8],
        fields: AHashMap<Vec<u8>, Vec<u8>>,
    ) -> Result<u64, String> {
        storage.xadd(stream, fields)
    }

    pub fn xread(
        &self,
        storage: &StorageEngine,
        streams: &[&[u8]],
        count: Option<usize>,
    ) -> Vec<(Vec<u8>, Vec<StreamEntry>)> {
        let mut results = Vec::new();
        let limit = count.unwrap_or(usize::MAX);

        for stream_name in streams {
            if let Ok(entries) = storage.xrange(stream_name, 0, u64::MAX) {
                let collected: Vec<StreamEntry> = entries
                    .into_iter()
                    .take(limit)
                    .map(|e| StreamEntry {
                        id: e.id,
                        fields: e.fields,
                    })
                    .collect();
                if !collected.is_empty() {
                    results.push((stream_name.to_vec(), collected));
                }
            }
        }

        results
    }

    pub fn xlen(&self, storage: &StorageEngine, stream: &[u8]) -> usize {
        storage.xlen(stream).unwrap_or(0)
    }
}

impl Default for StreamEngine {
    fn default() -> Self {
        Self::new()
    }
}
