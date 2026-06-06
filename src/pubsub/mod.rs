use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct Subscriber {
    pub id: u64,
    pub sender: broadcast::Sender<Vec<u8>>,
}

/// A thread-safe Pub/Sub broadcaster supporting exact channel subscriptions,
/// pattern-based subscriptions, and per-client tracking.
///
/// Uses `tokio::sync::broadcast` under the hood so every subscriber on a
/// channel receives every published message.  The `RwLock` wrappers ensure
/// safe concurrent access from arbitrary async tasks.
pub struct PubSubBroadcaster {
    channels: RwLock<HashMap<Vec<u8>, broadcast::Sender<Vec<u8>>>>,
    pattern_channels: RwLock<HashMap<Vec<u8>, broadcast::Sender<Vec<u8>>>>,
    /// Maps a client id to the set of channels it is subscribed to.
    client_subscriptions: RwLock<HashMap<u64, HashSet<Vec<u8>>>>,
    /// Monotonically increasing subscriber id generator.
    next_id: AtomicU64,
}

impl PubSubBroadcaster {
    /// Creates an empty broadcaster.
    pub fn new() -> Self {
        Self {
            channels: RwLock::new(HashMap::new()),
            pattern_channels: RwLock::new(HashMap::new()),
            client_subscriptions: RwLock::new(HashMap::new()),
            next_id: AtomicU64::new(1),
        }
    }

    /// Allocates a fresh subscriber id (useful when you need to track a
    /// client externally).
    pub fn next_subscriber_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Subscribes to a concrete channel.  Returns a [`broadcast::Receiver`]
    /// that will yield every message published to `channel` from this point
    /// forward.
    ///
    /// If the channel does not yet exist it is created transparently.
    pub fn subscribe(&self, channel: &[u8]) -> broadcast::Receiver<Vec<u8>> {
        let mut channels = self.channels.write();
        let sender = channels
            .entry(channel.to_vec())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(256);
                tx
            })
            .clone();
        sender.subscribe()
    }

    /// Subscribes `client_id` to `channel` and records the association so
    /// that [`unsubscribe_all`] can clean up later.
    pub fn subscribe_client(&self, client_id: u64, channel: &[u8]) -> broadcast::Receiver<Vec<u8>> {
        {
            let mut subs = self.client_subscriptions.write();
            subs.entry(client_id)
                .or_default()
                .insert(channel.to_vec());
        }
        self.subscribe(channel)
    }

    /// Subscribes to a pattern channel (for `PSUBSCRIBE`).
    ///
    /// In a full implementation the pattern would be evaluated against
    /// incoming channel names at publish time.  Here we treat each pattern
    /// as its own channel key so that [`publish_pattern`] can target it
    /// directly.
    pub fn psubscribe(&self, pattern: &[u8]) -> broadcast::Receiver<Vec<u8>> {
        let mut channels = self.pattern_channels.write();
        let sender = channels
            .entry(pattern.to_vec())
            .or_insert_with(|| {
                let (tx, _) = broadcast::channel(256);
                tx
            })
            .clone();
        sender.subscribe()
    }

    /// Subscribes `client_id` to a pattern channel and records the
    /// association.
    pub fn psubscribe_client(&self, client_id: u64, pattern: &[u8]) -> broadcast::Receiver<Vec<u8>> {
        {
            let mut subs = self.client_subscriptions.write();
            subs.entry(client_id)
                .or_default()
                .insert(pattern.to_vec());
        }
        self.psubscribe(pattern)
    }

    /// Removes a single channel subscription for `client_id`.
    ///
    /// Returns `true` if the client was subscribed to the channel.
    pub fn unsubscribe(&self, client_id: u64, channel: &[u8]) -> bool {
        let mut subs = self.client_subscriptions.write();
        if let Some(channels) = subs.get_mut(&client_id) {
            channels.remove(channel)
        } else {
            false
        }
    }

    /// Removes **all** channel and pattern subscriptions for `client_id`.
    pub fn unsubscribe_all(&self, client_id: u64) {
        let mut subs = self.client_subscriptions.write();
        subs.remove(&client_id);
    }

    /// Publishes `message` to every subscriber of `channel`.
    ///
    /// Returns the number of subscribers that received the message.
    /// Subscribers whose buffers are full are silently dropped (the
    /// broadcast channel handles this internally).
    pub fn publish(&self, channel: &[u8], message: &[u8]) -> usize {
        let channels = self.channels.read();
        match channels.get(channel) {
            Some(sender) => {
                let _ = sender.send(message.to_vec());
                sender.receiver_count()
            }
            None => 0,
        }
    }

    /// Publishes `message` to a pattern channel.
    ///
    /// Returns the number of receivers that received the message.
    pub fn publish_pattern(&self, pattern: &[u8], message: &[u8]) -> usize {
        let channels = self.pattern_channels.read();
        match channels.get(pattern) {
            Some(sender) => {
                let _ = sender.send(message.to_vec());
                sender.receiver_count()
            }
            None => 0,
        }
    }

    /// Publishes `message` to every subscriber whose channel name matches
    /// `pattern` using simple glob semantics (`*` matches any sequence,
    /// `?` matches a single byte).
    ///
    /// Returns the total number of receivers that received the message.
    pub fn publish_matching(&self, pattern: &[u8], message: &[u8]) -> usize {
        let channels = self.channels.read();
        let mut total = 0usize;
        for (name, sender) in channels.iter() {
            if glob_match(pattern, name) {
                let _ = sender.send(message.to_vec());
                total += sender.receiver_count();
            }
        }
        total
    }

    /// Returns the number of subscribers currently listening on `channel`.
    pub fn subscriber_count(&self, channel: &[u8]) -> usize {
        let channels = self.channels.read();
        channels
            .get(channel)
            .map(|s| s.receiver_count())
            .unwrap_or(0)
    }

    /// Returns the set of channels that `client_id` is subscribed to.
    pub fn client_channels(&self, client_id: u64) -> Vec<Vec<u8>> {
        let subs = self.client_subscriptions.read();
        subs.get(&client_id)
            .map(|s| s.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Removes a channel from the internal map if no active senders remain.
    pub fn cleanup_channel(&self, channel: &[u8]) {
        let mut channels = self.channels.write();
        if let Some(sender) = channels.get(channel) {
            if sender.receiver_count() == 0 {
                channels.remove(channel);
            }
        }
    }

    /// Removes a pattern channel from the internal map if no active senders
    /// remain.
    pub fn cleanup_pattern_channel(&self, pattern: &[u8]) {
        let mut channels = self.pattern_channels.write();
        if let Some(sender) = channels.get(pattern) {
            if sender.receiver_count() == 0 {
                channels.remove(pattern);
            }
        }
    }
}

impl Default for PubSubBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

/// Simple glob matching: `*` matches any sequence of bytes, `?` matches
/// exactly one byte.  Everything else must match literally.
fn glob_match(pattern: &[u8], text: &[u8]) -> bool {
    glob_match_inner(pattern, text, 0, 0)
}

fn glob_match_inner(pattern: &[u8], text: &[u8], pi: usize, ti: usize) -> bool {
    let mut p = pi;
    let mut t = ti;

    loop {
        if p == pattern.len() {
            return t == text.len();
        }

        match pattern[p] {
            b'*' => {
                p += 1;
                // Try matching zero or more characters.
                for skip in 0..=text.len() - t {
                    if glob_match_inner(pattern, text, p, t + skip) {
                        return true;
                    }
                }
                return false;
            }
            b'?' => {
                if t == text.len() {
                    return false;
                }
                p += 1;
                t += 1;
            }
            c => {
                if t == text.len() || text[t] != c {
                    return false;
                }
                p += 1;
                t += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::broadcast::error::RecvError;

    #[test]
    fn publish_no_subscribers_returns_zero() {
        let b = PubSubBroadcaster::new();
        assert_eq!(b.publish(b"chan", b"hello"), 0);
    }

    #[test]
    fn subscribe_and_publish() {
        let b = PubSubBroadcaster::new();
        let mut rx = b.subscribe(b"news");

        b.publish(b"news", b"breaking");
        assert_eq!(rx.try_recv().unwrap(), b"breaking".to_vec());
    }

    #[test]
    fn multiple_subscribers_receive_message() {
        let b = PubSubBroadcaster::new();
        let mut rx1 = b.subscribe(b"chat");
        let mut rx2 = b.subscribe(b"chat");
        let mut rx3 = b.subscribe(b"chat");

        b.publish(b"chat", b"hi");

        assert_eq!(rx1.try_recv().unwrap(), b"hi".to_vec());
        assert_eq!(rx2.try_recv().unwrap(), b"hi".to_vec());
        assert_eq!(rx3.try_recv().unwrap(), b"hi".to_vec());
    }

    #[test]
    fn publish_different_channels_isolated() {
        let b = PubSubBroadcaster::new();
        let mut rx_a = b.subscribe(b"a");
        let mut rx_b = b.subscribe(b"b");

        b.publish(b"a", b"for_a");
        assert_eq!(rx_a.try_recv().unwrap(), b"for_a".to_vec());
        assert!(matches!(rx_b.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
    }

    #[test]
    fn unsubscribe_removes_client_tracking() {
        let b = PubSubBroadcaster::new();
        b.subscribe_client(1, b"ch1");
        b.subscribe_client(1, b"ch2");

        assert!(b.unsubscribe(1, b"ch1"));
        let channels = b.client_channels(1);
        assert_eq!(channels.len(), 1);
        assert!(channels.contains(&b"ch2".to_vec()));

        assert!(!b.unsubscribe(1, b"ch1"));
    }

    #[test]
    fn unsubscribe_all_clears_client() {
        let b = PubSubBroadcaster::new();
        b.subscribe_client(1, b"x");
        b.subscribe_client(1, b"y");
        b.unsubscribe_all(1);
        assert!(b.client_channels(1).is_empty());
    }

    #[test]
    fn subscriber_count() {
        let b = PubSubBroadcaster::new();
        assert_eq!(b.subscriber_count(b"t"), 0);
        let _rx1 = b.subscribe(b"t");
        assert_eq!(b.subscriber_count(b"t"), 1);
        let _rx2 = b.subscribe(b"t");
        assert_eq!(b.subscriber_count(b"t"), 2);
    }

    #[test]
    fn psubscribe_and_publish_pattern() {
        let b = PubSubBroadcaster::new();
        let mut rx = b.psubscribe(b"logs.*");
        b.publish_pattern(b"logs.*", b"entry1");
        assert_eq!(rx.try_recv().unwrap(), b"entry1".to_vec());
    }

    #[test]
    fn publish_matching_glob() {
        let b = PubSubBroadcaster::new();
        let mut rx1 = b.subscribe(b"orders.us");
        let mut rx2 = b.subscribe(b"orders.eu");
        let mut rx3 = b.subscribe(b"metrics.latency");

        let total = b.publish_matching(b"orders.*", b"data");
        assert_eq!(total, 2);
        assert_eq!(rx1.try_recv().unwrap(), b"data".to_vec());
        assert_eq!(rx2.try_recv().unwrap(), b"data".to_vec());
        assert!(matches!(rx3.try_recv(), Err(broadcast::error::TryRecvError::Empty)));
    }

    #[test]
    fn glob_match_basic() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"a*", b"abc"));
        assert!(glob_match(b"*z", b"xyz"));
        assert!(glob_match(b"a?c", b"abc"));
        assert!(!glob_match(b"a?c", b"ac"));
        assert!(glob_match(b"hello", b"hello"));
        assert!(!glob_match(b"hello", b"world"));
        assert!(glob_match(b"orders.*", b"orders.us"));
        assert!(!glob_match(b"orders.*", b"metrics.latency"));
    }

    #[test]
    fn cleanup_removes_empty_channel() {
        let b = PubSubBroadcaster::new();
        {
            let _rx = b.subscribe(b"temp");
            assert_eq!(b.subscriber_count(b"temp"), 1);
        }
        // _rx dropped, receiver_count should be 0 now.
        b.cleanup_channel(b"temp");
        assert!(b.channels.read().get(b"temp".as_ref()).is_none());
    }

    #[test]
    fn default_creates_empty_broadcaster() {
        let b = PubSubBroadcaster::default();
        assert_eq!(b.publish(b"x", b"y"), 0);
    }

    #[test]
    fn next_subscriber_id_monotonic() {
        let b = PubSubBroadcaster::new();
        let id1 = b.next_subscriber_id();
        let id2 = b.next_subscriber_id();
        assert!(id2 > id1);
    }
}
