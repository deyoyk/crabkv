use std::collections::{BTreeMap, HashMap, VecDeque};

use chrono::{DateTime, Utc};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JobState {
    Pending,
    Running,
    Completed,
    Failed,
    Dead,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    pub queue: String,
    pub payload: Vec<u8>,
    pub state: JobState,
    pub priority: i32,
    pub attempts: u32,
    pub max_attempts: u32,
    pub created_at: DateTime<Utc>,
    pub run_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeadLetterEntry {
    pub job: Job,
    pub died_at: DateTime<Utc>,
    pub reason: String,
}

pub struct JobQueue {
    queues: RwLock<HashMap<String, BTreeMap<i32, VecDeque<Job>>>>,
    delayed: RwLock<HashMap<String, Vec<Job>>>,
    processing: RwLock<HashMap<String, Job>>,
    dead_letters: RwLock<HashMap<String, Vec<DeadLetterEntry>>>,
}

impl JobQueue {
    pub fn new() -> Self {
        Self {
            queues: RwLock::new(HashMap::new()),
            delayed: RwLock::new(HashMap::new()),
            processing: RwLock::new(HashMap::new()),
            dead_letters: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_job(
        &self,
        queue: &str,
        payload: Vec<u8>,
        priority: i32,
        delay_secs: Option<u64>,
    ) -> String {
        let id = Uuid::new_v4().to_string();
        let now = Utc::now();
        let run_at = delay_secs.map(|d| now + chrono::Duration::seconds(d as i64));

        let job = Job {
            id: id.clone(),
            queue: queue.to_string(),
            payload,
            state: JobState::Pending,
            priority,
            attempts: 0,
            max_attempts: 3,
            created_at: now,
            run_at,
            completed_at: None,
            error: None,
        };

        if delay_secs.is_some() {
            let mut delayed = self.delayed.write();
            delayed.entry(queue.to_string()).or_default().push(job);
        } else {
            let mut queues = self.queues.write();
            queues
                .entry(queue.to_string())
                .or_insert_with(BTreeMap::new)
                .entry(priority)
                .or_insert_with(VecDeque::new)
                .push_back(job);
        }

        id
    }

    pub fn pop_job(&self, queue: &str) -> Option<Job> {
        self.promote_delayed(queue);

        let mut queues = self.queues.write();
        if let Some(priority_map) = queues.get_mut(queue) {
            if let Some((_priority, queue_deque)) = priority_map.iter_mut().next() {
                if let Some(mut job) = queue_deque.pop_front() {
                    job.state = JobState::Running;
                    job.attempts += 1;
                    let id = job.id.clone();

                    let mut processing = self.processing.write();
                    processing.insert(id, job.clone());

                    priority_map.retain(|_, deque| !deque.is_empty());

                    return Some(job);
                }
            }
        }
        None
    }

    pub fn ack_job(&self, queue: &str, job_id: &str) -> bool {
        let mut processing = self.processing.write();
        if let Some(mut job) = processing.remove(job_id) {
            if job.queue != queue {
                processing.insert(job_id.to_string(), job);
                return false;
            }
            job.state = JobState::Completed;
            job.completed_at = Some(Utc::now());
            true
        } else {
            false
        }
    }

    pub fn retry_job(&self, queue: &str, job_id: &str) -> bool {
        let mut processing = self.processing.write();
        if let Some(mut job) = processing.remove(job_id) {
            if job.queue != queue {
                processing.insert(job_id.to_string(), job);
                return false;
            }

            if job.attempts >= job.max_attempts {
                job.state = JobState::Dead;
                let entry = DeadLetterEntry {
                    job: job.clone(),
                    died_at: Utc::now(),
                    reason: format!(
                        "Exceeded max attempts ({})",
                        job.max_attempts
                    ),
                };
                let mut dead = self.dead_letters.write();
                dead.entry(queue.to_string()).or_default().push(entry);
                return true;
            }

            job.state = JobState::Pending;
            job.error = None;

            let mut queues = self.queues.write();
            queues
                .entry(queue.to_string())
                .or_insert_with(BTreeMap::new)
                .entry(job.priority)
                .or_insert_with(VecDeque::new)
                .push_back(job);

            true
        } else {
            false
        }
    }

    pub fn fail_job(&self, queue: &str, job_id: &str, reason: &str) -> bool {
        let mut processing = self.processing.write();
        if let Some(mut job) = processing.remove(job_id) {
            if job.queue != queue {
                processing.insert(job_id.to_string(), job);
                return false;
            }

            if job.attempts >= job.max_attempts {
                job.state = JobState::Dead;
                let entry = DeadLetterEntry {
                    job: job.clone(),
                    died_at: Utc::now(),
                    reason: reason.to_string(),
                };
                let mut dead = self.dead_letters.write();
                dead.entry(queue.to_string()).or_default().push(entry);
            } else {
                job.state = JobState::Failed;
                job.error = Some(reason.to_string());

                let mut queues = self.queues.write();
                queues
                    .entry(queue.to_string())
                    .or_insert_with(BTreeMap::new)
                    .entry(job.priority)
                    .or_insert_with(VecDeque::new)
                    .push_back(job);
            }
            true
        } else {
            false
        }
    }

    pub fn get_dead_letters(&self, queue: &str) -> Vec<DeadLetterEntry> {
        let dead = self.dead_letters.read();
        dead.get(queue).cloned().unwrap_or_default()
    }

    pub fn queue_stats(&self, queue: &str) -> (usize, usize, usize) {
        let queues = self.queues.read();
        let processing = self.processing.read();
        let dead = self.dead_letters.read();

        let pending: usize = queues
            .get(queue)
            .map(|m| m.values().map(|d| d.len()).sum())
            .unwrap_or(0);

        let in_processing: usize = processing
            .values()
            .filter(|j| j.queue == queue)
            .count();

        let dead_count: usize = dead.get(queue).map(|v| v.len()).unwrap_or(0);

        (pending, in_processing, dead_count)
    }

    fn promote_delayed(&self, queue: &str) {
        let now = Utc::now();
        let mut delayed = self.delayed.write();
        if let Some(jobs) = delayed.get_mut(queue) {
            let mut promoted = Vec::new();
            jobs.retain(|job| {
                if let Some(run_at) = job.run_at {
                    if run_at <= now {
                        promoted.push(job.clone());
                        false
                    } else {
                        true
                    }
                } else {
                    true
                }
            });

            if !promoted.is_empty() {
                drop(delayed);
                let mut queues = self.queues.write();
                let priority_map = queues
                    .entry(queue.to_string())
                    .or_insert_with(BTreeMap::new);

                for job in promoted {
                    priority_map
                        .entry(job.priority)
                        .or_insert_with(VecDeque::new)
                        .push_back(job);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_and_pop_job() {
        let jq = JobQueue::new();
        let id = jq.add_job("default", b"hello".to_vec(), 0, None);
        let job = jq.pop_job("default").unwrap();
        assert_eq!(job.id, id);
        assert_eq!(job.payload, b"hello");
        assert_eq!(job.state, JobState::Running);
        assert_eq!(job.attempts, 1);
    }

    #[test]
    fn priority_ordering() {
        let jq = JobQueue::new();
        jq.add_job("q", b"low".to_vec(), 10, None);
        jq.add_job("q", b"high".to_vec(), 1, None);
        jq.add_job("q", b"mid".to_vec(), 5, None);

        let j1 = jq.pop_job("q").unwrap();
        assert_eq!(j1.payload, b"high");
        let j2 = jq.pop_job("q").unwrap();
        assert_eq!(j2.payload, b"mid");
        let j3 = jq.pop_job("q").unwrap();
        assert_eq!(j3.payload, b"low");
    }

    #[test]
    fn ack_removes_from_processing() {
        let jq = JobQueue::new();
        let id = jq.add_job("q", b"data".to_vec(), 0, None);
        jq.pop_job("q");
        assert!(jq.ack_job("q", &id));
        let (_, proc, _) = jq.queue_stats("q");
        assert_eq!(proc, 0);
    }

    #[test]
    fn retry_moves_back_to_pending() {
        let jq = JobQueue::new();
        let id = jq.add_job("q", b"data".to_vec(), 0, None);
        jq.pop_job("q");
        assert!(jq.retry_job("q", &id));
        let (pending, proc, _) = jq.queue_stats("q");
        assert_eq!(pending, 1);
        assert_eq!(proc, 0);
    }

    #[test]
    fn job_goes_to_dead_letter_after_max_attempts() {
        let jq = JobQueue::new();
        let id = jq.add_job("q", b"data".to_vec(), 0, None);

        for _ in 0..3 {
            jq.pop_job("q");
        }

        let mut found_dead = false;
        for _ in 0..3 {
            if jq.pop_job("q").is_some() {
                found_dead = true;
            }
        }
        assert!(found_dead);

        jq.fail_job("q", &id, "exhausted");
        let letters = jq.get_dead_letters("q");
        assert_eq!(letters.len(), 1);
        assert_eq!(letters[0].reason, "exhausted");
    }

    #[test]
    fn delayed_job_not_popped_immediately() {
        let jq = JobQueue::new();
        jq.add_job("q", b"delayed".to_vec(), 0, Some(3600));
        assert!(jq.pop_job("q").is_none());
    }

    #[test]
    fn queue_stats() {
        let jq = JobQueue::new();
        jq.add_job("q", b"a".to_vec(), 0, None);
        jq.add_job("q", b"b".to_vec(), 0, None);
        jq.pop_job("q");

        let (pending, processing, dead) = jq.queue_stats("q");
        assert_eq!(pending, 1);
        assert_eq!(processing, 1);
        assert_eq!(dead, 0);
    }
}
