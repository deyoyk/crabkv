use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};

use parking_lot::Mutex;

const AOF_DIR: &str = "aof";
const SNAPSHOT_DIR: &str = "snapshots";
const AOF_FILE: &str = "appendonly.aof";
const SNAPSHOT_FILE: &str = "dump.rdb";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceMode {
    AOF,
    Snapshot,
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AOFFsyncPolicy {
    Always,
    EverySec,
    No,
}

pub struct AofWriter {
    writer: Mutex<BufWriter<File>>,
    path: PathBuf,
    fsync_policy: AOFFsyncPolicy,
}

impl AofWriter {
    pub fn new(path: &Path, fsync_policy: AOFFsyncPolicy) -> Result<Self, std::io::Error> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
        let writer = Mutex::new(BufWriter::new(file));
        Ok(Self {
            writer,
            path: path.to_path_buf(),
            fsync_policy,
        })
    }

    pub fn append_command(&self, command: &[u8]) -> Result<(), std::io::Error> {
        let mut writer = self.writer.lock();
        writer.write_all(command)?;
        writer.write_all(b"\n")?;
        if self.fsync_policy == AOFFsyncPolicy::Always {
            writer.flush()?;
            let file = writer.get_ref();
            file.sync_all()?;
        }
        Ok(())
    }

    pub fn fsync(&self) -> Result<(), std::io::Error> {
        let mut writer = self.writer.lock();
        writer.flush()?;
        let file = writer.get_ref();
        file.sync_all()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

pub struct PersistenceEngine {
    mode: PersistenceMode,
    aof: Option<Mutex<AofWriter>>,
    data_dir: PathBuf,
    fsync_policy: AOFFsyncPolicy,
}

impl PersistenceEngine {
    pub fn new(mode: PersistenceMode, data_dir: &Path) -> Self {
        Self {
            mode,
            aof: None,
            data_dir: data_dir.to_path_buf(),
            fsync_policy: AOFFsyncPolicy::EverySec,
        }
    }

    pub fn with_fsync_policy(mut self, policy: AOFFsyncPolicy) -> Self {
        self.fsync_policy = policy;
        self
    }

    pub fn init(&mut self) -> Result<(), std::io::Error> {
        match self.mode {
            PersistenceMode::AOF => {
                let aof_dir = self.data_dir.join(AOF_DIR);
                fs::create_dir_all(&aof_dir)?;
                let aof_path = aof_dir.join(AOF_FILE);
                let writer = AofWriter::new(&aof_path, self.fsync_policy)?;
                self.aof = Some(Mutex::new(writer));
            }
            PersistenceMode::Snapshot => {
                let snap_dir = self.data_dir.join(SNAPSHOT_DIR);
                fs::create_dir_all(&snap_dir)?;
            }
            PersistenceMode::Disabled => {}
        }
        Ok(())
    }

    pub fn save_aof_command(&self, command: &[u8]) -> Result<(), std::io::Error> {
        if let Some(ref aof) = self.aof {
            let aof = aof.lock();
            aof.append_command(command)?;
            if self.fsync_policy == AOFFsyncPolicy::EverySec {
                aof.fsync()?;
            }
            Ok(())
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "AOF persistence not enabled",
            ))
        }
    }

    pub fn create_snapshot(&self, data: &[u8]) -> Result<(), std::io::Error> {
        let snap_dir = self.data_dir.join(SNAPSHOT_DIR);
        fs::create_dir_all(&snap_dir)?;

        let tmp_path = snap_dir.join(format!("{}.tmp", SNAPSHOT_FILE));
        let final_path = snap_dir.join(SNAPSHOT_FILE);

        {
            let file = File::create(&tmp_path)?;
            let mut writer = BufWriter::new(file);
            writer.write_all(data)?;
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }

    pub fn load_snapshot(&self) -> Result<Option<Vec<u8>>, std::io::Error> {
        let snap_path = self.data_dir.join(SNAPSHOT_DIR).join(SNAPSHOT_FILE);
        if !snap_path.exists() {
            return Ok(None);
        }
        let data = fs::read(&snap_path)?;
        Ok(Some(data))
    }

    pub fn replay_aof(&self) -> Result<Vec<Vec<u8>>, std::io::Error> {
        let aof_path = self.data_dir.join(AOF_DIR).join(AOF_FILE);
        if !aof_path.exists() {
            return Ok(Vec::new());
        }

        let file = File::open(&aof_path)?;
        let reader = BufReader::new(file);
        let mut commands = Vec::new();

        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            commands.push(line.into_bytes());
        }

        Ok(commands)
    }

    pub fn compact_aof(&self) -> Result<(), std::io::Error> {
        let aof_dir = self.data_dir.join(AOF_DIR);
        let aof_path = aof_dir.join(AOF_FILE);

        if !aof_path.exists() {
            return Ok(());
        }

        let commands = self.replay_aof()?;
        let compacted_path = aof_dir.join(format!("{}.compact", AOF_FILE));
        let tmp_path = aof_dir.join(format!("{}.tmp", AOF_FILE));

        {
            let file = File::create(&tmp_path)?;
            let mut writer = BufWriter::new(file);
            for cmd in &commands {
                writer.write_all(cmd)?;
                writer.write_all(b"\n")?;
            }
            writer.flush()?;
            writer.get_ref().sync_all()?;
        }

        fs::rename(&tmp_path, &compacted_path)?;
        fs::remove_file(&aof_path)?;
        fs::rename(&compacted_path, &aof_path)?;

        if let Some(ref aof) = self.aof {
            let mut aof = aof.lock();
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&aof_path)?;
            *aof = AofWriter {
                writer: Mutex::new(BufWriter::new(file)),
                path: aof_path,
                fsync_policy: self.fsync_policy,
            };
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_dir() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("crabkv_test_{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn aof_writer_creates_file_and_appends() {
        let dir = temp_dir();
        let aof_path = dir.join("test.aof");

        let writer = AofWriter::new(&aof_path, AOFFsyncPolicy::No).unwrap();
        writer.append_command(b"SET key1 value1").unwrap();
        writer.append_command(b"SET key2 value2").unwrap();
        drop(writer);

        let content = fs::read_to_string(&aof_path).unwrap();
        assert_eq!(content, "SET key1 value1\nSET key2 value2\n");

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn aof_writer_fsync_always() {
        let dir = temp_dir();
        let aof_path = dir.join("test.aof");

        let writer = AofWriter::new(&aof_path, AOFFsyncPolicy::Always).unwrap();
        writer.append_command(b"SET x 1").unwrap();
        writer.append_command(b"SET y 2").unwrap();

        let content = fs::read_to_string(&aof_path).unwrap();
        assert!(content.contains("SET x 1"));
        assert!(content.contains("SET y 2"));

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn aof_replay() {
        let dir = temp_dir();
        let mut engine = PersistenceEngine::new(PersistenceMode::AOF, &dir)
            .with_fsync_policy(AOFFsyncPolicy::No);
        engine.init().unwrap();

        engine.save_aof_command(b"SET a 1").unwrap();
        engine.save_aof_command(b"SET b 2").unwrap();
        engine.save_aof_command(b"DEL a").unwrap();

        let commands = engine.replay_aof().unwrap();
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0], b"SET a 1");
        assert_eq!(commands[1], b"SET b 2");
        assert_eq!(commands[2], b"DEL a");

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn snapshot_create_and_load() {
        let dir = temp_dir();
        let engine = PersistenceEngine::new(PersistenceMode::Snapshot, &dir);
        engine.init().unwrap();

        let data = b"snapshot-data-v1";
        engine.create_snapshot(data).unwrap();

        let loaded = engine.load_snapshot().unwrap();
        assert_eq!(loaded, Some(data.to_vec()));

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn load_snapshot_returns_none_when_missing() {
        let dir = temp_dir();
        let engine = PersistenceEngine::new(PersistenceMode::Snapshot, &dir);
        engine.init().unwrap();

        let loaded = engine.load_snapshot().unwrap();
        assert_eq!(loaded, None);

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn compact_aof_removes_duplicates() {
        let dir = temp_dir();
        let mut engine = PersistenceEngine::new(PersistenceMode::AOF, &dir)
            .with_fsync_policy(AOFFsyncPolicy::No);
        engine.init().unwrap();

        engine.save_aof_command(b"SET x 1").unwrap();
        engine.save_aof_command(b"SET x 2").unwrap();
        engine.save_aof_command(b"SET y 1").unwrap();

        engine.compact_aof().unwrap();

        let commands = engine.replay_aof().unwrap();
        assert_eq!(commands.len(), 3);
        assert_eq!(commands[0], b"SET x 1");
        assert_eq!(commands[1], b"SET x 2");
        assert_eq!(commands[2], b"SET y 1");

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn disabled_mode_no_aof() {
        let dir = temp_dir();
        let engine = PersistenceEngine::new(PersistenceMode::Disabled, &dir);
        let result = engine.save_aof_command(b"SET x 1");
        assert!(result.is_err());

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn empty_aof_replay() {
        let dir = temp_dir();
        let mut engine = PersistenceEngine::new(PersistenceMode::AOF, &dir);
        engine.init().unwrap();

        let commands = engine.replay_aof().unwrap();
        assert!(commands.is_empty());

        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn persistence_mode_variants() {
        assert_ne!(PersistenceMode::AOF, PersistenceMode::Snapshot);
        assert_ne!(PersistenceMode::AOF, PersistenceMode::Disabled);
        assert_ne!(PersistenceMode::Snapshot, PersistenceMode::Disabled);
    }

    #[test]
    fn fsync_policy_variants() {
        assert_ne!(AOFFsyncPolicy::Always, AOFFsyncPolicy::EverySec);
        assert_ne!(AOFFsyncPolicy::Always, AOFFsyncPolicy::No);
        assert_ne!(AOFFsyncPolicy::EverySec, AOFFsyncPolicy::No);
    }
}
