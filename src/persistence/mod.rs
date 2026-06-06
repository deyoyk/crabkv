use std::path::{Path, PathBuf};
use std::fs::{File, OpenOptions};
use std::io::{Write, BufWriter, BufRead, BufReader};
use parking_lot::Mutex;

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
        Ok(Self {
            writer: Mutex::new(BufWriter::new(file)),
            path: path.to_path_buf(),
            fsync_policy,
        })
    }

    pub fn append_command(&self, command: &[u8]) -> Result<(), std::io::Error> {
        let mut writer = self.writer.lock();
        writer.write_all(command)?;
        writer.write_all(b"\n")?;
        writer.flush()?;
        if self.fsync_policy == AOFFsyncPolicy::Always {
            writer.get_ref().sync_data()?;
        }
        Ok(())
    }

    pub fn fsync(&self) -> Result<(), std::io::Error> {
        let writer = self.writer.lock();
        writer.get_ref().sync_data()
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
        std::fs::create_dir_all(&self.data_dir)?;
        if self.mode == PersistenceMode::AOF {
            let aof_path = self.data_dir.join("appendonly.aof");
            let writer = AofWriter::new(&aof_path, self.fsync_policy)?;
            self.aof = Some(Mutex::new(writer));
        }
        Ok(())
    }

    pub fn save_aof_command(&self, command: &[u8]) -> Result<(), std::io::Error> {
        if let Some(aof) = &self.aof {
            aof.lock().append_command(command)
        } else {
            Ok(())
        }
    }

    pub fn create_snapshot(&self, data: &[u8]) -> Result<(), std::io::Error> {
        let tmp_path = self.data_dir.join("dump.rdb.tmp");
        let final_path = self.data_dir.join("dump.rdb");
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(data)?;
            file.sync_all()?;
        }
        std::fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }

    pub fn load_snapshot(&self) -> Result<Option<Vec<u8>>, std::io::Error> {
        let path = self.data_dir.join("dump.rdb");
        if !path.exists() {
            return Ok(None);
        }
        let mut file = File::open(&path)?;
        let mut data = Vec::new();
        use std::io::Read;
        file.read_to_end(&mut data)?;
        Ok(Some(data))
    }

    pub fn replay_aof(&self) -> Result<Vec<Vec<u8>>, std::io::Error> {
        let path = self.data_dir.join("appendonly.aof");
        if !path.exists() {
            return Ok(Vec::new());
        }
        let file = File::open(&path)?;
        let reader = BufReader::new(file);
        let mut commands = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if !line.trim().is_empty() {
                commands.push(line.into_bytes());
            }
        }
        Ok(commands)
    }

    pub fn compact_aof(&self) -> Result<(), std::io::Error> {
        let aof_path = self.data_dir.join("appendonly.aof");
        let tmp_path = self.data_dir.join("appendonly.aof.tmp");
        if !aof_path.exists() {
            return Ok(());
        }
        let file = File::open(&aof_path)?;
        let reader = BufReader::new(file);
        let mut out = File::create(&tmp_path)?;
        for line in reader.lines() {
            let line = line?;
            if !line.trim().is_empty() {
                writeln!(out, "{}", line)?;
            }
        }
        out.sync_all()?;
        std::fs::rename(&tmp_path, &aof_path)?;
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
    fn test_aof_write_and_replay() {
        let dir = temp_dir();
        let mut engine = PersistenceEngine::new(PersistenceMode::AOF, &dir)
            .with_fsync_policy(AOFFsyncPolicy::No);
        engine.init().unwrap();
        engine.save_aof_command(b"SET foo bar").unwrap();
        engine.save_aof_command(b"SET baz qux").unwrap();
        let cmds = engine.replay_aof().unwrap();
        assert_eq!(cmds.len(), 2);
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_snapshot() {
        let dir = temp_dir();
        let engine = PersistenceEngine::new(PersistenceMode::Snapshot, &dir);
        engine.create_snapshot(b"snapshot data").unwrap();
        let data = engine.load_snapshot().unwrap();
        assert_eq!(data, Some(b"snapshot data".to_vec()));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn test_compact() {
        let dir = temp_dir();
        let mut engine = PersistenceEngine::new(PersistenceMode::AOF, &dir)
            .with_fsync_policy(AOFFsyncPolicy::No);
        engine.init().unwrap();
        engine.save_aof_command(b"SET a 1").unwrap();
        engine.save_aof_command(b"SET b 2").unwrap();
        engine.compact_aof().unwrap();
        let cmds = engine.replay_aof().unwrap();
        assert_eq!(cmds.len(), 2);
        fs::remove_dir_all(&dir).ok();
    }
}
