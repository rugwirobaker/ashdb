use std::fs;
use std::path::{Path, PathBuf};

pub struct TempDir {
    path: PathBuf,
}

impl TempDir {
    pub fn new() -> std::io::Result<Self> {
        let base = PathBuf::from("/tmp/ashdb_tests");
        fs::create_dir_all(&base)?;

        let unique_name = format!("test_{}_{}", std::process::id(), rand_suffix());
        let path = base.join(unique_name);

        if path.exists() {
            fs::remove_dir_all(&path)?;
        }
        fs::create_dir(&path)?;

        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

pub struct NamedTempFile {
    path: PathBuf,
}

impl NamedTempFile {
    pub fn new() -> std::io::Result<Self> {
        let temp_dir = PathBuf::from("/tmp/ashdb_tests");
        fs::create_dir_all(&temp_dir)?;

        let unique_name = format!("file_{}_{}", std::process::id(), rand_suffix());
        let path = temp_dir.join(unique_name);

        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn reopen(&self) -> std::io::Result<fs::File> {
        fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&self.path)
    }

    pub fn as_file(&self) -> &Path {
        &self.path
    }
}

impl Drop for NamedTempFile {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn rand_suffix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}
