use libc::{flock, LOCK_EX, LOCK_NB, LOCK_UN};
use std::fs::File;
use std::io::{self, Error};
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

pub struct FileLock {
    file: Option<File>,
    path: PathBuf,
}

impl FileLock {
    /// Creates a new FileLock and locks the file.
    pub fn lock<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let file = File::create(&path)?;
        let fd = file.as_raw_fd();

        // Attempt to acquire an exclusive, non-blocking lock.
        let result = unsafe { flock(fd, LOCK_EX | LOCK_NB) };
        if result != 0 {
            return Err(Error::last_os_error());
        }

        Ok(Self {
            file: Some(file),
            path: path.as_ref().to_path_buf(),
        })
    }

    /// Unlocks and removes the lock file.
    pub fn unlock(mut self) -> io::Result<()> {
        if let Some(file) = self.file.take() {
            let fd = file.as_raw_fd();

            // Release the lock.
            let result = unsafe { flock(fd, LOCK_UN) };
            if result != 0 {
                return Err(Error::last_os_error());
            }
        }
        // Remove the lock file.
        std::fs::remove_file(&self.path)?;

        Ok(())
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        if let Some(file) = &self.file {
            let fd = file.as_raw_fd();
            let _ = unsafe { flock(fd, LOCK_UN) };
        }

        // Attempt to remove the lock file.
        if let Err(e) = std::fs::remove_file(&self.path) {
            eprintln!("Failed to remove lock file: {}", e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_and_unlock() {
        let lock_path = "/tmp/testdatabase.lock";

        // Ensure the lock file does not exist initially.
        assert!(!Path::new(lock_path).exists());

        // Acquire the lock.
        let lock = FileLock::lock(lock_path).expect("Failed to acquire lock");

        // Ensure the lock file now exists.
        assert!(Path::new(lock_path).exists());

        // Unlock and remove the lock file.
        lock.unlock().expect("Failed to release lock");

        // Ensure the lock file has been removed.
        assert!(!Path::new(lock_path).exists());
    }

    #[test]
    fn test_double_lock() {
        let lock_path = "/tmp/testdatabase_double.lock";

        // Acquire the first lock.
        let _lock1 = FileLock::lock(lock_path).expect("Failed to acquire first lock");

        // Attempt to acquire a second lock on the same file.
        let lock2 = FileLock::lock(lock_path);

        // Ensure the second lock fails.
        assert!(lock2.is_err());
    }

    #[test]
    fn test_auto_unlock_on_drop() {
        let lock_path = "/tmp/testdatabase_auto.lock";

        {
            // Acquire the lock inside a scoped block.
            let _lock = FileLock::lock(lock_path).expect("Failed to acquire lock");

            // Ensure the lock file exists within the scope.
            assert!(Path::new(lock_path).exists());
        }

        // After the lock goes out of scope, it should be automatically unlocked.
        assert!(!Path::new(lock_path).exists());
    }
}
