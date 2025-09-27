use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

#[cfg(unix)]
use std::os::unix::io::AsRawFd;

#[cfg(windows)]
use std::os::windows::io::AsRawHandle;

pub struct FileLock {
    _file: File,
    path: PathBuf,
}

impl FileLock {
    /// Creates a new FileLock and locks the file.
    /// The lock file contains the process ID for debugging purposes.
    pub fn lock<P: AsRef<Path>>(path: P) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();

        // Create the file if it doesn't exist, or open it if it does
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        // Try to acquire platform-specific lock
        Self::try_lock(&file)?;

        // Write process ID to the lock file for debugging
        writeln!(file, "{}", std::process::id())?;
        file.flush()?;

        Ok(Self { _file: file, path })
    }

    /// Platform-specific lock acquisition
    #[cfg(unix)]
    fn try_lock(file: &File) -> io::Result<()> {
        use libc::{flock, LOCK_EX, LOCK_NB};

        let fd = file.as_raw_fd();
        let result = unsafe { flock(fd, LOCK_EX | LOCK_NB) };
        if result != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(windows)]
    fn try_lock(file: &File) -> io::Result<()> {
        use std::os::windows::io::AsRawHandle;
        use winapi::um::fileapi::LockFileEx;
        use winapi::um::winnt::{LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY};

        let handle = file.as_raw_handle();
        let result = unsafe {
            LockFileEx(
                handle as *mut _,
                LOCKFILE_EXCLUSIVE_LOCK | LOCKFILE_FAIL_IMMEDIATELY,
                0,
                !0,
                !0,
                std::ptr::null_mut(),
            )
        };

        if result == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    fn try_lock(_file: &File) -> io::Result<()> {
        // Fallback for other platforms - just succeed
        // This is not ideal but allows compilation on unsupported platforms
        Ok(())
    }

    /// Manually unlock the file.
    /// Note: The lock is automatically released when the FileLock is dropped.
    pub fn unlock(self) -> io::Result<()> {
        // The file will be unlocked automatically when dropped
        // We don't remove the lock file to avoid race conditions
        Ok(())
    }

    /// Get the path of the lock file
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        // File lock is automatically released when the file is closed
        // We don't need to manually unlock or remove the file
        // The OS will handle cleanup when the process exits
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_lock_and_unlock() {
        let lock_path = "/tmp/testdatabase.lock";

        // Clean up any existing lock file
        let _ = fs::remove_file(lock_path);

        // Acquire the lock.
        let lock = FileLock::lock(lock_path).expect("Failed to acquire lock");

        // Ensure the lock file now exists and contains process ID
        assert!(Path::new(lock_path).exists());
        let content = fs::read_to_string(lock_path).expect("Failed to read lock file");
        assert!(content.contains(&std::process::id().to_string()));

        // Unlock (file remains but lock is released)
        lock.unlock().expect("Failed to release lock");

        // Clean up
        let _ = fs::remove_file(lock_path);
    }

    #[test]
    fn test_double_lock() {
        let lock_path = "/tmp/testdatabase_double.lock";

        // Clean up any existing lock file
        let _ = fs::remove_file(lock_path);

        // Acquire the first lock.
        let _lock1 = FileLock::lock(lock_path).expect("Failed to acquire first lock");

        // Attempt to acquire a second lock on the same file.
        let lock2 = FileLock::lock(lock_path);

        // Ensure the second lock fails.
        assert!(lock2.is_err());

        // Clean up
        drop(_lock1);
        let _ = fs::remove_file(lock_path);
    }

    #[test]
    fn test_auto_unlock_on_drop() {
        let lock_path = "/tmp/testdatabase_auto.lock";

        // Clean up any existing lock file
        let _ = fs::remove_file(lock_path);

        {
            // Acquire the lock inside a scoped block.
            let _lock = FileLock::lock(lock_path).expect("Failed to acquire lock");

            // Ensure the lock file exists within the scope.
            assert!(Path::new(lock_path).exists());
        }

        // After the lock goes out of scope, we should be able to acquire it again
        // (proving the lock was released even though the file still exists)
        let _lock2 = FileLock::lock(lock_path).expect("Should be able to acquire lock after drop");

        // Clean up
        let _ = fs::remove_file(lock_path);
    }

    #[test]
    fn test_lock_path() {
        let lock_path = "/tmp/testdatabase_path.lock";

        // Clean up any existing lock file
        let _ = fs::remove_file(lock_path);

        let lock = FileLock::lock(lock_path).expect("Failed to acquire lock");
        assert_eq!(lock.path(), Path::new(lock_path));

        // Clean up
        let _ = fs::remove_file(lock_path);
    }
}
