//! Provides a buffered writer for performing aligned, direct I/O.
//!
//! Direct I/O (`O_DIRECT` on Linux) bypasses the operating system's page cache.
//! This can improve performance for applications that manage their own caching (like databases)
//! by reducing CPU overhead and avoiding cache pollution.
//!
//! However, direct I/O imposes strict alignment requirements:
//! 1. The memory buffer's starting address must be aligned to the block size.
//! 2. The number of bytes to write must be a multiple of the block size.
//! 3. The file offset must be a multiple of the block size.
//!
//! This module provides `AlignedWriter`, a wrapper that satisfies these requirements
//! while offering a convenient `std::io::Write` interface. It uses an internal,
//! aligned buffer to stage writes. When the buffer is full or `flush()` is called,
//! it pads the data with zeroes to the next alignment boundary and writes it to the file.
//!
//! # Examples
//!
//! ```no_run
//! use std::fs::{File, OpenOptions};
//! use std::os::unix::fs::OpenOptionsExt;
//! use std::io::Write;
//!
//! // Note: Import the AlignedWriter from its module path.
//! // use my_project::aligned_writer::AlignedWriter;
//!
//! # use std::io;
//! # use std::alloc::{alloc, dealloc, Layout};
//! # use std::ops::{Deref, DerefMut};
//! # const ALIGNMENT: usize = 4096;
//! # pub struct AlignedWriter { file: File, buffer: AlignedBuffer, position: usize }
//! # impl AlignedWriter { pub fn new(file: File, capacity: usize) -> io::Result<Self> { Ok(Self { file, buffer: AlignedBuffer::new(capacity), position: 0, }) } }
//! # impl Write for AlignedWriter { fn write(&mut self, buf: &[u8]) -> io::Result<usize> { Ok(0) } fn flush(&mut self) -> io::Result<()> { Ok(()) } }
//! # impl Drop for AlignedWriter { fn drop(&mut self) { } }
//! # struct AlignedBuffer { data: *mut u8, capacity: usize, layout: Layout }
//! # impl AlignedBuffer { fn new(capacity: usize) -> Self { let layout = Layout::from_size_align(capacity, ALIGNMENT).unwrap(); let data = unsafe { alloc(layout) }; Self { data, capacity, layout } } }
//! # impl Drop for AlignedBuffer { fn drop(&mut self) { unsafe { dealloc(self.data, self.layout) } } }
//! # impl Deref for AlignedBuffer { type Target = [u8]; fn deref(&self) -> &[u8] { unsafe { std::slice::from_raw_parts(self.data, self.capacity) } } }
//! # impl DerefMut for AlignedBuffer { fn deref_mut(&mut self) -> &mut [u8] { unsafe { std::slice::from_raw_parts_mut(self.data, self.capacity) } } }
//!
//! // 1. Open a file with the O_DIRECT flag.
//! let file = OpenOptions::new()
//!     .write(true)
//!     .create(true)
//!     .custom_flags(libc::O_DIRECT)
//!     .open("my_wal.log")
//!     .unwrap();
//!
//! // 2. Create an AlignedWriter with a capacity that is a multiple of the alignment.
//! let mut writer = AlignedWriter::new(file, 8192).unwrap();
//!
//! // 3. Use it like any other `Write` implementation.
//! writer.write_all(b"First log entry.").unwrap();
//! writer.write_all(b"Second log entry.").unwrap();
//!
//! // 4. Flush to ensure all buffered data (including padding) is written to disk.
//! writer.flush().unwrap();
//! ```

use std::alloc::{alloc, dealloc, Layout};
use std::fs::File;
use std::io::{self, Write};
use std::ops::{Deref, DerefMut};

/// The alignment required for direct I/O, typically the block or page size.
/// On modern systems, this is almost always 4096 bytes (4 KiB).
const ALIGNMENT: usize = 4096;

/// A buffered writer that abstracts the complexities of aligned direct I/O.
///
/// This struct wraps a `File` handle and provides a `Write` implementation. Data
/// written to it is first stored in an internal, memory-aligned buffer. When this
/// buffer is full, or when `flush()` is called, its contents are written to the
/// underlying file, padded with zeroes to meet the direct I/O size alignment requirements.
pub struct AlignedWriter {
    /// The underlying file handle. It **must** be opened with the `O_DIRECT`
    /// flag for this writer to function as intended.
    file: File,
    /// The memory-aligned buffer used for staging writes. Data is collected here
    /// before being written to the file in aligned chunks.
    buffer: AlignedBuffer,
    /// A cursor tracking the number of bytes currently written to the internal
    /// `buffer`. It ranges from `0` (empty) to `buffer.capacity` (full).
    /// This is **not** the position in the file, but the position within the buffer.
    position: usize,
}

impl AlignedWriter {
    pub fn new(file: File, capacity: usize) -> io::Result<Self> {
        Ok(Self {
            file,
            buffer: AlignedBuffer::new(capacity),
            position: 0,
        })
    }
}

impl Write for AlignedWriter {
    /// Writes a slice of bytes into the internal buffer.
    ///
    /// This method copies data from `buf` into the writer's internal aligned buffer.
    /// If a write operation would cause the internal buffer to overflow, the buffer
    /// is automatically flushed to the underlying file first.
    ///
    /// If the incoming `buf` is larger than the buffer's total capacity, the largest
    /// possible aligned chunk of `buf` is written directly to the file, bypassing the
    /// internal buffer entirely. Any remaining data is then copied into the buffer.
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Case 1: The incoming data fits in the buffer's remaining space.
        let space_in_buffer = self.buffer.capacity - self.position;
        if buf.len() <= space_in_buffer {
            let end = self.position + buf.len();
            self.buffer[self.position..end].copy_from_slice(buf);
            self.position = end;
            return Ok(buf.len());
        }

        // If we're here, the buffer will overflow. We MUST flush the existing content.
        self.flush()?;

        // Case 2: The incoming data is LARGER than the entire buffer capacity.
        if buf.len() > self.buffer.capacity {
            let aligned_len = (buf.len() / ALIGNMENT) * ALIGNMENT;
            self.file.write_all(&buf[..aligned_len])?;

            let remainder = &buf[aligned_len..];
            self.buffer[..remainder.len()].copy_from_slice(remainder);
            self.position = remainder.len();

            return Ok(buf.len());
        }

        // Case 3: The data now fits in the freshly emptied buffer.
        // This case replaces the original recursive call.
        self.buffer[..buf.len()].copy_from_slice(buf);
        self.position = buf.len();

        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        if self.position == 0 {
            return Ok(());
        }

        let aligned_size = self.position.div_ceil(ALIGNMENT) * ALIGNMENT;

        // Zero-pad the buffer to the next alignment boundary
        for i in self.position..aligned_size {
            self.buffer[i] = 0;
        }

        self.file.write_all(&self.buffer[..aligned_size])?;
        self.position = 0;

        Ok(())
    }
}

unsafe impl Send for AlignedBuffer {}

struct AlignedBuffer {
    data: *mut u8,
    capacity: usize,
    layout: Layout,
}

impl AlignedBuffer {
    fn new(capacity: usize) -> Self {
        let layout = Layout::from_size_align(capacity, 4096).unwrap();
        let data = unsafe { alloc(layout) };

        Self {
            data,
            capacity,
            layout,
        }
    }
}

impl Drop for AlignedBuffer {
    fn drop(&mut self) {
        unsafe { dealloc(self.data, self.layout) }
    }
}

impl Deref for AlignedBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        unsafe { std::slice::from_raw_parts(self.data, self.capacity) }
    }
}

impl DerefMut for AlignedBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        unsafe { std::slice::from_raw_parts_mut(self.data, self.capacity) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tmpfs::NamedTempFile;
    use std::io::Read;

    #[test]
    fn test_aligned_buffer_allocation() {
        let buffer = AlignedBuffer::new(8192);
        assert_eq!(buffer.capacity, 8192);
        assert_eq!(buffer.data as usize % 4096, 0);
    }

    #[test]
    fn test_aligned_buffer_deref() {
        let mut buffer = AlignedBuffer::new(8192);
        buffer[0] = 42;
        buffer[100] = 99;
        assert_eq!(buffer[0], 42);
        assert_eq!(buffer[100], 99);
    }

    #[test]
    fn test_write_small_data() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut writer = AlignedWriter::new(temp_file.reopen().unwrap(), 8192).unwrap();

        let data = b"Hello, World!";
        writer.write_all(data).unwrap();
        writer.flush().unwrap();

        let mut file = temp_file.reopen().unwrap();
        let mut result = vec![0u8; 4096];
        file.read_exact(&mut result).unwrap();

        assert_eq!(&result[..data.len()], data);
        assert!(result[data.len()..].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_write_exact_buffer_size() {
        let temp_file = NamedTempFile::new().unwrap();
        let buffer_size = 8192;
        let mut writer = AlignedWriter::new(temp_file.reopen().unwrap(), buffer_size).unwrap();

        let data = vec![0xAB; buffer_size];
        writer.write_all(&data).unwrap();
        writer.flush().unwrap();

        let mut file = temp_file.reopen().unwrap();
        let mut result = vec![0u8; buffer_size];
        file.read_exact(&mut result).unwrap();

        assert_eq!(result, data);
    }

    #[test]
    fn test_write_larger_than_buffer() {
        let temp_file = NamedTempFile::new().unwrap();
        let buffer_size = 8192;
        let mut writer = AlignedWriter::new(temp_file.reopen().unwrap(), buffer_size).unwrap();

        let data = vec![0xCD; 20000];
        writer.write_all(&data).unwrap();
        writer.flush().unwrap();

        let mut file = temp_file.reopen().unwrap();
        let aligned_size = 20000_usize.div_ceil(4096) * 4096;
        let mut result = vec![0u8; aligned_size];
        file.read_exact(&mut result).unwrap();

        assert_eq!(&result[..20000], &data[..]);
        assert!(result[20000..].iter().all(|&b| b == 0));
    }

    #[test]
    fn test_multiple_writes() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut writer = AlignedWriter::new(temp_file.reopen().unwrap(), 8192).unwrap();

        writer.write_all(b"First").unwrap();
        writer.write_all(b"Second").unwrap();
        writer.write_all(b"Third").unwrap();
        writer.flush().unwrap();

        let mut file = temp_file.reopen().unwrap();
        let mut result = vec![0u8; 4096];
        file.read_exact(&mut result).unwrap();

        let expected = b"FirstSecondThird";
        assert_eq!(&result[..expected.len()], expected);
    }

    #[test]
    fn test_padding_alignment() {
        let temp_file = NamedTempFile::new().unwrap();
        let mut writer = AlignedWriter::new(temp_file.reopen().unwrap(), 8192).unwrap();

        let data = vec![0xFF; 100];
        writer.write_all(&data).unwrap();
        writer.flush().unwrap();

        let metadata = temp_file.as_file().metadata().unwrap();
        assert_eq!(metadata.len(), 4096);
        assert_eq!(metadata.len() % 4096, 0);
    }

    #[test]
    fn test_auto_flush_on_full_buffer() {
        let temp_file = NamedTempFile::new().unwrap();
        let buffer_size = 4096;
        let mut writer = AlignedWriter::new(temp_file.reopen().unwrap(), buffer_size).unwrap();

        let first_write = vec![0xAA; 4000];
        let second_write = vec![0xBB; 200];

        writer.write_all(&first_write).unwrap();
        writer.write_all(&second_write).unwrap();
        writer.flush().unwrap();

        let mut file = temp_file.reopen().unwrap();
        let mut result = vec![0u8; 8192];
        file.read_exact(&mut result).unwrap();

        assert_eq!(&result[..4000], &first_write[..]);
        assert_eq!(&result[4096..4096 + 200], &second_write[..]);
    }
}
