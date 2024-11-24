use std::{
    fmt::Debug,
    io::{self, Write},
};

/// A writer that writes to multiple destinations simultaneously.
pub struct MultiWriter<W: Write> {
    pub writers: Vec<W>,
}

impl Debug for MultiWriter<Box<dyn Write>> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiWriter")
            .field("writers", &self.writers.len())
            .finish()
    }
}

impl<W: Write> MultiWriter<W> {
    /// Creates a new `MultiWriter` with an initial set of writers.
    pub fn new(writers: Vec<W>) -> Self {
        Self { writers }
    }

    /// Adds a new writer dynamically.
    pub fn add_writer(&mut self, writer: W) {
        self.writers.push(writer);
    }

    /// Returns a reference to a writer by index.
    pub fn get(&self, index: usize) -> Option<&W> {
        self.writers.get(index)
    }

    /// Returns a mutable reference to a writer by index.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut W> {
        self.writers.get_mut(index)
    }
}

impl<W: Write> Write for MultiWriter<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        for (i, writer) in self.writers.iter_mut().enumerate() {
            writer
                .write_all(buf)
                .map_err(|e| io::Error::new(e.kind(), format!("Writer {} failed: {}", i, e)))?;
        }
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        for (i, writer) in self.writers.iter_mut().enumerate() {
            writer.flush().map_err(|e| {
                io::Error::new(e.kind(), format!("Writer {} failed to flush: {}", i, e))
            })?;
        }
        Ok(())
    }
}
