mod record;

use crate::error::Result;
use crate::Error;

use byteorder::{BigEndian, WriteBytesExt};
use record::Record;
use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

pub struct Manifest {
    file: File,
    writer: BufWriter<File>,
}

impl Manifest {
    pub fn new(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)?;

        let writer = BufWriter::new(file.try_clone()?);

        Ok(Self { file, writer })
    }

    pub fn append(&mut self, record: Record) -> Result<()> {
        let record_bytes: Vec<u8> = record.try_into()?;

        // Compute checksum properly using Digest
        let mut digest = crc64fast::Digest::new();
        digest.write(&record_bytes);
        let checksum = digest.sum64();

        // Write in format: [length:u32][checksum:u64][record:bytes]
        self.writer
            .write_u32::<BigEndian>(record_bytes.len() as u32)?;
        self.writer.write_u64::<BigEndian>(checksum)?;
        self.writer.write_all(&record_bytes)?;

        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.file.sync_all()?;
        Ok(())
    }

    pub fn iter(&self) -> Result<ManifestIter> {
        ManifestIter::new(self.file.try_clone()?)
    }
}

pub struct ManifestIter {
    file: File,
}

impl ManifestIter {
    fn new(file: File) -> Result<Self> {
        // Seek to beginning for iteration
        let mut file = file;
        file.seek(SeekFrom::Start(0))?;
        Ok(Self { file })
    }
}

impl Iterator for ManifestIter {
    type Item = Result<Record>;

    fn next(&mut self) -> Option<Self::Item> {
        // Read record length
        let mut len_buf = [0u8; 4];
        match self.file.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(e.into())),
        }

        // Read checksum
        let mut checksum_buf = [0u8; 8];
        if let Err(e) = self.file.read_exact(&mut checksum_buf) {
            return Some(Err(e.into()));
        }
        let stored_checksum = u64::from_be_bytes(checksum_buf);

        // Read record
        let record_len = u32::from_be_bytes(len_buf) as usize;
        let mut record_buf = vec![0u8; record_len];
        if let Err(e) = self.file.read_exact(&mut record_buf) {
            return Some(Err(e.into()));
        }

        // Verify checksum
        let mut digest = crc64fast::Digest::new();
        digest.write(&record_buf);
        let computed_checksum = digest.sum64();

        if computed_checksum != stored_checksum {
            return Err(Error::ChecksumMismatch).into();
        }
        Some(Record::try_from(record_buf.as_slice()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use record::FileInfo;
    use tempfile::tempdir;

    fn create_test_file_info(id: u64) -> FileInfo {
        FileInfo {
            id,
            size: 1024,
            min_key: vec![1, 2, 3],
            max_key: vec![9, 8, 7],
        }
    }

    #[test]
    fn test_manifest_basic_operations() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("MANIFEST");

        // Create manifest and write some records
        let mut manifest = Manifest::new(&manifest_path)?;

        let records = vec![
            Record::AddTable {
                id: 1,
                level: 0,
                info: create_test_file_info(1),
            },
            Record::BeginCompaction {
                job_id: 123,
                input_files: vec![create_test_file_info(1)],
                source_level: 1,
                target_level: 2,
            },
        ];

        // Write records
        for record in records.clone() {
            manifest.append(record)?;
        }
        manifest.sync()?;

        // Read records back
        let iter = manifest.iter()?;
        let read_records: Vec<Record> = iter.collect::<Result<Vec<_>>>()?;
        assert_eq!(read_records, records);

        Ok(())
    }

    #[test]
    fn test_manifest_corrupted_record() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("MANIFEST");

        // Write valid record
        let mut manifest = Manifest::new(&manifest_path)?;
        manifest.append(Record::AddTable {
            id: 1,
            level: 0,
            info: create_test_file_info(1),
        })?;
        manifest.sync()?;

        // Corrupt the file by writing random bytes at the end
        let mut file = OpenOptions::new().append(true).open(&manifest_path)?;
        file.write_all(&[0xFF; 100])?;
        file.sync_all()?;

        // Try reading records
        let manifest = Manifest::new(&manifest_path)?;
        let iter = manifest.iter()?;
        let results: Vec<Result<Record>> = iter.collect();

        // First record should be OK, second should be error
        assert!(results[0].is_ok());
        assert!(results[1].is_err());

        Ok(())
    }

    #[test]
    fn test_manifest_empty() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("MANIFEST");

        let manifest = Manifest::new(&manifest_path)?;
        let iter = manifest.iter()?;
        let records: Vec<Record> = iter.collect::<Result<Vec<_>>>()?;

        assert!(records.is_empty());
        Ok(())
    }

    #[test]
    fn test_add_table_record() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(&manifest_path)?;

        let record = Record::AddTable {
            id: 1,
            level: 0,
            info: create_test_file_info(42),
        };

        manifest.append(record.clone())?;
        manifest.sync()?;

        let read_records: Vec<Record> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(read_records, vec![record]);
        Ok(())
    }

    #[test]
    fn test_delete_table_record() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(&manifest_path)?;

        let record = Record::DeleteTable { id: 1, level: 0 };

        manifest.append(record.clone())?;
        manifest.sync()?;

        let read_records: Vec<Record> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(read_records, vec![record]);
        Ok(())
    }

    #[test]
    fn test_compaction_records() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(&manifest_path)?;

        let begin_record = Record::BeginCompaction {
            job_id: 123,
            input_files: vec![create_test_file_info(1), create_test_file_info(2)],
            source_level: 1,
            target_level: 2,
        };

        let end_record = Record::EndCompaction {
            job_id: 123,
            output_files: vec![create_test_file_info(3)],
            success: true,
        };

        manifest.append(begin_record.clone())?;
        manifest.append(end_record.clone())?;
        manifest.sync()?;

        let read_records: Vec<Record> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(read_records, vec![begin_record, end_record]);
        Ok(())
    }

    #[test]
    fn test_flush_records() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(&manifest_path)?;

        let begin_record = Record::BeginFlush {
            memtable_id: 1,
            wal_id: 42,
        };

        let end_record = Record::EndFlush {
            memtable_id: 1,
            output_files: vec![create_test_file_info(1)],
            success: true,
        };

        manifest.append(begin_record.clone())?;
        manifest.append(end_record.clone())?;
        manifest.sync()?;

        let read_records: Vec<Record> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(read_records, vec![begin_record, end_record]);
        Ok(())
    }

    #[test]
    fn test_mixed_records() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(&manifest_path)?;

        let records = vec![
            Record::AddTable {
                id: 1,
                level: 0,
                info: create_test_file_info(1),
            },
            Record::BeginCompaction {
                job_id: 123,
                input_files: vec![create_test_file_info(1)],
                source_level: 0,
                target_level: 1,
            },
            Record::EndCompaction {
                job_id: 123,
                output_files: vec![create_test_file_info(2)],
                success: true,
            },
            Record::DeleteTable { id: 1, level: 0 },
        ];

        for record in records.clone() {
            manifest.append(record)?;
        }
        manifest.sync()?;

        let read_records: Vec<Record> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(read_records, records);
        Ok(())
    }

    #[test]
    fn test_failed_operations() -> Result<()> {
        let dir = tempdir()?;
        let manifest_path = dir.path().join("MANIFEST");
        let mut manifest = Manifest::new(&manifest_path)?;

        let records = vec![
            Record::BeginCompaction {
                job_id: 123,
                input_files: vec![create_test_file_info(1)],
                source_level: 0,
                target_level: 1,
            },
            Record::EndCompaction {
                job_id: 123,
                output_files: vec![],
                success: false,
            },
            Record::BeginFlush {
                memtable_id: 1,
                wal_id: 42,
            },
            Record::EndFlush {
                memtable_id: 1,
                output_files: vec![],
                success: false,
            },
        ];
        for record in records.clone() {
            manifest.append(record)?;
        }
        manifest.sync()?;

        let read_records: Vec<Record> = manifest.iter()?.collect::<Result<Vec<_>>>()?;
        assert_eq!(read_records, records);
        Ok(())
    }
}
