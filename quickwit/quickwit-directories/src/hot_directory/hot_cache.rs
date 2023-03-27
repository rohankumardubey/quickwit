use std::collections::HashSet;
use std::io;
use std::path::PathBuf;

use prost::Message;
use quickwit_storage::OwnedBytes;

include!("./quickwit_proto.rs");

pub struct FileSliceCacheBuilder<'a> {
    buffer: &'a mut Vec<u8>,
    file_entry: &'a mut FileEntry,
}

#[derive(Default)]
pub struct StaticDirectoryCacheBuilder {
    file_paths: HashSet<PathBuf>,
    buffer: Vec<u8>,
    cache_meta: CacheMeta,
}

impl StaticDirectoryCacheBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_file(&mut self, file_path: PathBuf, file_length: u64) -> FileSliceCacheBuilder<'_> {
        assert!(!self.file_paths.contains(&file_path));
        self.file_paths.insert(file_path.clone());
        let file_entry = FileEntry {
            filepath: file_path.to_string_lossy().to_string(),
            file_length,
            file_slices: Vec::new(),
        };
        self.cache_meta.file_entries.push(file_entry);
        let (buffer, file_entries) = (&mut self.buffer, &mut self.cache_meta.file_entries);
        FileSliceCacheBuilder { buffer: &mut self.buffer, file_entry: file_entries.last_mut().unwrap()  }
    }

    pub fn write(self, wrt: &dyn io::Write) -> io::Result<()> {
        let cache_meta_bytes = self.cache_meta.encode_to_vec();
        wrt.write_all(cache_meta_bytes.len() as u64)?;
        wrt.write_all(&self.buffer)?;
        Ok(())
    }
}


pub struct StaticDirectoryCache {
    body: OwnedBytes,
}

impl StaticDirectoryCache {
    fn load(data: OwnedBytes) -> io::Result<Self> {
        let cache_meta_len = data.read_u64();
    }
}
