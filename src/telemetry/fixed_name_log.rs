use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct FixedNameCappedFile {
    inner: Arc<Mutex<FixedNameCappedFileState>>,
}

struct FixedNameCappedFileState {
    path: PathBuf,
    max_bytes: u64,
    file: File,
    size_bytes: u64,
}

impl FixedNameCappedFile {
    pub fn new(path: impl AsRef<Path>, max_bytes: u64) -> io::Result<Self> {
        if max_bytes == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_bytes must be > 0",
            ));
        }

        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let size_bytes = file.metadata().map(|meta| meta.len()).unwrap_or(0);

        Ok(Self {
            inner: Arc::new(Mutex::new(FixedNameCappedFileState {
                path,
                max_bytes,
                file,
                size_bytes,
            })),
        })
    }

    fn truncate_locked(state: &mut FixedNameCappedFileState) -> io::Result<()> {
        state.file.flush()?;
        state.file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&state.path)?;
        state.size_bytes = 0;
        Ok(())
    }
}

impl Write for FixedNameCappedFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("fixed-name log mutex poisoned"))?;

        if state.size_bytes > 0
            && state.size_bytes.saturating_add(buf.len() as u64) > state.max_bytes
        {
            Self::truncate_locked(&mut state)?;
        }

        let written = state.file.write(buf)?;
        state.size_bytes = state.size_bytes.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        let mut state = self
            .inner
            .lock()
            .map_err(|_| io::Error::other("fixed-name log mutex poisoned"))?;
        state.file.flush()
    }
}
