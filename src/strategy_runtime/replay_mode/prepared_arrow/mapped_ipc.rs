use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::RecordBatch;
use arrow_buffer::Buffer;
use arrow_ipc::convert::fb_to_schema;
use arrow_ipc::reader::{read_footer_length, FileDecoder};
use arrow_ipc::root_as_footer;

pub(super) fn read_mapped_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
    let file_len = usize::try_from(file.metadata()?.len()).context("Arrow file exceeds usize")?;
    anyhow::ensure!(file_len >= 10, "Arrow file is shorter than its trailer");

    // SAFETY: prepared replay artifacts are immutable for the lifetime of a loaded tape. The
    // read-only mapping is retained by every Arrow Buffer derived from it.
    let mmap =
        unsafe { memmap2::Mmap::map(&file) }.with_context(|| format!("mmap {}", path.display()))?;
    let buffer = Buffer::from(bytes::Bytes::from_owner(mmap));
    let trailer_start = buffer.len() - 10;
    let footer_len = read_footer_length(
        buffer[trailer_start..]
            .try_into()
            .expect("validated Arrow trailer length"),
    )?;
    anyhow::ensure!(
        footer_len <= trailer_start,
        "Arrow footer exceeds file bounds"
    );
    let footer = root_as_footer(&buffer[trailer_start - footer_len..trailer_start])
        .map_err(|error| anyhow::anyhow!("parse Arrow footer: {error:?}"))?;
    let ipc_schema = footer.schema().context("Arrow footer is missing schema")?;
    anyhow::ensure!(
        ipc_schema.endianness().equals_to_target_endianness(),
        "Arrow source endianness does not match target endianness"
    );
    let schema = fb_to_schema(ipc_schema);
    let mut decoder =
        FileDecoder::new(Arc::new(schema), footer.version()).with_require_alignment(true);

    for block in footer.dictionaries().iter().flatten() {
        let data = block_buffer(
            &buffer,
            block.offset(),
            block.metaDataLength(),
            block.bodyLength(),
        )?;
        decoder.read_dictionary(&block, &data)?;
    }

    let mut batches = Vec::with_capacity(footer.recordBatches().map_or(0, |rows| rows.len()));
    for block in footer.recordBatches().iter().flatten() {
        let data = block_buffer(
            &buffer,
            block.offset(),
            block.metaDataLength(),
            block.bodyLength(),
        )?;
        let batch = decoder
            .read_record_batch(&block, &data)?
            .context("Arrow footer record batch decoded to no batch")?;
        batches.push(batch);
    }
    Ok(batches)
}

fn block_buffer(buffer: &Buffer, offset: i64, metadata_len: i32, body_len: i64) -> Result<Buffer> {
    let offset = usize::try_from(offset).context("negative Arrow block offset")?;
    let metadata_len = usize::try_from(metadata_len).context("negative Arrow metadata length")?;
    let body_len = usize::try_from(body_len).context("negative Arrow body length")?;
    let len = metadata_len
        .checked_add(body_len)
        .context("Arrow block length overflow")?;
    let end = offset
        .checked_add(len)
        .context("Arrow block offset overflow")?;
    anyhow::ensure!(end <= buffer.len(), "Arrow block exceeds file bounds");
    Ok(buffer.slice_with_length(offset, len))
}
