use std::{
    collections::BTreeMap,
    ffi::OsString,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
};

use anyhow::{Context, Result, ensure};
use clap::Parser;
use crc32fast::Hasher;
use gzip_header::{FileSystemType, GzBuilder};
use memmap2::Mmap;
use rayon::prelude::*;
use zlib_rs::{
    DeflateFlush, MAX_WBITS, ReturnCode,
    deflate::{self, DeflateConfig},
};

#[derive(Parser, Debug)]
struct Cli {
    file: PathBuf,
    #[arg(short, long, default_value_t = 128 * 1024)] // 128 KiB
    block_size: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    compress_file(&cli.file, cli.block_size)?;

    Ok(())
}

fn compress_file(path: &Path, block_size: usize) -> Result<()> {
    let gz_extension = path
        .extension()
        .map(|e| {
            let mut e = e.to_owned();
            e.push(".gz");
            e
        })
        .unwrap_or_else(|| OsString::from(".gz"));
    let output_file = File::create(path.with_extension(gz_extension))?;
    let writer = BufWriter::new(output_file);

    let header = GzBuilder::new().os(FileSystemType::Unknown).into_header();

    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let blocks = mmap.chunks(block_size).collect::<Vec<_>>();
    let num_blocks = blocks.len();
    let total_size: u32 = file.metadata()?.len().try_into()?;

    let (tx, rx) = mpsc::channel::<(usize, Vec<u8>, Hasher)>();

    let writer_handle = thread::spawn(move || -> Result<()> {
        let mut writer = writer;
        let mut combined_hasher = Hasher::new();
        let mut pending_blocks = BTreeMap::<usize, (Vec<u8>, Hasher)>::new();
        let mut next_index_to_write = 0;

        writer.write_all(&header)?;

        for (index, block, hasher) in rx {
            pending_blocks.insert(index, (block, hasher));

            while let Some((block_to_write, hasher_to_combine)) =
                pending_blocks.remove(&next_index_to_write)
            {
                writer
                    .write_all(&block_to_write)
                    .with_context(|| format!("Failed to write block {}", next_index_to_write))?;
                combined_hasher.combine(&hasher_to_combine);
                next_index_to_write += 1;
            }
        }

        ensure!(
            pending_blocks.is_empty(),
            "Writer thread finished with pending blocks - channel closed unexpectedly."
        );
        ensure!(
            next_index_to_write == num_blocks,
            "Writer thread did not write all blocks."
        );

        let crc = combined_hasher.finalize();
        writer.write_all(&crc.to_le_bytes())?;
        writer.write_all(&total_size.to_le_bytes())?;

        writer.flush()?;

        Ok(())
    });

    let tx_compressor = tx.clone();
    blocks
        .into_par_iter()
        .enumerate()
        .try_for_each(move |(i, b)| -> Result<()> {
            let (compressed_block, hasher) = gzip_block(b, i == num_blocks - 1)?;
            tx_compressor
                .send((i, compressed_block, hasher))
                .map_err(|e| anyhow::anyhow!("Failed to send block {} to writer thread: {}", i, e))
        })?;

    drop(tx);

    writer_handle.join().unwrap()?;

    Ok(())
}

fn gzip_block(block: &[u8], is_last: bool) -> Result<(Vec<u8>, Hasher)> {
    let size = deflate::compress_bound(block.len());
    let mut buffer = vec![0; size];
    let deflated = deflate_block(&mut buffer, block, is_last)?;

    let mut hasher = Hasher::new();
    hasher.update(block);

    Ok((deflated.to_vec(), hasher))
}

fn deflate_block<'a>(output: &'a mut [u8], block: &[u8], is_last: bool) -> Result<&'a [u8]> {
    let config = DeflateConfig {
        // A negative `window_bits` generates raw deflate data with no zlib header or trailer.
        window_bits: -MAX_WBITS,
        ..Default::default()
    };

    let (flush, expected_err) = if is_last {
        (DeflateFlush::Finish, ReturnCode::Ok)
    } else {
        (DeflateFlush::SyncFlush, ReturnCode::DataError)
    };

    let (deflated, err) = deflate::compress_slice_with_flush(output, block, config, flush);
    ensure!(err == expected_err, "failed to deflate");

    Ok(deflated)
}
