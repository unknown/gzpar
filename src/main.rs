use std::{
    ffi::OsString,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    sync::mpsc,
    thread,
};

use anyhow::{Result, ensure};
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
    let mut writer = BufWriter::new(output_file);

    let header = GzBuilder::new().os(FileSystemType::Unknown).into_header();
    writer.write_all(&header)?;

    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let handle = thread::spawn(move || {
        for block in rx {
            writer.write_all(&block).unwrap();
        }
    });

    let file = File::open(path)?;
    let mmap = unsafe { Mmap::map(&file)? };
    let blocks = mmap.chunks(block_size).collect::<Vec<_>>();
    let num_blocks = blocks.len();

    let compressed_blocks = blocks
        .into_par_iter()
        .enumerate()
        .map(|(i, b)| {
            let (block, hasher) = gzip_block(b, i == num_blocks - 1).unwrap();
            tx.send(block).unwrap();
            hasher
        })
        .collect::<Vec<_>>();

    let mut combined_hasher = Hasher::new();
    for hasher in compressed_blocks.into_iter() {
        combined_hasher.combine(&hasher);
    }

    let crc = combined_hasher.finalize();
    tx.send(crc.to_le_bytes().to_vec()).unwrap();

    let total_size: u32 = file.metadata()?.len().try_into()?;
    tx.send(total_size.to_le_bytes().to_vec()).unwrap();

    drop(tx);
    handle.join().unwrap();

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
