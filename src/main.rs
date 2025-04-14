use std::{
    ffi::OsString,
    fs::{self, File},
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Result, ensure};
use clap::Parser;
use crc32fast::Hasher;
use gzip_header::{FileSystemType, GzBuilder};
use rayon::prelude::*;
use zlib_rs::{
    DeflateFlush, MAX_WBITS, ReturnCode,
    deflate::{self, DeflateConfig},
};

#[derive(Parser, Debug)]
struct Cli {
    file: PathBuf,
    #[arg(short, long, default_value_t = 128 * 1024)] // 128KB
    block_size: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    compress_file(&cli.file, cli.block_size)?;

    Ok(())
}

fn compress_file(path: &Path, block_size: usize) -> Result<()> {
    let bytes = fs::read(path)?;
    let blocks = bytes.chunks(block_size).collect::<Vec<_>>();
    let num_blocks = blocks.len();
    let compressed_blocks = blocks
        .into_par_iter()
        .enumerate()
        .map(|(i, b)| gzip_block(b, i == num_blocks - 1))
        .collect::<Vec<_>>();

    let gz_extension = path
        .extension()
        .map(|e| {
            let mut e = e.to_owned();
            e.push(".gz");
            e
        })
        .unwrap_or_else(|| OsString::from(".gz"));
    let mut output = File::create(path.with_extension(gz_extension))?;

    let header = GzBuilder::new().os(FileSystemType::Unknown).into_header();
    output.write_all(&header)?;

    let mut combined_hasher = Hasher::new();
    for (compressed_block, hasher) in compressed_blocks.into_iter().filter_map(|c| c.ok()) {
        output.write_all(&compressed_block)?;
        combined_hasher.combine(&hasher);
    }

    let crc = combined_hasher.finalize();
    output.write_all(&crc.to_le_bytes())?;

    let total_size: u32 = fs::metadata(path)?.len().try_into()?;
    output.write_all(&total_size.to_le_bytes())?;

    Ok(())
}

fn gzip_block(block: &[u8], is_last: bool) -> Result<(Vec<u8>, Hasher)> {
    let mut buffer = vec![0; block.len() * 2]; // TODO: fine tune this
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
