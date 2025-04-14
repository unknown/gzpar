use std::{
    ffi::OsString,
    fs::File,
    io::{Read, Write},
    path::{Path, PathBuf},
};

use anyhow::{Result, ensure};
use clap::Parser;
use crc32fast::Hasher;
use gzip_header::{FileSystemType, GzBuilder};

#[derive(Parser, Debug)]
struct Cli {
    file: PathBuf,
    #[arg(short, long, default_value_t = 128 * 1024)]
    chunk_size: usize,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    compress_file(&cli.file, cli.chunk_size)?;

    Ok(())
}

fn compress_file(path: &Path, chunk_size: usize) -> Result<()> {
    let mut file = File::open(path)?;
    let gz_extension = path
        .extension()
        .map(|e| {
            let mut e = e.to_owned();
            e.push(".gz");
            e
        })
        .unwrap_or_else(|| OsString::from(".gz"));
    let mut output = File::create(path.with_extension(gz_extension))?;

    let mut buffer = vec![0u8; chunk_size];
    while let Ok(n) = file.read(&mut buffer) {
        if n == 0 {
            break;
        }

        let chunk = &buffer[..n];
        let compressed = gzip_chunk(&chunk)?;
        output.write_all(&compressed)?;
    }

    Ok(())
}

fn gzip_chunk(chunk: &[u8]) -> Result<Vec<u8>> {
    let mut output = Vec::new();

    let header = GzBuilder::new().os(FileSystemType::Unknown).into_header();
    output.write_all(&header)?;

    let deflated = deflate_chunk(chunk)?;
    output.write_all(&deflated)?;

    let mut hasher = Hasher::new();
    hasher.update(chunk);
    let crc = hasher.finalize();
    output.write_all(&crc.to_le_bytes())?;

    let total_size = chunk.len() as u32;
    output.write_all(&total_size.to_le_bytes())?;

    Ok(output)
}

fn deflate_chunk(chunk: &[u8]) -> Result<Vec<u8>> {
    let mut strm = libz_rs_sys::z_stream::default();

    let version = libz_rs_sys::zlibVersion();
    let stream_size = core::mem::size_of_val(&strm) as i32;

    let level = libz_rs_sys::Z_DEFAULT_COMPRESSION; // the default compression level
    let method = libz_rs_sys::Z_DEFLATED;
    let window_bits = -15;
    let mem_level = 8;
    let strategy = libz_rs_sys::Z_DEFAULT_STRATEGY;
    let err = unsafe {
        libz_rs_sys::deflateInit2_(
            &mut strm,
            level,
            method,
            window_bits,
            mem_level,
            strategy,
            version,
            stream_size,
        )
    };
    ensure!(err == libz_rs_sys::Z_OK, "failed to initialize stream");

    strm.avail_in = chunk.len() as _;
    strm.next_in = chunk.as_ptr();

    let mut output = vec![0u8; chunk.len() * 2]; // TODO: use fine-tuned size
    strm.avail_out = output.len() as _;
    strm.next_out = output.as_mut_ptr();

    let err = unsafe { libz_rs_sys::deflate(&mut strm, libz_rs_sys::Z_FINISH) };
    ensure!(err == libz_rs_sys::Z_STREAM_END, "failed to deflate chunk");

    let err = unsafe { libz_rs_sys::deflateEnd(&mut strm) };
    ensure!(err == libz_rs_sys::Z_OK, "failed to deallocate stream");

    let deflated = &mut output[..strm.total_out as usize];

    Ok(deflated.to_vec())
}
