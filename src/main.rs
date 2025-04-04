use std::{
    fs::File,
    io::{BufReader, copy},
    path::{Path, PathBuf},
};

use anyhow::Result;
use clap::Parser;
use flate2::{Compression, write::GzEncoder};

#[derive(Parser, Debug)]
struct Cli {
    file: PathBuf,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    compress_file(&cli.file)?;

    Ok(())
}

fn compress_file(file_path: &Path) -> Result<()> {
    let file = File::open(file_path)?;
    let mut reader = BufReader::new(file);

    let extension = file_path
        .extension()
        .map(|e| {
            let mut e = e.to_owned();
            e.push(".gz");
            e
        })
        .unwrap_or_default();
    let output = File::create(file_path.with_extension(extension))?;
    let mut encoder = GzEncoder::new(output, Compression::default());

    copy(&mut reader, &mut encoder)?;
    Ok(())
}
