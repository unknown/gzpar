use std::{
    fs::File,
    io::{BufReader, copy},
    path::PathBuf,
};

use anyhow::{Context, Result};
use flate2::{Compression, write::GzEncoder};

const FILE_PATH: &str = "test/hello.txt";

fn main() -> Result<()> {
    let path = PathBuf::from(FILE_PATH);
    let file = File::open(&path).context("failed to open file")?;
    let mut reader = BufReader::new(file);

    let output_path = {
        let mut p = path.as_os_str().to_os_string();
        p.push(".gz");
        p
    };
    let output = File::create(output_path)?;
    let mut encoder = GzEncoder::new(output, Compression::default());

    copy(&mut reader, &mut encoder)?;
    Ok(())
}
