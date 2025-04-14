use zlib_rs::{
    DeflateFlush, ReturnCode,
    deflate::{self, DeflateConfig},
};

fn main() {
    let input = "Hello, World!";
    let mut output = [0; 128];

    let config = DeflateConfig::default();

    let (deflated, err) = deflate::compress_slice_with_flush(
        &mut output,
        input.as_bytes(),
        config,
        DeflateFlush::Finish,
    );
    assert_eq!(err, ReturnCode::Ok);

    println!("input:    {:?}", input.as_bytes());
    println!("deflated: {:?}", deflated);
}
