fn main() {
    let mut strm = libz_rs_sys::z_stream::default();

    let version = libz_rs_sys::zlibVersion();
    let stream_size = core::mem::size_of_val(&strm) as i32;

    let level = libz_rs_sys::Z_DEFAULT_COMPRESSION; // the default compression level
    let err = unsafe { libz_rs_sys::deflateInit_(&mut strm, level, version, stream_size) };
    assert_eq!(err, libz_rs_sys::Z_OK);

    let input = "Hello, World!";
    strm.avail_in = input.len() as _;
    strm.next_in = input.as_ptr();

    let mut output = [0u8; 32];
    strm.avail_out = output.len() as _;
    strm.next_out = output.as_mut_ptr();

    let err = unsafe { libz_rs_sys::deflate(&mut strm, libz_rs_sys::Z_FINISH) };
    assert_eq!(err, libz_rs_sys::Z_STREAM_END);

    let err = unsafe { libz_rs_sys::deflateEnd(&mut strm) };
    assert_eq!(err, libz_rs_sys::Z_OK);

    let deflated = &mut output[..strm.total_out as usize];

    let mut strm = libz_rs_sys::z_stream::default();
    let err = unsafe { libz_rs_sys::inflateInit_(&mut strm, version, stream_size) };
    assert_eq!(err, libz_rs_sys::Z_OK);

    strm.avail_in = deflated.len() as _;
    strm.next_in = deflated.as_ptr();

    let mut output = [0u8; 32];
    strm.avail_out = output.len() as _;
    strm.next_out = output.as_mut_ptr();

    let err = unsafe { libz_rs_sys::inflate(&mut strm, libz_rs_sys::Z_FINISH) };
    assert_eq!(err, libz_rs_sys::Z_STREAM_END);

    let err = unsafe { libz_rs_sys::inflateEnd(&mut strm) };
    assert_eq!(err, libz_rs_sys::Z_OK);

    let inflated = &output[..strm.total_out as usize];

    assert_eq!(inflated, input.as_bytes());

    println!("input:    {:?}", input.as_bytes());
    println!("deflated: {:?}", deflated);
    println!("inflated: {:?}", inflated);
}
