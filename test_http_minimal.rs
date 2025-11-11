use std::io::{Read, Write};
use std::net::TcpStream;

fn main() {
    println!("Connecting to 127.0.0.1:8080...");
    let mut stream = TcpStream::connect("127.0.0.1:8080").expect("Failed to connect");
    println!("Connected!");

    let request = b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";
    println!("Sending request...");
    stream.write_all(request).expect("Failed to write");
    println!("Request sent!");

    let mut buf = vec![0u8; 4096];
    println!("Reading response...");
    let n = stream.read(&mut buf).expect("Failed to read");
    println!("Read {} bytes", n);
    println!("Response:\n{}", String::from_utf8_lossy(&buf[..n]));
}
