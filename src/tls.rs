use rustls::ServerConfig;
use rustls_pemfile::{certs, private_key};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

pub fn load_tls_config() -> Arc<ServerConfig> {
    let cert_file = &mut BufReader::new(File::open("cert.pem").expect("Missing cert.pem"));
    let key_file = &mut BufReader::new(File::open("key.pem").expect("Missing key.pem"));

    let cert_chain = certs(cert_file).map(|c| c.unwrap()).collect();
    let key = private_key(key_file).unwrap().expect("Invalid private key");

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert_chain, key)
        .expect("Failed to build TLS config");

    Arc::new(config)
}