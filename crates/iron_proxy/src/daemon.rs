//! # Unix Daemonization
//!
//! This module handles the OS-level logic required to fork the proxy into a background
//! process. It redirects standard output and error to log files and manages the PID
//! file for graceful lifecycle control.

use std::fs;
use std::process;

/// The default filename used to store the daemon's Process ID.
const PID_FILE: &str = "iron-proxy.pid";

/// Attempts to gracefully stop a running Iron-Proxy daemon.
///
/// This function reads the PID from `iron-proxy.pid` and sends a `SIGTERM` (kill -15)
/// signal to the process, allowing it to drain connections before shutting down.
pub fn stop_background_process() {
    if let Ok(pid_str) = fs::read_to_string(PID_FILE) {
        let pid = pid_str.trim();
        println!("Sending graceful shutdown signal to PID {}...", pid);

        let status = process::Command::new("kill")
            .args(["-15", pid]) // -15 is SIGTERM
            .status();

        if status.is_ok() && status.unwrap().success() {
            println!("Iron-Proxy background process stopped.");
            let _ = fs::remove_file(PID_FILE);
        } else {
            eprintln!("Failed to stop process. Is it still running?");
        }
    } else {
        eprintln!("No {} file found. Is the daemon running?", PID_FILE);
    }
}

/// Forks the current process into a background daemon.
///
/// This uses the `daemonize` crate to detach from the terminal. Standard output
/// and standard error are redirected to the `iron-proxy.out` and `iron-proxy.err`
/// respectively.
///
/// # Errors
///
/// Returns a `String` containing the error message if the log files cannot be created
/// or if the OS refuses to fork the process.
pub fn fork_to_background() -> Result<(), String> {
    use daemonize::Daemonize;
    use std::fs::File;

    let stdout = File::create("iron-proxy.out").map_err(|e| e.to_string())?;
    let stderr = File::create("iron-proxy.err").map_err(|e| e.to_string())?;

    // The daemonization instance.
    let daemonize = Daemonize::new()
        .pid_file(PID_FILE)
        .working_directory(".")
        .stdout(stdout)
        .stderr(stderr);

    match daemonize.start() {
        Ok(_) => Ok(()),
        Err(e) => Err(format!("Error starting daemon: {}", e)),
    }
}
