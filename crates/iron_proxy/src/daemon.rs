use std::fs;
use std::process;

const PID_FILE: &str = "iron-proxy.pid";

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

pub fn fork_to_background() -> Result<(), String> {
    use daemonize::Daemonize;
    use std::fs::File;

    let stdout = File::create("iron-proxy.out").map_err(|e| e.to_string())?;
    let stderr = File::create("iron-proxy.err").map_err(|e| e.to_string())?;

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
