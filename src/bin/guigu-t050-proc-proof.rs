fn main() {
    if guigu_agent_bridge::privileged_proc_proof::helper_main().is_err() {
        std::process::exit(3);
    }
}
