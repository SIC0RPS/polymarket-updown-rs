#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CliMode {
    Run,
    OptimizeUnsupported,
    Approve,
}

pub(crate) fn detect_cli_mode(args: &[String]) -> CliMode {
    if args.iter().any(|arg| arg == "--optimize") {
        CliMode::OptimizeUnsupported
    } else if args.iter().any(|arg| arg == "--approve") {
        CliMode::Approve
    } else {
        CliMode::Run
    }
}
