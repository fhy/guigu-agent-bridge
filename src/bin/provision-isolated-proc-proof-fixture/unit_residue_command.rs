use super::unit_residue::{Cause, UNIT_NAME, UNIT_PARENT, run_with_hooks};
use std::{path::Path, process::Command};

pub(super) fn parse_expected(value: &str) -> Result<[u8; 32], Cause> {
    if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(Cause::Arguments);
    }
    if value.bytes().any(|byte| byte.is_ascii_uppercase()) {
        return Err(Cause::Arguments);
    }
    let mut result = [0_u8; 32];
    for (index, output) in result.iter_mut().enumerate() {
        *output = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .map_err(|_| Cause::Arguments)?;
    }
    Ok(result)
}

pub(super) fn state_is_absent(output: &[u8]) -> bool {
    let Ok(text) = std::str::from_utf8(output) else {
        return false;
    };
    let mut load = None;
    let mut active = None;
    let mut sub = None;
    let mut pid = None;
    for line in text.lines() {
        let Some((key, value)) = line.split_once('=') else {
            return false;
        };
        let slot = match key {
            "LoadState" => &mut load,
            "ActiveState" => &mut active,
            "SubState" => &mut sub,
            "MainPID" => &mut pid,
            _ => return false,
        };
        if slot.replace(value).is_some() {
            return false;
        }
    }
    matches!(
        (load, active, sub, pid),
        (Some("not-found"), Some("inactive"), Some("dead"), Some("0"))
    )
}

pub(super) fn run_production(expected: &str) -> Result<(), Cause> {
    let expected = parse_expected(expected)?;
    run_with_hooks(
        Path::new(UNIT_PARENT),
        expected,
        |_| {},
        || {
            Command::new("/usr/bin/systemctl")
                .args(["--user", "daemon-reload"])
                .status()
                .is_ok_and(|status| status.success())
        },
        || {
            Command::new("/usr/bin/systemctl")
                .args([
                    "--user",
                    "show",
                    UNIT_NAME,
                    "--property=LoadState",
                    "--property=ActiveState",
                    "--property=SubState",
                    "--property=MainPID",
                ])
                .output()
                .is_ok_and(|output| output.status.success() && state_is_absent(&output.stdout))
        },
    )
}

fn run_cli_with(
    mut args: impl Iterator<Item = String>,
    action: impl FnOnce(&str) -> Result<(), Cause>,
) -> Result<(), Cause> {
    let expected = match (args.next(), args.next(), args.next()) {
        (Some(flag), Some(expected), None) if flag == "--expected-sha256" => expected,
        _ => return Err(Cause::Arguments),
    };
    parse_expected(&expected)?;
    action(&expected)
}

pub(super) fn run_cli(args: impl Iterator<Item = String>) -> Result<(), Cause> {
    run_cli_with(args, run_production)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_shapes_are_strict_and_refuse_before_action() {
        let valid = "01".repeat(32);
        let uppercase = "AA".repeat(32);
        let cases = [
            vec![],
            vec!["--wrong"],
            vec!["--wrong", &valid],
            vec!["--expected-sha256"],
            vec!["--expected-sha256", "bad-digest"],
            vec!["--expected-sha256", &uppercase],
            vec!["--expected-sha256", &valid, "--expected-sha256", &valid],
            vec!["--expected-sha256", &valid, "extra"],
        ];
        for case in cases {
            let mut called = false;
            assert_eq!(
                run_cli_with(case.into_iter().map(str::to_owned), |_| {
                    called = true;
                    Ok(())
                }),
                Err(Cause::Arguments)
            );
            assert!(!called);
        }

        let mut called = false;
        assert_eq!(
            run_cli_with(
                ["--expected-sha256".to_owned(), valid.clone()].into_iter(),
                |expected| {
                    called = true;
                    assert_eq!(expected, valid);
                    Ok(())
                },
            ),
            Ok(())
        );
        assert!(called);
    }
}
