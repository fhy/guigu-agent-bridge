use super::{ALLOW, AND, DENY, EQ, LOAD, RET, SET, program};

fn evaluate(number: u32, architecture: u32, arguments: [u64; 6]) -> u32 {
    let filter = program(1234);
    let mut data = [0u8; 64];
    data[..4].copy_from_slice(&number.to_le_bytes());
    data[4..8].copy_from_slice(&architecture.to_le_bytes());
    for (index, argument) in arguments.into_iter().enumerate() {
        data[16 + 8 * index..24 + 8 * index].copy_from_slice(&argument.to_le_bytes());
    }
    let mut accumulator = 0;
    let mut position = 0;
    while let Some(instruction) = filter.get(position) {
        position += 1;
        match instruction.code {
            LOAD => {
                accumulator = u32::from_le_bytes(
                    data[instruction.k as usize..instruction.k as usize + 4]
                        .try_into()
                        .unwrap(),
                )
            }
            EQ => {
                position += if accumulator == instruction.k {
                    instruction.jt
                } else {
                    instruction.jf
                } as usize
            }
            SET => {
                position += if accumulator & instruction.k != 0 {
                    instruction.jt
                } else {
                    instruction.jf
                } as usize
            }
            AND => accumulator &= instruction.k,
            RET => return instruction.k,
            _ => panic!("unsupported filter instruction"),
        }
    }
    panic!("unterminated filter")
}

#[test]
fn network_import_process_escape_and_policy_mutation_are_denied_by_the_compiled_filter() {
    for number in [
        41, 42, 43, 44, 45, 46, 47, 49, 50, 51, 52, 53, 54, 55, 57, 58, 59, 62, 101, 165, 200, 272,
        288, 299, 307, 308, 310, 311, 317, 321, 322, 425, 426, 427, 434, 438,
    ] {
        assert_eq!(
            evaluate(number, 0xc000003e, [0; 6]),
            DENY,
            "syscall={number}"
        );
    }
    assert_eq!(evaluate(0x40000000 | 39, 0xc000003e, [0; 6]), DENY);
    assert_eq!(evaluate(39, 0x40000003, [0; 6]), 0x80000000);
    assert_eq!(evaluate(39, 0xc000003e, [0; 6]), ALLOW);
}

#[test]
fn clone_allows_only_same_process_threads_and_clone3_returns_enosys() {
    let required = 0x100 | 0x800 | 0x10000;
    assert_eq!(evaluate(56, 0xc000003e, [required, 0, 0, 0, 0, 0]), ALLOW);
    for flags in [
        0,
        17,
        required & !0x10000,
        required | 0x20000000,
        required | (1 << 32),
    ] {
        assert_eq!(evaluate(56, 0xc000003e, [flags, 0, 0, 0, 0, 0]), DENY);
    }
    assert_eq!(
        evaluate(435, 0xc000003e, [0; 6]),
        0x00050000 | libc::ENOSYS as u32
    );
}

#[test]
fn policy_readbacks_and_thread_signals_are_argument_restricted() {
    for option in [libc::PR_GET_NO_NEW_PRIVS, libc::PR_GET_SECCOMP] {
        assert_eq!(
            evaluate(157, 0xc000003e, [option as u64, 0, 0, 0, 0, 0]),
            ALLOW
        );
        assert_eq!(
            evaluate(157, 0xc000003e, [option as u64 | 1 << 32, 0, 0, 0, 0, 0]),
            DENY
        );
    }
    assert_eq!(
        evaluate(
            157,
            0xc000003e,
            [libc::PR_SET_SECCOMP as u64, 0, 0, 0, 0, 0]
        ),
        DENY
    );
    assert_eq!(evaluate(234, 0xc000003e, [1234, 1235, 6, 0, 0, 0]), ALLOW);
    for arguments in [
        [5678, 1235, 6, 0, 0, 0],
        [1234, 1235, 9, 0, 0, 0],
        [1234, 1235, 6 | 1 << 32, 0, 0, 0],
    ] {
        assert_eq!(evaluate(234, 0xc000003e, arguments), DENY);
    }
}
