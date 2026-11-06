# Prospective systemd owner observation

This opt-in local recovery path never reconstructs historical owner evidence.
Run each Bridge owner as the main process of a dedicated user transient service
with `Type=exec`, `KillMode=control-group`, `Restart=no` and
`RemainAfterExit=yes`. Do not use a shell wrapper. Before starting, set:

- `BRIDGE_OWNER_CONFIG`: the exact configuration path.
- `BRIDGE_OWNER_CAPSULE`: a new absolute filename in a private mode-0700 directory.

The Bridge captures its own live unit, invocation, parent user-manager, process
start identity, cgroup device/inode and DB/config/binary bindings. It creates the
capsule with mode 0600 and binds its digest to initial owner evidence. Existing
files, unsafe paths, shared-unit main PID mismatches or incomplete bindings
refuse startup. Without the opt-in environment variable, startup is unchanged.

Retain the unit's invocation until recovery completes. Signal the dedicated
unit's processes rather than stopping/deleting the unit before observation.
`RemainAfterExit=yes` retains the terminal record after a clean main-process
exit. Recovery requires two stable systemd terminal snapshots with the original
invocation and main-process start identity and an empty ControlGroup. A live
foreign child, changed unit, or lost invocation refuses; ENOENT alone is not
proof. Do not reset-failed, collect, recreate, or restart the unit first.

After exit, use the exact current owner evidence revision:

```text
guigu-agent-bridge recover-systemd-owner --database <database> --config <config> --capsule <start-capsule> --revision <current-revision>
```

The command acquires ordinary startup exclusion and a scoped Matrix fence where
required. It independently validates the capsule and systemd records before
entering the lifecycle writer. It refuses admin bindings, foreign active owners,
leases, continuations, queues, workspace claims and execution effects. It does
not clear work, replay tasks, free unrelated leases or relax readiness. A
prospective stopped/scan_pending owner can be finalized only with its original
capsule binding and an unchanged evidence chain/revision. Unknown evidence and
legacy owners without capsules remain fail-closed.

A sibling `.terminal.json` file records the bound terminal observation, and
the reaped evidence records `reason=systemd-terminal-v1`. Only successful
readback authorizes subsequent normal startup. Keep failed capsules and logs;
never fill missing fields by hand. Once recovery and evidence preservation are
complete, the retained empty unit can be stopped normally.
