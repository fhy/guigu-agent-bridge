# v0.2.6

v0.2.6 adds the reviewed T039 offline pre-acceptance handoff. The
`recover-selected-preacceptance` command selects one eligible tuple in a read-only
snapshot and passes it in memory to the existing T037 recovery transaction.

The command is fixed-output and redacted. T037 independently revalidates and fences
the write transaction; repeated selection after successful closure returns `empty`.

This release contains no production reconciliation or Observer operation.
