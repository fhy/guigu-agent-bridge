# Agent Bootstrap

Project workflow state and project-specific governance are maintained in the private
sibling checkout `../guigu-agent-bridge-governance`.

Before any task analysis, code edit, review, commit, or task-state change:

1. Resolve this repository's root and confirm `../guigu-agent-bridge-governance` is a
   Git checkout.
2. Read `../guigu-agent-bridge-governance/AGENTS.md` completely.
3. Follow its startup protocol and treat its `docs/TASK_BOARD.md` as authoritative.
4. Inspect this code repository only after identifying the active task and exact code
   base from the governance repository.

Fail closed if the sibling governance checkout is missing, unreadable, dirty in a way
that conflicts with the task, or disagrees with chat/task state. Do not reconstruct
task status from this repository's Git history and do not create a replacement local
task board.

This public repository remains authoritative for source code, tests, architecture,
ADRs, protocol contracts, configuration examples, and user documentation.
