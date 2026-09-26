# v0.2.7

v0.2.7 adds fixed, redacted diagnostics for the reviewed read-only
`select-preacceptance` and `recover-selected-preacceptance` commands. Selector
failures retain their bounded categories and exact exit statuses without exposing
database paths, tuple identifiers, SQL, or stored values.

The T038 read-only selector and T039 in-memory handoff remain unchanged. T037
continues to own the single durable recovery transaction and independently
revalidates and fences any mutation. This release contains no production
reconciliation or Observer operation.
