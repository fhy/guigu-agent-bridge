# v0.2.11 Candidate Notes

v0.2.11 is a local, unpublished release candidate containing the reviewed T044
Matrix pre-start recovery behavior. The authenticated initialization response is
transferred to the single sync owner and processed before a second sync. A batch cursor
is saved only after every relevant item has a durable terminal disposition.

Missing decryption, backpressure, consumer closure and storage failures leave the batch
cursor unchanged and stop the required owner with a bounded redacted class. Intentional
shutdown cancels a pending disposition without checkpointing, and runtime supervision
retains the first Matrix or task-worker failure while bounding owner joins.

These properties are covered by deterministic offline tests. This document does not
claim that the retained production event was replayed or recovered, and it does not
authorize Observer startup, Matrix traffic, T036, publication or deployment. The README
installation example remains pinned to the published v0.2.10 release until v0.2.11 is
independently reviewed and published.
