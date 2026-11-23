# v0.2.12 Local Candidate Notes

This is an unpublished local candidate based on the reviewed T050-N implementation
and T052 main integration. It does not authorize publication, tags or deployment.

## Accepted behavior and limits

Independent staging acceptance exercised real native A2A card, authenticated send,
identical replay, conflict, get and cancel; real private **unencrypted** Matrix
multi-account messaging; restart with the same private database, deduplication,
timeout/cancellation and owned-process shutdown/reap. This is not Matrix E2EE proof.
The application source is unchanged by this release slice. Live evidence used an
earlier image (embedded `cd96ac0`), not an exact v0.2.12 build. Accepted assembled
coverage was 48 Rust harnesses (890 passed, 4 ignored), 2 doctests and 25 Python
tests, not a single final full-gate PASS. Historical failed gates, intermittent
failure and retained file/session/room residue are not erased by acceptance.

## Packaging

Cargo includes the build script, sources and embedded migrations. A package outside
a Git checkout builds the ordinary application without staging objects. In that
case source checkout identity is explicitly unavailable: SOURCE_COMMIT is zero and
SOURCE_DIRTY is true. Cargo VCS hints are not authentication; existing historical
artifact admission therefore remains disabled. This does not alter normal ACP,
A2A or Matrix runtime behavior, or restore complex process proof prerequisites.

## Upgrade and rollback

Startup applies embedded migrations; inspect the migration set and take a
consistent database backup before an independently authorized upgrade. Preserve
matching configuration, old executable and Matrix crypto-store generation where
applicable. There is no automatic schema downgrade. Binary rollback alone cannot
undo an applied migration; restore a consistent pre-upgrade database and matching
crypto-store generation under a separate reviewed procedure. No production
database or protected historical T046/R23 object is used by candidate preparation.

## Remaining release steps

After independent readiness review, separately authorize registry publication and
tagging of the exact candidate, then update installation references only after
publication is confirmed. Production readiness/deployment is a separate task.
