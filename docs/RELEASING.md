# Releasing this connector

Read this before pushing a tag. The order matters, and getting it wrong destroys the tag
mid-publish in a way that is not cleanly recoverable.

## The trap

`release.yml` runs `conduitio/automation/actions/check_connector_tag`, which compares the pushed
tag against `connector.yaml`'s `specification.version` and, on mismatch, **deletes the tag**
(`Delete Invalid Tag`, `if: failure()`).

`publish.yml` fires on the *same* tag, in parallel, with no ordering between the two workflows. It
creates the GitHub release and starts cosign-signing within ~20s; `release.yml` reaches its delete
in ~30-40s. So a mismatch does not simply fail — it most likely leaves a **created release with
signed assets pointing at a tag that no longer exists**, possibly with a registry PR already open.

Re-pushing the same tag does not recover it: the release object already exists, `--verify-tag`
semantics change, and signed assets are already attached.

## The order

1. **Bump `connector.yaml`'s `specification.version` to the exact tag** you are about to push,
   including the `v`. Commit it. This survives `make generate` — `conn-sdk-cli`'s specgen only
   fills the version when it is empty (`specgen.go`: `if existingSpecs.Specification.Version == ""`),
   and never derives it from git tags. `validate-generated-files` stays green.
2. Merge that commit to `main`.
3. Push the tag.

## Rehearsing

There is no safe partial rehearsal by tag shape alone. `publish.yml` correctly ignores
`v0.1.0-rc.1` (its filter is `v[0-9]+.[0-9]+.[0-9]+`), but `release.yml` triggers on `*` and
`check_connector_tag` accepts prerelease suffixes — so an rc tag passes format validation, fails
the `connector.yaml` comparison, and **gets deleted**. You would learn nothing.

To rehearse GoReleaser only, set `connector.yaml` to exactly the rc string first. Then the rc tag
runs `release.yml` end to end and leaves the registry untouched.

## What is unrehearsable

Cosign keyless signing against this repo's OIDC identity, and the register job's PR against the
index repo. Both run for real the first time.

The register job pushes a branch to `ConduitIO/conduit-connector-registry`, which needs
`contents: write` on `REGISTRY_INDEX_PR_TOKEN` there — broader than the action's own docs describe.
**That scope is confirmed by evidence rather than by reading the secret:** the same org secret has
already pushed branches and opened PRs to that repo four times — registry PRs #13 `generator`,
#14 `postgres`, #15 `log`, #16 `kafka`, each on a `connector-publish-action/<name>-<sha>` branch
created by the workflow, all merged.

And unlike the tag-deletion trap, a failure here is **recoverable**: the release, signatures and
provenance are already correct and immutable, so the register step can be re-run, or the index PR
opened by hand from the artifacts it produced. Nothing has to be re-signed.

## `minConduitVersion` / `minProtocolVersion` are a one-way door

`publish.yml` declares `0.15.0` / `0.9.0`, matching every connector already in the index. These
land in the first registry entry and cannot be changed without a re-registration PR, so the choice
is deliberate rather than inherited:

**They describe what THIS CONNECTOR needs, not what the RAG template needs.** pgvector is an
ordinary destination connector; it works against a 0.15.0 engine. The `postgres-pgvector-rag`
template additionally needs architecture v2 and the `ai.chunk`/`ai.embed` processors (which declare
`minConduitVersion: 0.20.0`), but those are the *template's* prerequisites and the template surfaces
them itself — `conduit pipelines init` prints them, and a drift guard in `template_gallery_test.go`
keeps that prose honest.

Raising this floor to 0.20.0 to match the template would wrongly refuse installation to anyone
using pgvector outside the RAG pipeline, which is a legitimate and probably common case. The
alternative failure — someone installs pgvector on a 0.15.0 engine and then discovers the template
needs more — is caught at `pipelines init` time with an actionable message, not silently.

## First registration pins the publisher identity permanently

On first registration the index entry pins:

```
^https://github\.com/ConduitIO/conduit-connector-pgvector/\.github/workflows/publish\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$
```

`publish.yml` cannot be renamed or moved afterwards without a re-registration PR.
