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

Note the register job pushes a branch to `ConduitIO/conduit-connector-registry`, which needs
`contents: write` on `REGISTRY_INDEX_PR_TOKEN` there — broader than the action's own docs
describe. If that token was minted to the narrower documented scope, the run fails at the **last**
step, after the artifact is already signed, released and provenanced. Confirm the scope before the
first tag.

## First registration pins the publisher identity permanently

On first registration the index entry pins:

```
^https://github\.com/ConduitIO/conduit-connector-pgvector/\.github/workflows/publish\.yml@refs/tags/v[0-9]+\.[0-9]+\.[0-9]+$
```

`publish.yml` cannot be renamed or moved afterwards without a re-registration PR.
