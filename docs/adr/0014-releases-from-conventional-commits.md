# 0014: Releases From Conventional Commits

- **Status:** Accepted
- **Date:** 2026-11-22

## Context

Releases need a version that says what changed, notes for users, and artifacts that users can verify.

## Decision

- Pull requests are squash-merged, and their titles follow Conventional Commits, checked in CI; the title becomes the commit on `main`
- Versions are computed from these commits with svu (`next --always --v0`): before 1.0, a breaking change raises the minor version and anything else the patch version
- git-cliff writes the release notes from the same commits, for the GitHub Release and the Releases page of the documentation
- Commits are signed. Release images and charts are signed with cosign (keyless) and carry an SBOM and a provenance attestation

## Consequences

- A pull request title is part of the changelog and decides the version, so it has to describe the change for users
- Users can verify that an image or chart was built by the release workflow from a tag
