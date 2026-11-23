# Security Policy

## Supported Versions

Security fixes go into the latest release. Please check that a problem still exists there before you report it.

## Verifying Releases

Every release's container image and Helm chart are signed with [cosign](https://docs.sigstore.dev/), keyless: the signature names the release workflow of this repository and the tag it ran for, attested by GitHub, so no key has to be distributed. Verify them before deploying:

```bash
cosign verify ghcr.io/go-tram/tram:0.2.0 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '(?i)^https://github\.com/go-tram/tram/\.github/workflows/release\.yml@refs/tags/v'

cosign verify ghcr.io/go-tram/charts/tram:0.2.0 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --certificate-identity-regexp '(?i)^https://github\.com/go-tram/tram/\.github/workflows/release\.yml@refs/tags/v'
```

The image also carries an SBOM, which lists its contents, and a provenance attestation, which records how it was built, for each platform:

```bash
docker buildx imagetools inspect ghcr.io/go-tram/tram:0.2.0 --format '{{ json .SBOM }}'
docker buildx imagetools inspect ghcr.io/go-tram/tram:0.2.0 --format '{{ json .Provenance }}'
```

Releases before `v0.2.0` are not signed.

## Reporting a Vulnerability

Please do not report vulnerabilities in public issues, pull requests or discussions.

Report them privately through GitHub instead: on the [Security tab](https://github.com/go-tram/tram/security) of the repository, choose **Report a vulnerability**, or open the [form](https://github.com/go-tram/tram/security/advisories/new) directly. Include:

- the affected version, image tag or commit
- how TRAM runs: binary, container, Helm chart or manifests
- the steps or the stream configuration that show the problem
- what an attacker can do with it

You will get an answer in the advisory. Once a fix is released, the advisory is published, crediting you unless you prefer otherwise.

## What Is Not a Vulnerability

TRAM has no authentication or authorization of its own, by design. Whoever can create a stream decides which ports TRAM opens and where it sends data. The [Security section of the README](README.md#-security) describes these trust boundaries and how to restrict access, e.g. to the API, the OTLP source and the `Stream` resources of the Kubernetes operator. Behavior described there is not a vulnerability, but a way around those restrictions is.
