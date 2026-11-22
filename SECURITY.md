# Security Policy

## Supported Versions

Security fixes go into the latest release. Please check that a problem still exists there before you report it.

## Reporting a Vulnerability

Please do not report vulnerabilities in public issues, pull requests or discussions.

Report them privately through GitHub instead: on the [Security tab](https://github.com/GO-TRAM/TRAM/security) of the repository, choose **Report a vulnerability**, or open the [form](https://github.com/GO-TRAM/TRAM/security/advisories/new) directly. Include:

- the affected version, image tag or commit
- how TRAM runs: binary, container, Helm chart or manifests
- the steps or the stream configuration that show the problem
- what an attacker can do with it

You will get an answer in the advisory. Once a fix is released, the advisory is published, crediting you unless you prefer otherwise.

## What Is Not a Vulnerability

TRAM has no authentication or authorization of its own, by design. Whoever can create a stream decides which ports TRAM opens and where it sends data. The [Security section of the README](README.md#-security) describes these trust boundaries and how to restrict access, e.g. to the API, the OTLP source and the `Stream` resources of the Kubernetes operator. Behavior described there is not a vulnerability, but a way around those restrictions is.
