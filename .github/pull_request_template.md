<!--
The title becomes the commit message on main, since pull requests are
squash-merged, and has to follow Conventional Commits, e.g.
`feat(sink): add a Loki sink` or `fix(k8s): keep volumes of suspended streams`.
See CONTRIBUTING.md.
-->

## What and why

<!-- What the change does and why it is needed. Link the issue it solves, e.g. "Closes #123". -->

## How it was tested

<!-- Tests added or run, e.g. `task check`, `task test:containers` or `task test:envtest`. -->

## Checklist

- [ ] `task check` passes (lint, unit and end-to-end tests)
- [ ] Code is generated (`task generate`) if API, proto or CRD types changed
- [ ] Tests cover the new behavior
- [ ] Documentation is updated (README.md, COMPONENTS.md, the operator README) if behavior or configuration changed
- [ ] Commits are signed off (`git commit --signoff`) to accept the [CLA](../CLA.md)
