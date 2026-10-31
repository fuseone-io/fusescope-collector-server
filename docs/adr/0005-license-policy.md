# 0005 · License policy

- Status: proposed — confirm against the FuseScope-wide MPL-2.0 decision
- Date: 2026-10-31

## Context

This repository is Apache-2.0, like the upstream collector. The upstream contrib modules it
builds on depend, unmodified, on a few MPL-2.0 libraries (for example HashiCorp's).

## Decision

- Source here is Apache-2.0. GPL, LGPL, AGPL, SSPL, BUSL and Commons-Clause never enter.
- Dependencies may be MIT, BSD-2/3-Clause, ISC or Apache-2.0, and MPL-2.0 only as an
  **unmodified** dependency. MPL-2.0 code is never copied or adapted into this repository.
- CI checks every module of the built binary against this list.

## Consequences

Removing an MPL-2.0 dependency is not required; modifying one is a new decision.
