# 0011 · The Kubernetes agent DaemonSet may run as root

- Status: accepted (2026-11-08)
- Amends: [ADR 0004](0004-security-baseline.md), "Distroless, non-root, read-only root filesystem images", for the
  Kubernetes agent DaemonSet only.
- Chart: [fusescope-charts#40](https://github.com/fuseone-io/fusescope-charts/pull/40) (`fusescopeAgent`).
- Deciders: FuseOne maintainers.

## Context

In agent mode ([DESIGN](../../DESIGN.md) §2) the collector runs as a DaemonSet and reads the node: pod logs under
`/var/log/pods` and host metrics through the host's root mounted at `/hostfs`. Kubernetes nodes keep pod logs, and
parts of `/proc`, readable by root only. `fsGroup` and supplementary groups do not change the ownership or mode of
`hostPath` files, so a non-root process cannot read them on most nodes, and nothing portable makes it.

ADR 0004 requires non-root images. The image stays distroless and non-root by default; what the agent needs is a
pod that runs it as uid 0.

## Decision

The Kubernetes agent DaemonSet may run as **uid 0, only to read pod logs and host metrics**, under all of these:

- **Off by default.** The agent is installed only when enabled.
- **Uid 0 only for what needs it:** with pod logs or host metrics enabled. With kubelet stats alone it runs as a
  non-root user (65532).
- **A values override runs it non-root** (`runAsUser`, `runAsGroup`, `runAsNonRoot: true`) where the nodes let a
  non-root user read those files.
- **The container stays locked down:**
  - `privileged: false`;
  - `allowPrivilegeEscalation: false`;
  - a read-only root filesystem;
  - every capability dropped;
  - seccomp `RuntimeDefault`.

  The chart refuses to render any other container security context.
- **No host namespace:** `hostNetwork`, `hostPID` and `hostIPC` are false.
- **Host paths read-only**, except the directory that holds its log read offsets (checkpoints).
- **Minimal RBAC:** read pods, namespaces, nodes and replica sets for the `k8s_attributes` processor, and `get`
  `nodes/stats` only with kubelet stats.
- **It is not an ingest endpoint.** It has no OTLP receiver: applications keep sending to the gateway, and the agent
  sends only what it collects, over mTLS with a token (ADR 0004).

The gateway, every other deployment of the collector, and the image itself stay non-root.

## Risks

- **Uid 0 with `/hostfs` reads broadly, capabilities or not.** The host's root is mounted read-only, and uid 0 owns
  most of it. Dropping every capability does not stop root from reading files root owns: the agent can read most of
  the node's files. Depending on how the container runtime binds submounts into `/hostfs`, that can include other
  pods' secrets and service account tokens under `/var/lib/kubelet`. A compromise of the agent is a read compromise
  of the node. Read-only mounts, no host namespaces, no privilege escalation and the
  seccomp profile limit what it can change, not what it can read.
- **Pod Security.** The `restricted` and `baseline` levels forbid host path volumes, and `restricted` forbids root.
  With pod logs or host metrics, the agent runs only in a namespace labelled `privileged`.

## Consequences

- The chart documents the exception, the Pod Security requirement and the override, and its tests assert each
  condition above.
- Narrowing `/hostfs` to the paths the host metrics scrapers read (`/proc`, `/sys`, and the mounts the filesystem
  scraper reports) would shrink the first risk. It is a follow-up, not a condition of this decision.
- Any other component that wants root needs its own ADR.
