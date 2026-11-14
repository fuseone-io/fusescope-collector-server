# 0014 · OTLP from pods to the agent on their node

- Status: accepted (2026-11-13)
- Date: 2026-11-13
- Issue: [#95](https://github.com/fuseone-io/fusescope-collector-server/issues/95)
- Amends: [ADR 0004](0004-security-baseline.md) (the loopback exception)
- Related: [ADR 0006](0006-single-ingress.md) (ingest authentication),
  [ADR 0011](0011-agent-daemonset-root.md) (the Kubernetes agent DaemonSet),
  [ADR 0013](0013-otlp-plaintext-for-migration.md) (plaintext OTLP during a SigNoz migration)

## Context

**What exists.** The chart's Kubernetes agent (`fusescopeAgent`, ADR 0011) is a DaemonSet without
`hostNetwork`. It reads pod logs, host metrics and kubelet stats, and sends them to the gateway over
mTLS. It has no OTLP input for the SDKs of the pods on its node. An SDK in a cluster sends to the
gateway today, over an endpoint with TLS and a key, or in plaintext only by the opt-in of ADR 0013,
which is meant to end.

**What constrains a node-local path.**
- A pod's `localhost` is its own network namespace: it does not reach the DaemonSet.
- A `hostPort` may be reachable beyond the node, depending on the CNI and the network.
- ADR 0004 requires TLS 1.3 on every network listener. ADR 0006 governs authentication on the
  gateway's public receivers; this node-local receiver is not one of them, and this ADR chooses a
  bearer token for it.
- Certificates are issued for names. A path whose address changes per node needs a certificate per
  node IP.

**The options weighed.**
- A ClusterIP Service with `internalTrafficPolicy: Local`.
- The node's IP from the downward API (`status.hostIP`) and a `hostPort`.
- A unix socket shared through a `hostPath`.
- Plaintext, as ADR 0013.

## Decision

1. **The agent serves an authenticated OTLP receiver, `otlp/sdks`,** on gRPC 4317 and HTTP 4318,
   off by default.
2. **SDKs reach it through a ClusterIP Service with `internalTrafficPolicy: Local`.**
   - **The supported route through the Service is node-local.** The Service sends a pod's
     connections only to the agent on the pod's node.
   - **With no agent on the node, the Service has no endpoint there,** and connections fail. The
     SDK retries. The agent's scheduling constraints (tolerations, node selectors and affinity)
     decide which nodes are covered.
   - **It requires Kubernetes 1.26 or later,** where `internalTrafficPolicy` is generally available.
3. **The route is not isolation.** A pod can still try to connect to any agent's Pod IP directly.
   **TLS 1.3 and the bearer token are the security boundary.** A NetworkPolicy listing the peers
   admitted on the two ports is additional defence. It cannot express "same node".
4. **TLS 1.3, with the certificate reloaded.** The server certificate covers the Service's DNS
   names, issued by cert-manager or provided as an existing Secret. The receiver reloads it, so a
   renewed certificate takes effect without a restart.
5. **A required bearer token, read from a file.** It comes from a Secret and is separate from the
   token the agent presents to the gateway. The extension serves gRPC and HTTP and follows changes
   to the file.
6. **No plaintext, no `hostPort`, no `hostNetwork`.**
7. **Host-service mode keeps loopback OTLP.** The reference configuration
   (`config/agent-kubernetes.yaml`) keeps its OTLP receiver on `localhost:4317` and
   `localhost:4318`, plaintext and without a token. Only processes in the agent's network namespace
   reach it, such as processes on the host when the agent runs as a host service. It is not
   reachable from other hosts or from ordinary pods. ADR 0004 records this as its single loopback
   exception.

## Rejected

- **The node's IP and a `hostPort`.** The SDK's certificate check needs a name or an IP the
  certificate covers, so every node IP would need one, and the port may be reachable beyond the
  node.
- **A `hostPath` socket.** Every application pod would mount a host path, which pod security
  policies often refuse. Not every SDK exports OTLP over a unix socket.
- **Plaintext.** ADR 0013 accepts it only during a migration, with explicit exit criteria. This
  path is meant to replace it.

## Consequences

- **SDKs that move to the agent leave ADR 0013's plaintext,** and keep their telemetry on the node
  until the agent's mTLS connection to the gateway.
- **Credentials have to reach the application namespaces.** Pods cannot mount a Secret from
  another namespace. The CA bundle and the token have to be copied or synced into each namespace
  whose SDKs send, and rotated there too.
- **The token is shared, and that is a risk.** Any holder can:
  - send telemetry with forged resource attributes: the agent's Kubernetes enrichment uses the
    connection's pod, but attributes the SDK sets pass through;
  - consume the agent's and the gateway's resources.

  Mitigations: rotate the token; keep the NetworkPolicy's peer list narrow; rely on the agent's
  `memory_limiter`. Per-workload identity is future work.
- **The agents' own telemetry is not centralized yet.** Refusals and drops at `otlp/sdks` are not
  visible in FuseScope until the agents' self-metrics reach it.
- **Work in fusescope-charts:**
  - an OTLP-only agent, with all three pipelines working when the node sources are off;
  - the Service, the certificate and the token;
  - guards on missing credentials, ports and the Kubernetes version;
  - the optional NetworkPolicy;
  - documentation of the SDK environment and of the distribution of the credentials.
