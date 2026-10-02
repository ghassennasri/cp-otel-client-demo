# From the demo to production

The demo shows an integration pattern. It is not a production template. This page lists the
decisions to make before using the pattern on a real Confluent Platform cluster.

## Target topology

```
 Each CP host                                  Gateway tier (2+ instances)       Backends
 ┌────────────────────────────────┐
 │ CP JVM ─JMX─> JMX Exporter ──┐ │
 │                              ├─┼─ OTLP ──>  OTel gateway  ── remote write ─> VictoriaMetrics ─> Grafana
 │ CP log files ────────────────┘ │            (load balanced) ── OTLP logs ───> Data Prepper ─> OpenSearch
 │        OTel agent (systemd)    │                            ── OTLP traces ─> Tempo (optional)
 └────────────────────────────────┘
```

- **Agents** stay local and simple. They read the local JMX endpoint and log files and buffer on local disk.
- **Gateways** are the policy point: routing, TLS identity, filtering and backend credentials.
  Run at least two behind a load balancer.
- Each JMX target must be scraped by **one** collector only. Adding gateway replicas does not
  remove duplicates when several collectors scrape the same target.

## Keep the dashboards working

The Confluent dashboards depend on exact metric names and on the labels `env`, `job`, `instance`
(and `kafka_connect_cluster_id` for Connect). The demo keeps them with:

- `trim_metric_suffixes: false` on the Prometheus receiver;
- `translation_strategy: UnderscoreEscapingWithoutSuffixes` on the remote write exporter;
- no processor that overwrites `service.name`, because the receiver rebuilds `job` from it.

After each CP upgrade, run the dashboard audit (`scripts/dashboard-audit.py`). Explain or fix
every empty panel with the metrics that really exist. Do not assume an empty panel means "healthy".

## Log collection

- Give the agent user read access with ACLs, including default ACLs, so access stays after rotation.
- Make sure rotated files are not compressed or deleted before the agent has read them.
- Decide the backlog policy for the first rollout (`start_at: beginning` reads all existing files).
- Do not collect the same output twice, for example with both filelog and journald.
- Exclude high-volume files that nobody searches (the demo excludes GC logs).
- Set the real logger timezone (`otel_log_timezone`).
- Audit events in Kafka topics are a separate scope. They are not covered by reading log files.

## Delivery guarantees

| Mechanism | What it gives | What it does not give |
|---|---|---|
| File offsets on disk (agent) | Resume after an agent restart | Exactly-once delivery; recovery of files already deleted |
| Persistent OTLP queues (agent, gateway) | Buffer during a backend outage | Unlimited retention; protection if the disk is lost |
| Remote write WAL (gateway) | Buffer metric exports | Collection while the agent is down |
| Data Prepper in-memory buffer (demo) | Absorbs bursts | Durability if Data Prepper crashes |

Retries can create duplicates. If logs must not be lost, check the buffering and acknowledgement
options of your Data Prepper version and size the queues for the outage you want to survive.

## Security

| Demo | Production |
|---|---|
| Plaintext Kafka, OTLP and JMX | TLS or mTLS on OTLP; JMX Exporter bound to loopback or protected |
| OpenSearch security plugin disabled | Authentication, roles and index permissions |
| Generated local Grafana password | Secret store, SSO and role-based access |
| Gateway runs as root in its container | Dedicated non-root user; volumes created with the right owner |
| Privileged Docker hosts, Docker connection | Managed hosts, approved SSH/PAM automation path |

## Sizing

Measure before you size. Start with one representative non-production cluster under normal load:

- active series and samples per second (first estimate: `samples/s ≈ active_series / scrape_interval_s`);
- log bytes per second and index growth per day;
- queue disk = measured throughput × the outage window you want to cover, plus a margin.

Topic, partition and client labels can dominate the series count. Filter the JMX rules to what the
dashboards and alerts really use. Verbose request logs can dominate log volume.

## KRaft controllers

The number of voting controllers is decided by fault tolerance, not by the number of racks.
Three voters tolerate one failure, and four voters still tolerate only one. Check the current
quorum first. Then follow the supported Confluent Platform procedure for any change. The demo
uses three controllers and does not change the quorum.

## Ownership

Agree on the owners before the rollout. A typical split:

| Area | Usually owned by |
|---|---|
| CP packages, inventory, JMX Exporter rules, log format | Kafka platform team |
| OTel agents and OS permissions on CP hosts | Kafka platform team with the infrastructure team |
| Gateways, routing and telemetry schema | Observability team |
| VictoriaMetrics, Grafana, OpenSearch, Data Prepper | Observability / logging team |
| KMinion and application telemetry | Current owners of these deployments |
| CI/CD and access automation | Tool owners and the security team |

## Suggested rollout

1. Validate the demo in a lab and keep its test reports.
2. Confirm the support scope for each component in writing.
3. Pilot on one non-production cluster: phase 1 (metrics), then phase 2 (logs).
4. Measure volumes, test a backend outage and an agent restart, and define the alerts.
5. Roll out cluster by cluster. Add application traces (phase 3) and KIP-714 (phase 4) per team, when needed.
