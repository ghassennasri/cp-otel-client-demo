# Sources and version provenance

Version-specific source files take precedence over examples copied from an unrelated release.

| Subject | Primary source | Use in this kit |
|---|---|---|
| CP 8.3.2 release | https://docs.confluent.io/platform/8.3/release-notes/index.html | Published target CP patch |
| CP / Ansible / Python compatibility | https://docs.confluent.io/ansible/current/ansible-requirements.html | Ansible 11 / Python 3.12 selection; explicit CP version |
| cp-ansible tag | https://github.com/confluentinc/cp-ansible/tree/v8.3.2 | Actual collection used for syntax validation |
| Collection metadata | https://raw.githubusercontent.com/confluentinc/cp-ansible/v8.3.2/galaxy.yml | Collection version 8.3.2 |
| CP variables | https://github.com/confluentinc/cp-ansible/blob/v8.3.2/roles/variables/defaults/main.yml | JMX source paths/ports, component log directory variables, explicit package version |
| CP service defaults | https://github.com/confluentinc/cp-ansible/tree/v8.3.2/roles | Preserve computed JVM agents/options when reducing demo heap |
| Docker/systemd test approach | https://github.com/confluentinc/cp-ansible/tree/v8.3.2/molecule | Inspiration for privileged Linux hosts and cgroup setup; this kit is independently assembled |
| Requested monitoring stack | https://github.com/confluentinc/jmx-monitoring-stacks/tree/f376263fc6d7270185ab6abab9a8c379079e2a92/jmxexporter-prometheus-grafana | Dashboard sources |
| JMX rules | https://github.com/confluentinc/jmx-monitoring-stacks/tree/f376263fc6d7270185ab6abab9a8c379079e2a92/shared-assets/jmx-exporter | Four vendored rule files and JMX 1.1 reference |
| cp-ansible monitoring integration | https://github.com/confluentinc/jmx-monitoring-stacks/blob/f376263fc6d7270185ab6abab9a8c379079e2a92/jmxexporter-prometheus-grafana/cp-ansible/README.md | Component source-path overrides |
| OTel release | https://github.com/open-telemetry/opentelemetry-collector-releases/releases/tag/v0.162.0 | Collector 0.162.0 binaries/images |
| Prometheus receiver | https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.162.0/receiver/prometheusreceiver/README.md | Scrape configuration and suffix handling |
| Remote write exporter | https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.162.0/exporter/prometheusremotewriteexporter/README.md | Translation strategy, WAL and exporter-specific queue |
| File log receiver | https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.162.0/receiver/filelogreceiver/README.md | File positions, rotation, multiline and receiver retries |
| OTel file storage | https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.162.0/extension/storage/filestorage | Persistent offsets and queue storage |
| OTel Java agent release | https://github.com/open-telemetry/opentelemetry-java-instrumentation/releases/tag/v2.20.1 | Trace-only auto-instrumentation for the demo application |
| Kafka compatibility | https://docs.confluent.io/platform/current/installation/versions-interoperability.html | CP 8.3.x embeds Kafka 4.3.x |
| Kafka client 4.3.0 | https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/4.3.0/ | Reproducible client and reporter API compilation |
| KIP-714 | https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability | Kafka protocol telemetry model and CLIENT_METRICS resources |
| KIP-1076 | https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=315494236 | Optional application metric registration API; detected but not used |
| VictoriaMetrics OTel integration | https://docs.victoriametrics.com/victoriametrics/integrations/opentelemetry/ | Native OTLP is an alternative; this demo chooses remote write for Prometheus naming compatibility |
| VictoriaMetrics release | https://github.com/VictoriaMetrics/VictoriaMetrics/releases/tag/v1.153.0 | Pinned backend version |
| Data Prepper log source | https://docs.opensearch.org/latest/data-prepper/pipelines/configuration/sources/otel-logs-source/ | OTLP/gRPC source, port 21892, output format |
| Data Prepper release | https://github.com/opensearch-project/data-prepper/releases/tag/2.16.0 | Pinned ingestion service version |
| OpenSearch release | https://github.com/opensearch-project/OpenSearch/releases/tag/3.8.0 | Pinned storage/UI version |
| Grafana release | https://github.com/grafana/grafana/releases/tag/v13.2.3 | Pinned dashboard application version |
| Tempo release | https://github.com/grafana/tempo/releases/tag/v2.8.2 | Pinned trace backend version |


The official cp-ansible v8.3.2 archive retrieved for inspection had SHA-256 `c46f7176c455d0c4766f8d9f63008bd95112b8e36b834a471bc1c944d69e8005`. Runtime installation uses the explicit git tag; strict supply-chain locking should also pin the resolved commit and image digests in the customer's artifact system.
