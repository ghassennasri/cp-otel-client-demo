# Third-party provenance and modifications

The four YAML files in `vendor/jmx-exporter/` originate from the Confluent `jmx-monitoring-stacks` repository at commit `f376263fc6d7270185ab6abab9a8c379079e2a92`. Its Apache License 2.0 text is included in `vendor/LICENSE-jmx-monitoring-stacks`. The broker rules have one local addition for the four `com.demo.kip714` reporter MBean counters; the other three rule files are unchanged. Checksums are recorded in `vendor/SHA256SUMS.json`.

The five upstream JSON dashboards in `assets/dashboards/` originate from the same commit and license. Local modifications: `${Prometheus}` datasource references become the provisioned UID `cp-victoriametrics`; import-time `__inputs` are removed; numeric `id` is set to null; JSON is reformatted. In `kafka-cluster-kraft.json`, the `instance` variable has `allValue: ".*"`, so that "All" also matches the dedicated KRaft controllers (the variable only lists brokers). Dashboard UIDs, titles and PromQL expressions are retained. The customer-observability and KIP-714 dashboards are original integration assets in this repository.

Other files are integration code and documentation prepared for this customer demonstration. They do not change the licenses or support terms of Confluent Platform, cp-ansible, OpenTelemetry, Grafana, VictoriaMetrics, OpenSearch, Data Prepper or their dependencies. Binary products and images are downloaded from their original distribution sources at runtime.

This demonstration is not an official Confluent product or an assurance of support for the complete observability stack.
