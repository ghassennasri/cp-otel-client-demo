package com.demo.kip714;

/**
 * JMX view of the reporter counters. The broker JMX Exporter rules turn them into
 * {@code kip714_reporter_payloads_*_total} metrics.
 */
public interface ClientTelemetryReporterMetricsMBean {
    /** PushTelemetry payloads handed to the reporter by the broker. */
    long getPayloadsReceived();

    /** Payloads successfully sent to the OpenTelemetry Collector. */
    long getPayloadsExported();

    /** Payloads that could not be parsed or sent. */
    long getPayloadsFailed();

    /** Payloads discarded because the in-memory queue was full. */
    long getPayloadsDropped();
}
