package com.demo.kip714;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.opentelemetry.proto.collector.metrics.v1.ExportMetricsServiceRequest;
import io.opentelemetry.proto.collector.metrics.v1.MetricsServiceGrpc;
import io.opentelemetry.proto.common.v1.AnyValue;
import io.opentelemetry.proto.common.v1.KeyValue;
import io.opentelemetry.proto.metrics.v1.MetricsData;
import io.opentelemetry.proto.metrics.v1.ResourceMetrics;
import io.opentelemetry.proto.resource.v1.Resource;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.server.telemetry.ClientTelemetryContext;
import org.apache.kafka.server.telemetry.ClientTelemetryExporter;
import org.apache.kafka.server.telemetry.ClientTelemetryExporterProvider;
import org.apache.kafka.server.telemetry.ClientTelemetryPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.management.StandardMBean;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Broker-side KIP-714 plugin that forwards Kafka client metrics to an OpenTelemetry Collector.
 *
 * <p>Flow: a producer/consumer with {@code enable.metrics.push=true} sends its metrics to the
 * broker in {@code PushTelemetry} requests. The payload is already OTLP protobuf
 * ({@code MetricsData}). The broker hands it to this exporter, which adds a few identity
 * attributes and sends it over OTLP/gRPC to the collector.
 *
 * <p>Enable it on the broker with:
 * <pre>
 * metric.reporters=com.demo.kip714.ClientOtlpMetricsReporter
 * confluent.telemetry.external.client.metrics.push.enabled=true
 * client.telemetry.otlp.endpoint=otel-gateway:4317
 * </pre>
 * Which clients push which metrics is controlled separately with {@code kafka-client-metrics}.
 *
 * <p>This is demo code: plaintext gRPC, a small in-memory queue and no retry. Payloads that cannot
 * be queued or sent are counted and dropped, so the broker request path is never blocked.
 */
public final class ClientOtlpMetricsReporter implements MetricsReporter, Configurable,
        ClientTelemetryExporterProvider, ClientTelemetryReporterMetricsMBean {
    /** Broker property holding the collector's OTLP/gRPC endpoint (host:port). */
    public static final String ENDPOINT_CONFIG = "client.telemetry.otlp.endpoint";
    private static final String DEFAULT_ENDPOINT = "otel-gateway:4317";
    private static final String MBEAN_NAME = "com.demo.kip714:type=ClientTelemetryReporter";
    // Kafka 4.3 reports "OTLP"; "application/x-protobuf" is accepted for other broker versions.
    private static final String CONTENT_TYPE_OTLP = "OTLP";
    private static final String CONTENT_TYPE_PROTOBUF = "application/x-protobuf";
    private static final Logger LOG = LoggerFactory.getLogger(ClientOtlpMetricsReporter.class);

    // Operational counters, exposed through JMX (see init) and scraped like any broker metric.
    private final AtomicLong received = new AtomicLong();
    private final AtomicLong exported = new AtomicLong();
    private final AtomicLong failed = new AtomicLong();
    private final AtomicLong dropped = new AtomicLong();

    // One sender thread with a bounded queue: export work is moved off the broker request thread.
    private final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<>(1024), Executors.defaultThreadFactory(), (task, pool) -> dropped.incrementAndGet());
    private volatile ManagedChannel channel;
    private volatile MetricsServiceGrpc.MetricsServiceBlockingStub stub;
    private volatile ObjectName mBeanName;

    @Override
    public void configure(Map<String, ?> configs) {
        Object configuredEndpoint = configs.get(ENDPOINT_CONFIG);
        String endpoint = configuredEndpoint == null ? DEFAULT_ENDPOINT : configuredEndpoint.toString();
        channel = ManagedChannelBuilder.forTarget(endpoint).usePlaintext().build();
        stub = MetricsServiceGrpc.newBlockingStub(channel);
        LOG.info("KIP-714 OTLP reporter configured for {}", endpoint);
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        try {
            mBeanName = new ObjectName(MBEAN_NAME);
            MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (!server.isRegistered(mBeanName)) {
                // StandardMBean is required because the interface name does not follow the
                // "<ClassName>MBean" convention that plain registerMBean(this, ...) expects.
                server.registerMBean(new StandardMBean(this, ClientTelemetryReporterMetricsMBean.class), mBeanName);
            }
        } catch (Exception error) {
            LOG.warn("Unable to register reporter MBean {}", MBEAN_NAME, error);
        }
    }

    /** Called by the broker once, when both enabling properties are set. */
    @Override
    public ClientTelemetryExporter clientTelemetryExporter() {
        return this::exportMetrics;
    }

    /** Runs on a broker request thread: copy the payload and hand it to the sender thread. */
    private void exportMetrics(ClientTelemetryContext context, ClientTelemetryPayload payload) {
        received.incrementAndGet();
        String contentType = payload.contentType();
        if (!CONTENT_TYPE_OTLP.equalsIgnoreCase(contentType) && !contentType.startsWith(CONTENT_TYPE_PROTOBUF)) {
            failed.incrementAndGet();
            LOG.warn("Unsupported KIP-714 content type: {}", contentType);
            return;
        }
        ByteBuffer source = payload.data().duplicate();
        byte[] bytes = new byte[source.remaining()];
        source.get(bytes);
        String clientId = context.authorizableRequestContext().clientId();
        String clientAddress = context.authorizableRequestContext().clientAddress().getHostAddress();
        String clientInstanceId = payload.clientInstanceId().toString();
        int interval = context.pushIntervalMs();
        try {
            executor.execute(() -> send(bytes, clientId, clientAddress, clientInstanceId, interval));
        } catch (RuntimeException error) {
            dropped.incrementAndGet();
            LOG.warn("Dropping KIP-714 payload because the exporter queue is full", error);
        }
    }

    /** Runs on the sender thread: enrich the OTLP resource and export it to the collector. */
    private void send(byte[] bytes, String clientId, String clientAddress, String clientInstanceId, int interval) {
        try {
            MetricsData metricsData = MetricsData.parseFrom(ByteString.copyFrom(bytes));
            List<ResourceMetrics> enriched = new ArrayList<>(metricsData.getResourceMetricsCount());
            for (ResourceMetrics resourceMetrics : metricsData.getResourceMetricsList()) {
                Resource.Builder resource = resourceMetrics.hasResource()
                    ? resourceMetrics.getResource().toBuilder() : Resource.newBuilder();
                Set<String> keys = new HashSet<>();
                resource.getAttributesList().forEach(attribute -> keys.add(attribute.getKey()));
                // Added only when the client did not send them. They become VictoriaMetrics labels
                // (client_id, client_instance_id, ...) and make the series easy to filter.
                addAttribute(resource, keys, "client.id", clientId);
                addAttribute(resource, keys, "client.instance.id", clientInstanceId);
                addAttribute(resource, keys, "client.address", clientAddress);
                addAttribute(resource, keys, "kafka.push.interval.ms", Integer.toString(interval));
                addAttribute(resource, keys, "telemetry.source", "kip714");
                enriched.add(resourceMetrics.toBuilder().setResource(resource).build());
            }
            ExportMetricsServiceRequest request = ExportMetricsServiceRequest.newBuilder()
                .addAllResourceMetrics(enriched).build();
            stub.withDeadlineAfter(10, TimeUnit.SECONDS).export(request);
            long count = exported.incrementAndGet();
            if (count == 1 || count % 100 == 0) {
                LOG.info("Exported {} KIP-714 payloads; latest client.id={}, client.instance.id={}",
                    count, clientId, clientInstanceId);
            }
        } catch (Exception error) {
            failed.incrementAndGet();
            LOG.warn("Failed to export KIP-714 payload for client.id={}", clientId, error);
        }
    }

    private static void addAttribute(Resource.Builder resource, Set<String> keys, String key, String value) {
        if (value != null && !value.isBlank() && keys.add(key)) {
            resource.addAttributes(KeyValue.newBuilder().setKey(key)
                .setValue(AnyValue.newBuilder().setStringValue(value).build()).build());
        }
    }

    // This class is a MetricsReporter only so that the broker loads it from metric.reporters.
    // Broker metrics themselves are collected by the JMX Exporter, so these callbacks are no-ops.
    @Override
    public void metricChange(KafkaMetric metric) {
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
    }

    @Override
    public void close() {
        executor.shutdown();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
        if (channel != null) {
            channel.shutdown();
        }
        if (mBeanName != null) {
            try {
                ManagementFactory.getPlatformMBeanServer().unregisterMBean(mBeanName);
            } catch (Exception ignored) {
                // The broker may already have torn down the platform MBean server registration.
            }
        }
    }

    @Override
    public long getPayloadsReceived() {
        return received.get();
    }

    @Override
    public long getPayloadsExported() {
        return exported.get();
    }

    @Override
    public long getPayloadsFailed() {
        return failed.get();
    }

    @Override
    public long getPayloadsDropped() {
        return dropped.get();
    }
}
