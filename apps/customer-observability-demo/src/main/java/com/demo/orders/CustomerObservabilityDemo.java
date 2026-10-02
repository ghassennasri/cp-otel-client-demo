package com.demo.orders;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.metrics.JmxReporter;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Small order service used to show classic application observability next to Kafka.
 *
 * <p>{@code POST /order} produces an order to Kafka; a consumer in the same process reads it.
 * The three signals use three different, standard mechanisms:
 * <ul>
 *   <li>metrics: JMX MBeans (custom {@link OrderMetrics} + Kafka client metrics), exposed by the
 *       JMX Exporter javaagent and scraped by the OpenTelemetry gateway;</li>
 *   <li>logs: one JSON object per line in a file ({@link JsonLog}), read by the gateway filelog receiver;</li>
 *   <li>traces: the OpenTelemetry Java agent (no code needed for HTTP and Kafka spans), plus two
 *       manual spans, {@code order.create} and {@code order.process}.</li>
 * </ul>
 * KIP-714 is disabled here on purpose ({@code enable.metrics.push=false}); see the kip714-demo app.
 */
public final class CustomerObservabilityDemo {
    // Dedicated topic: the load-test traffic on demo-orders must not be processed as orders.
    private static final String TOPIC = "customer-orders";
    private static final OpenTelemetry OTEL = GlobalOpenTelemetry.get();
    private static final Tracer TRACER = OTEL.getTracer("customer-orders-demo");
    // Write / read the W3C trace context in Kafka record headers, so that the consumer span
    // continues the producer's trace.
    private static final TextMapSetter<Headers> HEADER_SETTER = (headers, key, value) -> {
        if (headers != null) {
            headers.remove(key).add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    };
    private static final TextMapGetter<Headers> HEADER_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Headers headers) {
            if (headers == null) {
                return Collections.emptyList();
            }
            Set<String> keys = new LinkedHashSet<>();
            headers.forEach(header -> keys.add(header.key()));
            return keys;
        }

        @Override
        public String get(Headers headers, String key) {
            if (headers == null || headers.lastHeader(key) == null) {
                return null;
            }
            return new String(headers.lastHeader(key).value(), StandardCharsets.UTF_8);
        }
    };

    private final OrderMetrics metrics = new OrderMetrics();
    private final JsonLog log;
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;

    private CustomerObservabilityDemo() throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092");
        log = new JsonLog(Path.of(System.getenv().getOrDefault("APP_LOG_PATH", "/var/log/customer-demo/application.log")));
        producer = new KafkaProducer<>(producerProperties(bootstrap));
        consumer = new KafkaConsumer<>(consumerProperties(bootstrap));
        MBeanServer server = ManagementFactory.getPlatformMBeanServer();
        server.registerMBean(metrics, new ObjectName("com.demo.orders:type=OrderMetrics"));
    }

    public static void main(String[] args) throws Exception {
        CustomerObservabilityDemo app = new CustomerObservabilityDemo();
        Runtime.getRuntime().addShutdownHook(new Thread(app::close));
        app.start();
    }

    private void start() throws IOException {
        consumer.subscribe(Collections.singletonList(TOPIC));
        Thread.ofPlatform().name("customer-order-consumer").start(this::consume);
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        server.createContext("/health", exchange -> respond(exchange, 200, "ok\n"));
        server.createContext("/order", this::handleOrder);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.start();
        log.write("INFO", "customer observability application started", null);
    }

    private void handleOrder(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            respond(exchange, 405, "POST required\n");
            return;
        }
        String orderId = UUID.randomUUID().toString();
        Span span = TRACER.spanBuilder("order.create").startSpan();
        try (Scope ignored = span.makeCurrent()) {
            log.write("INFO", "order created", orderId);
            metrics.created();
            ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, orderId, orderId);
            OTEL.getPropagators().getTextMapPropagator().inject(Context.current(), record.headers(), HEADER_SETTER);
            producer.send(record).get(10, TimeUnit.SECONDS);
            log.write("INFO", "Kafka record produced", orderId);
            respond(exchange, 202, "{\"order_id\":\"" + orderId + "\"}\n");
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            fail(exchange, span, orderId, error);
        } catch (ExecutionException | TimeoutException error) {
            fail(exchange, span, orderId, error);
        } finally {
            span.end();
        }
    }

    private void consume() {
        while (!Thread.currentThread().isInterrupted()) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<String, String> record : records) {
                Context parent = OTEL.getPropagators().getTextMapPropagator()
                    .extract(Context.current(), record.headers(), HEADER_GETTER);
                Span span = TRACER.spanBuilder("order.process").setParent(parent).startSpan();
                Instant started = Instant.now();
                try (Scope ignored = span.makeCurrent()) {
                    log.write("INFO", "Kafka record consumed", record.key());
                    metrics.processed(Duration.between(started, Instant.now()).toMillis());
                    log.write("INFO", "order processed", record.key());
                } catch (RuntimeException error) {
                    metrics.failed();
                    span.recordException(error);
                    span.setStatus(StatusCode.ERROR);
                    log.write("ERROR", "processing error: " + error.getMessage(), record.key());
                } finally {
                    span.end();
                }
            }
            if (!records.isEmpty()) {
                consumer.commitSync();
            }
        }
    }

    private void fail(HttpExchange exchange, Span span, String orderId, Exception error) throws IOException {
        metrics.failed();
        span.recordException(error);
        span.setStatus(StatusCode.ERROR);
        log.write("ERROR", "order creation failed: " + error.getMessage(), orderId);
        respond(exchange, 503, "order failed\n");
    }

    private static void respond(HttpExchange exchange, int status, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static Properties producerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "customer-orders-producer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, false);  // no KIP-714 in this app
        // Expose Kafka client metrics as JMX MBeans for the JMX Exporter.
        properties.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, JmxReporter.class.getName());
        properties.put(CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG, "TRACE");
        return properties;
    }

    private static Properties consumerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "customer-orders-consumer");
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "customer-orders-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // "earliest": orders sent before the consumer joined its group are not skipped.
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        properties.put(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, false);  // no KIP-714 in this app
        // Expose Kafka client metrics as JMX MBeans for the JMX Exporter.
        properties.put(CommonClientConfigs.METRIC_REPORTER_CLASSES_CONFIG, JmxReporter.class.getName());
        properties.put(CommonClientConfigs.METRICS_RECORDING_LEVEL_CONFIG, "TRACE");
        return properties;
    }

    private void close() {
        try {
            producer.close(Duration.ofSeconds(5));
            consumer.wakeup();
            consumer.close(Duration.ofSeconds(5));
            log.close();
        } catch (Exception ignored) {
            // Shutdown is best effort.
        }
    }
}