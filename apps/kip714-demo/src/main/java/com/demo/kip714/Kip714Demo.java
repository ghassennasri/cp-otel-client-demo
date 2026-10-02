package com.demo.kip714;

import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Plain Kafka producer + consumer used to demonstrate KIP-714 client telemetry.
 *
 * <p>There is deliberately no OpenTelemetry agent or SDK here. The only telemetry setting is
 * {@code enable.metrics.push=true} (the default since Kafka 3.7). The clients then push their own
 * metrics to the broker, and the broker plugin forwards them to the OpenTelemetry Collector.
 * Which clients push is decided on the cluster with {@code kafka-client-metrics}, not in the app.
 */
public final class Kip714Demo {
    private static final String TOPIC = "kip714-demo";
    private static final String PRODUCER_CLIENT_ID = "kip714-demo-producer";
    private static final String CONSUMER_CLIENT_ID = "kip714-demo-consumer";
    private static final Duration INSTANCE_ID_TIMEOUT = Duration.ofSeconds(30);

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicLong sequence = new AtomicLong();
    private final KafkaProducer<String, String> producer;
    private final KafkaConsumer<String, String> consumer;

    private Kip714Demo() {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "broker1:9092");
        producer = new KafkaProducer<>(producerProperties(bootstrap));
        consumer = new KafkaConsumer<>(consumerProperties(bootstrap));
    }

    public static void main(String[] args) throws Exception {
        Kip714Demo app = new Kip714Demo();
        Runtime.getRuntime().addShutdownHook(new Thread(app::close));
        app.start();
    }

    private void start() throws IOException {
        // The client instance id is the key the broker uses for this client's telemetry.
        // It is printed so it can be matched with the client_instance_id label in VictoriaMetrics.
        printInstanceId("Producer", PRODUCER_CLIENT_ID, producer.clientInstanceId(INSTANCE_ID_TIMEOUT));
        Thread.ofPlatform().name("kip714-producer").start(this::produce);
        Thread.ofPlatform().name("kip714-consumer").start(this::consume);

        HttpServer health = HttpServer.create(new InetSocketAddress(8080), 0);
        health.createContext("/health", exchange -> {
            byte[] body = "ok\n".getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
            exchange.close();
        });
        health.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        health.start();
    }

    /** Produces one small record per second. */
    private void produce() {
        while (running.get()) {
            long number = sequence.incrementAndGet();
            String value = "kip714-event-" + number + "-" + Instant.now();
            try {
                producer.send(new ProducerRecord<>(TOPIC, Long.toString(number), value)).get();
                Thread.sleep(1000);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                return;
            } catch (Exception error) {
                System.err.println("KIP-714 producer error: " + error.getMessage());
            }
        }
    }

    /** Consumes the records. KafkaConsumer is not thread-safe, so it is only used on this thread. */
    private void consume() {
        try {
            consumer.subscribe(Collections.singletonList(TOPIC));
            // The consumer fetches its telemetry subscription during poll(), so poll first.
            consumer.poll(Duration.ofSeconds(1));
            printInstanceId("Consumer", CONSUMER_CLIENT_ID, consumer.clientInstanceId(INSTANCE_ID_TIMEOUT));
            long processed = 0;
            while (running.get()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (!records.isEmpty()) {
                    consumer.commitSync();
                    processed += records.count();
                    if (processed % 60 < records.count()) {
                        System.out.printf("KIP-714 consumer processed %d records%n", processed);
                    }
                }
            }
        } catch (WakeupException expectedOnShutdown) {
            // close() wakes the consumer up so that the loop exits.
        } finally {
            consumer.close(Duration.ofSeconds(5));
        }
    }

    private static void printInstanceId(String role, String clientId, Uuid instanceId) {
        System.out.printf("KIP-714 %s:%n  client.id = %s%n  client.instance.id = %s%n", role, clientId, instanceId);
    }

    private static Properties producerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        // KIP-714: push client metrics to the broker (explicit here for readability).
        properties.put(ProducerConfig.ENABLE_METRICS_PUSH_CONFIG, true);
        return properties;
    }

    private static Properties consumerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "kip714-demo-group");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        // KIP-714: push client metrics to the broker (explicit here for readability).
        properties.put(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG, true);
        return properties;
    }

    private void close() {
        running.set(false);
        producer.close(Duration.ofSeconds(5));
        consumer.wakeup();
    }
}
