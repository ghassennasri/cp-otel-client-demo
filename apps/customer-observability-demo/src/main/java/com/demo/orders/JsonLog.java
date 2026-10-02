package com.demo.orders;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Writes one JSON object per line. The current trace_id and span_id are added to every line,
 * so a log record in OpenSearch can be linked to its trace in Tempo.
 */
final class JsonLog implements AutoCloseable {
    private static final ObjectMapper JSON = new ObjectMapper();
    private final BufferedWriter writer;

    JsonLog(Path path) throws IOException {
        Files.createDirectories(path.getParent());
        writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    synchronized void write(String level, String message, String orderId) {
        SpanContext span = Span.current().getSpanContext();
        Map<String, Object> event = new LinkedHashMap<>();
        event.put("timestamp", Instant.now().toString());
        event.put("level", level);
        event.put("service", "customer-orders-demo");
        event.put("order_id", orderId == null ? "" : orderId);
        event.put("trace_id", span.isValid() ? span.getTraceId() : "");
        event.put("span_id", span.isValid() ? span.getSpanId() : "");
        event.put("message", message);
        try {
            writer.write(JSON.writeValueAsString(event));
            writer.newLine();
            writer.flush();
        } catch (IOException error) {
            throw new IllegalStateException("Unable to write application log", error);
        }
    }

    @Override
    public synchronized void close() throws IOException {
        writer.close();
    }
}