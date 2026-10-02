package com.demo.orders;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Business counters registered as the MBean {@code com.demo.orders:type=OrderMetrics}.
 * apps/customer-observability-demo/jmx-exporter.yaml maps them to customer_orders_* metrics.
 */
public final class OrderMetrics implements OrderMetricsMBean {
    private final AtomicLong ordersCreated = new AtomicLong();
    private final AtomicLong ordersProcessed = new AtomicLong();
    private final AtomicLong ordersFailed = new AtomicLong();
    private final AtomicLong processingLatencyMs = new AtomicLong();

    void created() {
        ordersCreated.incrementAndGet();
    }

    void processed(long latencyMs) {
        ordersProcessed.incrementAndGet();
        processingLatencyMs.set(latencyMs);
    }

    void failed() {
        ordersFailed.incrementAndGet();
    }

    @Override
    public long getOrdersCreated() {
        return ordersCreated.get();
    }

    @Override
    public long getOrdersProcessed() {
        return ordersProcessed.get();
    }

    @Override
    public long getOrdersFailed() {
        return ordersFailed.get();
    }

    @Override
    public long getProcessingLatencyMs() {
        return processingLatencyMs.get();
    }
}