package com.demo.orders;

public interface OrderMetricsMBean {
    long getOrdersCreated();
    long getOrdersProcessed();
    long getOrdersFailed();
    long getProcessingLatencyMs();
}