package com.example.kafkapartitiontest;

import java.util.UUID;

public record Order(UUID orderId, UUID customerId, OrderType type, Status status) {
}
