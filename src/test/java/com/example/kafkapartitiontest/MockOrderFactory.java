package com.example.kafkapartitiontest;

import java.util.Stack;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class MockOrderFactory {

    private final WeightedCollection<OrderType> orderTypeWeights;

    private final WeightedCollection<UUID> customerIds;

    public MockOrderFactory(WeightedCollection<OrderType> orderTypeWeights, WeightedCollection<UUID> customerIds) {
        this.orderTypeWeights = orderTypeWeights;
        this.customerIds = customerIds;
    }

    public Stack<Order> generateWeightedOrders(int size) {

        return IntStream.range(0, size)
                .mapToObj(unused -> new Order(UUID.randomUUID(), customerIds.next(), orderTypeWeights.next(), Status.BACK_ORDER)
                ).collect(Collectors.toCollection(Stack::new));
    }
}
