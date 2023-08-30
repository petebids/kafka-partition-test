package com.example.kafkapartitiontest;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Stack;
import java.util.UUID;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestPartitionAllocation {


    @DisplayName("A poor choice of partition key")
    @Test
    void poorPartitionKey() {

        MockOrderFactory mockOrderFactory = getMockOrderFactory();

        Stack<Order> orders = mockOrderFactory.generateWeightedOrders(1_000_000);

        MockKafkaTopic mockKafkaTopic = MockTopicGenerator
                .mockTopic(() -> orders.pop().type().toString(), 10, 1_000_000);

        Map<Integer, Integer> summary = mockKafkaTopic.getSummary();



        assertEquals(summary.get(0), 0);
        assertEquals(summary.get(1), 0);
        assertEquals(summary.get(2), 0);
        assertEquals(summary.get(3), 0);
        assertEquals(summary.get(4), 0);
        assertEquals(summary.get(5), 0);
        assertThat(summary.get(6), greaterThan(200_000));
        assertEquals(summary.get(7), 0);
        assertEquals(summary.get(8), 0);
        assertThat(summary.get(9), allOf(greaterThan(700_000), lessThan(800_000)));

    }


    @DisplayName("A slightly improved partition key")
    @Test
    void slightImprovement() {





    }


    @DisplayName("An ideal partition Key")
    @Test
    void idealKey() {


    }


    private static MockOrderFactory getMockOrderFactory() {
        WeightedCollection<OrderType> orderTypeWeightedCollection = new WeightedCollection<>();

        orderTypeWeightedCollection.add(1, OrderType.DELIVERY);
        orderTypeWeightedCollection.add(3, OrderType.PICKUP);

        WeightedCollection<UUID> customerIdWeight = new WeightedCollection<>();

        IntStream.range(0, 100).forEach(uuid -> customerIdWeight.add(1, UUID.randomUUID()));

        MockOrderFactory mockOrderFactory = new MockOrderFactory(orderTypeWeightedCollection, customerIdWeight);
        return mockOrderFactory;
    }
}
