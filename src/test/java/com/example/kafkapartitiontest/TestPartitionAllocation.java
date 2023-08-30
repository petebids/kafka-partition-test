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
        assertThat(summary.get(6), allOf(greaterThan(200_000), lessThan(300_000)));
        assertEquals(summary.get(7), 0);
        assertEquals(summary.get(8), 0);
        assertThat(summary.get(9), allOf(greaterThan(700_000), lessThan(800_000)));

    }


    @DisplayName("A slightly improved partition key")
    @Test
    void slightImprovement() {

        MockOrderFactory mockOrderFactory = getMockOrderFactory();

        Stack<Order> orders = mockOrderFactory.generateWeightedOrders(1_000_000);

        MockKafkaTopic mockKafkaTopic = MockTopicGenerator
                .mockTopic(() -> orders.pop().customerId().toString(), 10, 1_000_000);

        Map<Integer, Integer> summary = mockKafkaTopic.getSummary();

        //{0=70291, 1=130089, 2=80153, 3=39853, 4=69583, 5=150634, 6=180126, 7=69353, 8=70051, 9=139867}

        assertThat(summary.get(0), allOf(greaterThan(70_000), lessThan(80_000)));
        assertThat(summary.get(1), allOf(greaterThan(130_000), lessThan(140_000)));
        assertThat(summary.get(2), allOf(greaterThan(70_000), lessThan(80_000)));
        assertThat(summary.get(3), allOf(greaterThan(30_000), lessThan(40_000)));
        assertThat(summary.get(4), allOf(greaterThan(60_000), lessThan(80_000)));
        assertThat(summary.get(5), allOf(greaterThan(130_000), lessThan(180_000)));
        assertThat(summary.get(6), allOf(greaterThan(150_000), lessThan(200_000)));
        assertThat(summary.get(7), allOf(greaterThan(55_000), lessThan(85_000)));
        assertThat(summary.get(8), allOf(greaterThan(70_000), lessThan(80_000)));
        assertThat(summary.get(9), allOf(greaterThan(130_000), lessThan(140_000)));



    }


    @DisplayName("An ideal partition Key")
    @Test
    void idealKey() {


        MockOrderFactory mockOrderFactory = getMockOrderFactory();

        Stack<Order> orders = mockOrderFactory.generateWeightedOrders(1_000_000);

        MockKafkaTopic mockKafkaTopic = MockTopicGenerator
                .mockTopic(() -> orders.pop().orderId().toString(), 10, 1_000_000);

        Map<Integer, Integer> summary = mockKafkaTopic.getSummary();


        System.out.println(summary);


        assertThat(summary.get(0), allOf(greaterThan(99_000), lessThan(101_000)));
        assertThat(summary.get(1), allOf(greaterThan(99_000), lessThan(101_000)));
        assertThat(summary.get(2), allOf(greaterThan(99_000), lessThan(101_000)));
        assertThat(summary.get(3), allOf(greaterThan(99_000), lessThan(101_000)));
        assertThat(summary.get(4), allOf(greaterThan(99_000), lessThan(101_000)));
        assertThat(summary.get(5), allOf(greaterThan(99_000), lessThan(101_000)));
        assertThat(summary.get(6), allOf(greaterThan(99_000), lessThan(101_000)));
        assertThat(summary.get(7), allOf(greaterThan(99_000), lessThan(101_000)));
        assertThat(summary.get(8), allOf(greaterThan(99_000), lessThan(101_000)));
        assertThat(summary.get(9), allOf(greaterThan(99_000), lessThan(101_000)));

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
