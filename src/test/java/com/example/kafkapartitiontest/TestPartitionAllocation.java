package com.example.kafkapartitiontest;

import lombok.SneakyThrows;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
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



        assertThat(summary.get(0), allOf(greaterThan(45_000), lessThan(65_000)));
        assertThat(summary.get(1), allOf(greaterThan(145_000), lessThan(160_000)));
        assertThat(summary.get(2), allOf(greaterThan(85_000), lessThan(100_000)));
        assertThat(summary.get(3), allOf(greaterThan(38_000), lessThan(45_000)));
        assertThat(summary.get(4), allOf(greaterThan(110_00), lessThan(130_000)));
        assertThat(summary.get(5), allOf(greaterThan(130_000), lessThan(180_000)));
        assertThat(summary.get(6), allOf(greaterThan(55_000), lessThan(65_000)));
        assertThat(summary.get(7), allOf(greaterThan(90_000), lessThan(115_000)));
        assertThat(summary.get(8), allOf(greaterThan(120_000), lessThan(150_000)));
        assertThat(summary.get(9), allOf(greaterThan(85_000), lessThan(100_000)));



    }


    @DisplayName("An ideal partition Key")
    @Test
    void idealKey() {


        MockOrderFactory mockOrderFactory = getMockOrderFactory();

        Stack<Order> orders = mockOrderFactory.generateWeightedOrders(1_000_000);



        MockKafkaTopic mockKafkaTopic = MockTopicGenerator
                .mockTopic(() -> orders.pop().orderId().toString(), 10, 1_000_000);

        Map<Integer, Integer> summary = mockKafkaTopic.getSummary();


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


    @SneakyThrows
    private static MockOrderFactory getMockOrderFactory() {
        WeightedCollection<OrderType> orderTypeWeightedCollection = new WeightedCollection<>();

        orderTypeWeightedCollection.add(1, OrderType.DELIVERY);
        orderTypeWeightedCollection.add(3, OrderType.PICKUP);

        WeightedCollection<UUID> customerIdWeight = new WeightedCollection<>();


        List<String> customerIds = Files.readAllLines(Paths.get("uuids.txt"));


        IntStream.range(0, 100).forEach(i -> customerIdWeight.add(1, UUID.fromString(customerIds.get(i))));


        MockOrderFactory mockOrderFactory = new MockOrderFactory(orderTypeWeightedCollection, customerIdWeight);
        return mockOrderFactory;
    }
}
