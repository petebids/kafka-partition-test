package com.example.kafkapartitiontest;

import org.apache.kafka.clients.producer.internals.BuiltInPartitioner;

import java.nio.charset.StandardCharsets;
import java.util.function.Supplier;
import java.util.stream.IntStream;

public class MockTopicGenerator {

    public static MockKafkaTopic mockTopic(Supplier<String> partitionKeySupplier, int partitionCount, int iterations) {

        MockKafkaTopic mockKafkaTopic = new MockKafkaTopic(partitionCount);

        IntStream.range(0, iterations )
                .forEach(unused -> {
                    String partitionKey = partitionKeySupplier.get();
                    int i = BuiltInPartitioner.partitionForKey(partitionKey.getBytes(StandardCharsets.UTF_8), partitionCount);
                    mockKafkaTopic.publishToPartition(i, partitionKey);
                });

        return mockKafkaTopic;
    }

}
