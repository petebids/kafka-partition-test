package com.example.kafkapartitiontest;

import java.util.*;

public class MockKafkaTopic {

    private final ArrayList<ArrayList<String>> data;


    public MockKafkaTopic(int partitions) {
        data = new ArrayList<>();
        for (int i = 0; i < partitions ; i++) {
            data.add(new ArrayList<>());
        }
    }


    public void publishToPartition(int partition, String key) {

        data.get(partition).add(key);
    }


    public  Map<Integer, Integer> getSummary() {

        Map<Integer, Integer> summary = new HashMap<>();
        for (int i = 0; i < data.size(); i++) {
            summary.put(i, data.get(i).size());

        }

        return summary;
    }
}
