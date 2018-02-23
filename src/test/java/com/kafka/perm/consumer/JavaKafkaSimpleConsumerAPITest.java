/*package com.kafka.perm.consumer;
import java.util.ArrayList;
import java.util.List;

*//**
 * Created by gerry on 12/21.
 *//*
public class JavaKafkaSimpleConsumerAPITest {
    public static void main(String[] args) {
        JavaKafkaSimpleConsumerAPI example = new JavaKafkaSimpleConsumerAPI();
        long maxReads = 300;
        String topic = "report";
        int partitionID = 0;

        KafkaTopicPartitionInfo topicPartitionInfo = new KafkaTopicPartitionInfo(topic, partitionID);
        List<KafkaBrokerInfo> seeds = new ArrayList<KafkaBrokerInfo>();
        seeds.add(new KafkaBrokerInfo("10.95.32.23", 9092));

        try {
            example.run(maxReads, topicPartitionInfo, seeds);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 获取该topic所属的所有分区ID列表
        System.out.println(example.fetchTopicPartitionIDs(seeds, topic, 100000, 64 * 1024, "client-id"));
    }
}*/