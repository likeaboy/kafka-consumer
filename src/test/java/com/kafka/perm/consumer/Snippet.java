package com.kafka.perm.consumer;
/*package secfox.soc.consumer.mutil;

import java.util.Arrays;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

public class Snippet {
	*//** Check consumer lag and skip message if need. *//*
		public static void checkLagAndSkip(KafkaConsumer<String, String> consumer, String topic, int maxLag) {
			try {
				PartitionInfo partition = consumer.partitionsFor(topic).get(0);
				TopicPartition tp = new TopicPartition(partition.topic(), partition.partition());
				long next = consumer.position(tp);
				long latest = consumer.endOffsets(Arrays.asList(tp)).get(tp).longValue();
				long lag = latest - next;
				if (lag > maxLag) {
					long seek = latest - maxLag;
					logger.info("Kafka lag " + lag + " is bigger than max " + maxLag + ", seek to " + seek);
					consumer.seek(tp, seek);
				} else {
					logger.info("Kafka lag: " + lag);
				}
			} catch (Exception e) {
				logger.error(e.getMessage(), e);
			}
		}
}

*/