package com.ldq.study.utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaConsumerUtils {

    public static void consumer(String groupId, String topic) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(KafkaConfigUtils.getConsumerProperty(groupId));

        consumer.subscribe(Arrays.asList(topic.split(" ")));
        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(10000);
            Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
            while (iterator.hasNext()) {
                ConsumerRecord<String, String> next = iterator.next();
                System.out.println(next.toString());
            }
        }
    }
}
