package com.ldq.study.connect;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Iterator;
import	java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaUtils {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        String brokerList = "localhost:9092";
        props.put("bootstrap.servers", brokerList);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        String topic = "text-demo";

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        int messageNum = 100;
        for (int i = 0; i < messageNum; i++) {
//            ProducerRecord<String, String> record = new ProducerRecord<>(topic, topic + "-value" + i);
            ProducerRecord<String, String> record = new ProducerRecord<>(topic,  "value" + i);
            System.out.println(producer.send(record).get());
        }

        Properties props1 = new Properties();
        props1.put("bootstrap.servers", brokerList);
        props1.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props1.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        props1.put("group.id", "test-1");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props1);
        consumer.subscribe(Arrays.asList(topic.split(" ")));

        while (true) {
            ConsumerRecords<String, String> poll = consumer.poll(10000);
            Iterator<ConsumerRecord<String, String>> iterator = poll.iterator();
            while( iterator.hasNext()) {
                ConsumerRecord<String, String> next =  iterator.next();
                System.out.println(next.toString());
            }
        }
    }
}
