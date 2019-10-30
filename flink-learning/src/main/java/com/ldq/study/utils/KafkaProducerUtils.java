package com.ldq.study.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class KafkaProducerUtils {

    private static KafkaProducer<String, String> producer = new KafkaProducer<>(KafkaConfigUtils.getProducerProperty());

    public static void send(String topic, String key, String value) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        System.out.println(producer.send(record).get()+ ", value = " + value);
    }
}
