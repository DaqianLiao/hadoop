package com.ldq.study.utils;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class KafkaConfigUtils {

    private static final String BROKER_LIST = "localhost:9092";

    /**
     * 返回kafka 生产者配置信息
     *
     * @return
     */
    public static Properties getProducerProperty() {
        return getProducerProperty(BROKER_LIST);
    }

    /**
     * 返回指定brokerList的kafka 生产者信息
     *
     * @param brokerList
     * @return
     */
    public static Properties getProducerProperty(String brokerList) {
        Properties props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    /**
     * @param brokerList
     * @param groupId
     * @return
     */
    public static Properties getConsumerProperty(String brokerList, String groupId) {
        Properties props = new Properties();

        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        return props;
    }

    /**
     *
     * @param groupId
     * @return
     */
    public static Properties getConsumerProperty(String groupId) {
        return getConsumerProperty(BROKER_LIST, groupId);
    }
}
