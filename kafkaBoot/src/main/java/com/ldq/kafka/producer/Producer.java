package com.ldq.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.UUID;

/**
 * @Author: Daqian Liao
 * @Description:
 * @Date:
 */
@Component
@Slf4j
public class Producer {

    @Autowired
    private ProducerResultHandler handler;
    @Autowired
    private KafkaTemplate kafkaTemplate;

    //发送消息方法
    public void send() {
        send("test", "key-" + System.currentTimeMillis(), UUID.randomUUID().toString());
    }


    /**
     * 批量发送Kafka消息对象
     *
     * @param kafkaEvents
     */
    public void send(List<KafkaEvent> kafkaEvents) {
        if (kafkaEvents.size() > 5) {
            log.info("send event size = " + kafkaEvents.size());
        }

        for (KafkaEvent kafkaEvent : kafkaEvents) {
            send(kafkaEvent);
        }
    }

    /**
     * 发送消息对象
     *
     * @param kafkaEvent
     */
    public void send(KafkaEvent kafkaEvent) {

        send(kafkaEvent.getTopic(), kafkaEvent.getKey(), kafkaEvent.getMsg());
    }

    /**
     * 异步发送消息，获取消息状态
     *
     * @param topic
     * @param key
     * @param value
     */
    public void send(String topic, String key, String value) {
        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topic, key, value);
        future.addCallback(handler);
    }

}
