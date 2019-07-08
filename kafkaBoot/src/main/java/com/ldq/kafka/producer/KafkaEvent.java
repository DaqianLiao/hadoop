package com.ldq.kafka.producer;

import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

/**
 * @Author: Daqian Liao
 * @Description:
 * @Date:
 */
@Data
@NoArgsConstructor
public class KafkaEvent {
    private String topic;
    private String key;
    private String msg;
    private Date receiveTime;
    private Date sendTime;

}
