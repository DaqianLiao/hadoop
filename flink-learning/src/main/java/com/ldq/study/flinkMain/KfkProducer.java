package com.ldq.study.flinkMain;

import com.ldq.study.utils.KafkaProducerUtils;
import org.apache.flink.api.common.time.Time;

public class KfkProducer {

    public static void main(String[] args) throws Exception {
        int num = 10;
        long timestamp = System.currentTimeMillis();
        String topic = "text-demo";
        for (int i = 0; i < num; i++) {
            Thread.sleep(Time.seconds(2).toMilliseconds());
            KafkaProducerUtils.send(topic, null, "value=" + timestamp + "-" + i);
        }
    }

}
