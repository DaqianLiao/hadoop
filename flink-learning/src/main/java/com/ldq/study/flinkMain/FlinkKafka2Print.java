package com.ldq.study.flinkMain;

import com.ldq.study.sink.MyPrintSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * 当flink程序启动后，在往kafka中发送数据，才能看到消费的效果
 */
public class FlinkKafka2Print {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test-8");

        env.addSource(new FlinkKafkaConsumer<>("text-demo", new SimpleStringSchema(), properties))
                .addSink(new MyPrintSink());

        env.execute("flink job start");
    }
}
