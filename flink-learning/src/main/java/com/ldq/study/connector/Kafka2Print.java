package com.ldq.study.connector;

import com.ldq.study.sink.MyPrintSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaTableSource;
import org.apache.flink.table.api.TableSchema;

import java.util.Properties;

public class Kafka2Print {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("group.id", "test-8");

        env.addSource(new FlinkKafkaConsumer<>("text-demo", new SimpleStringSchema(), properties))
                .addSink(new PrintSinkFunction<>(false));

        env.execute("flink job start");
    }
}
