package com.ldq.study.flinkMain;

import com.ldq.study.sink.MyPrintSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * 设置检查点，当中断流式任务是，任务会从中断的offset出开始执行
 */
public class FlinkKafkaCheckpointPrint {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500);

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test-8");

        DataStreamSource<String> stringDataStreamSource = env.addSource(new FlinkKafkaConsumer<>("text-demo", new SimpleStringSchema(), properties));

        stringDataStreamSource.uid("uid&");
        stringDataStreamSource .addSink(new MyPrintSink());

        env.execute("flink job start");
    }
}
