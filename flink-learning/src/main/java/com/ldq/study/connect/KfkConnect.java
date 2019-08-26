package com.ldq.study.connect;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class KfkConnect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        //定义kafka broker信息
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        //使用flink消费
        FlinkKafkaConsumer<String> books = new FlinkKafkaConsumer<>("books",
                new SimpleStringSchema(), properties);
        //设置从开始offset消费
        books.setStartFromEarliest();

        //创建source
        DataStreamSource<String> topic = env.addSource(books);
        //第一个string 表示输入的字符串，第二个string表示输出的字符串的类型
        SingleOutputStreamOperator<String> map = topic.map((MapFunction<String, String>) str -> str);
        tEnv.registerDataStream("books",map,"name");

//        Table table = tEnv.sqlQuery("select * from books");
        Table table = tEnv.sqlQuery("select name, count(1) as cnt from books group by name");

        //回退更新、重点核心细节，固定格式的Row.class
        tEnv.toRetractStream(table, Row.class).print();
        env.execute("kafka count");
    }
}
