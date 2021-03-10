package com.ldq.study.flinkMain;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;

public class KeyByCase {


    public static void baseKeyBy() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0) // 以数组的第一个元素作为key
                .map((MapFunction<Tuple2<Long, Long>, String>) longLongTuple2 -> "key:" + longLongTuple2.f0 + ",value" +
                        ":" + longLongTuple2.f1)
                .print();

        env.execute("execute");
    }

    public static void reduceCount() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0) // 以数组的第一个元素作为key
                .reduce((ReduceFunction<Tuple2<Long, Long>>) (t2, t1) -> new Tuple2<>(t1.f0, t2.f1 + t1.f1)) // value做累加
                .print();

        env.execute("execute");
    }

    public static void sum() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // this can be used in a streaming program like this (assuming we have a StreamExecutionEnvironment env)
        KeyedStream keyedStream = env.fromElements(Tuple2.of(2L, 3L), Tuple2.of(1L, 5L), Tuple2.of(1L, 7L),
                Tuple2.of(2L, 4L), Tuple2.of(1L, 2L))
                .keyBy(0) // 以数组的第一个元素作为key
                ;

        SingleOutputStreamOperator<Tuple2> sumStream = keyedStream.sum(0);
//        sumStream = keyedStream.sum(0);
//        sumStream = keyedStream.sum("key");
//        sumStream = keyedStream.min(0);
//        sumStream = keyedStream.min("key");
//        sumStream = keyedStream.max(0);
//        sumStream = keyedStream.max("key");
//        sumStream = keyedStream.minBy(0);
//        sumStream = keyedStream.minBy("key");
//        sumStream = keyedStream.maxBy(0);
//        sumStream = keyedStream.maxBy("key");
        sumStream.addSink(new PrintSinkFunction<>());

        env.execute("execute");
    }

    public static void main(String[] args) throws Exception {
//        baseKeyBy();
//        reduceCount();
        sum();
    }
}
