package com.ldq.study.base;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class HelloWorld {
    public static void main(String[] args) throws Exception {
        //创建运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        List<String> list = Arrays.asList(new String[]{"hello", "java", "hello", "flink"});
        //将集合转化为数据源
        DataSet<String> text = env.fromCollection(list);
        //打印数据结果
        text.print();

        DataSet<Tuple2<String, Integer>> counts = text.flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        counts.print();
        System.out.println(counts.collect().toString());
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) {
            System.out.println("s = " + s);
            String[] split = s.split("\\W+");
            for (String s1 : split) {
                System.out.println("s1 = " + s1);
                if (s1.length() > 0) {
                    collector.collect(new Tuple2<String, Integer>(s1, 1));
                }
            }
        }
    }

}
