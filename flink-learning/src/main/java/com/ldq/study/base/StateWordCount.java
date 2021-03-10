package com.ldq.study.base;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class StateWordCount {
    public static void main(String[] args) throws Exception {
        final ParameterTool parameters = ParameterTool.fromArgs(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameters);

        // Checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);

        // StateBackend
        StateBackend stateBackend = new FsStateBackend("file:///Users/diligent_leo/Code/java/bigdata/flink-learning/data/flink");
        env.setStateBackend(stateBackend);

        env .addSource(new SourceFromFile())
                .setParallelism(1)
                .name("demo-source")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        String[] arr = value.split(",");
                        for (String item : arr) {
                            out.collect(new Tuple2<>(item, 1));
                        }
                    }
                })
                .name("demo-flatMap")
                .keyBy(0)
                .flatMap(new WordCountFlatMap())
                .print();

        env.execute("StateWordCount");
    }
    static class SourceFromFile extends RichSourceFunction<String> {
        private volatile Boolean isRunning = true;

        @Override
        public void run(SourceFunction.SourceContext ctx) throws Exception {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("./data/test.txt"));
            while (isRunning) {
                String line = bufferedReader.readLine();
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                ctx.collect(line);
                TimeUnit.SECONDS.sleep(10);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
    static class WordCountFlatMap extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        private ValueState<Tuple2<String, Integer>> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);

            // 配置 StateTTL(TimeToLive)
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.minutes(3))   // 存活时间
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)  // 永远不返回过期的用户数据
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)  // 每次写操作创建和更新时,修改上次访问时间戳
                    .setTimeCharacteristic(StateTtlConfig.TimeCharacteristic.ProcessingTime) // 目前只支持 ProcessingTime
                    .build();

            // 创建 ValueStateDescriptor
            ValueStateDescriptor descriptor = new ValueStateDescriptor("wordCountStateDesc",
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

            // 激活 StateTTL
            descriptor.enableTimeToLive(ttlConfig);

            // 基于 ValueStateDescriptor 创建 ValueState
            valueState = getRuntimeContext().getState(descriptor);

        }

        @Override
        public void flatMap(Tuple2<String, Integer> input, Collector<Tuple2<String, Integer>> collector) throws Exception {
            Tuple2<String, Integer> currentState = valueState.value();

            // 初始化 ValueState 值
            if (null == currentState) {
                currentState = new Tuple2<>(input.f0, 0);
            }

            Tuple2<String, Integer> newState = new Tuple2<>(currentState.f0, currentState.f1 + input.f1);

            // 更新 ValueState 值
            valueState.update(newState);

            collector.collect(newState);
        }
    }
}
