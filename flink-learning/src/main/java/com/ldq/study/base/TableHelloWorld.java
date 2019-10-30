package com.ldq.study.base;

import com.ldq.study.entity.WC;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TableHelloWorld {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<String> list = Arrays.asList(new String[]{"hello", "java", "hello", "flink"});
        //将集合转化为数据源
        List<WC> collect = list.stream().map(s -> new WC(s,1)).collect(Collectors.toList());
        DataSet<WC> text = env.fromCollection(collect);

        tEnv.registerDataSet("words", text, "name, cnt");

//        Table table = tEnv.sqlQuery("select * from words");
        Table table = tEnv.sqlQuery("select name, SUM(cnt) as cnt from words group by name");
//        Table table = tEnv.sqlQuery("select name, cnt from words where name like '%i%'");

        DataSet<WC> wcDataSet = tEnv.toDataSet(table, WC.class);
        wcDataSet.print();

    }

}
