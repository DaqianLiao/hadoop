package com.ldq.study.sql;

import com.alibaba.fastjson.JSON;
import com.ldq.study.entity.Item;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TopNCase {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        List<String> list = Arrays.asList(new String[]{
                "java,java,1001,20,2019-01-02"
                , "C++,C++,1002,90,2019-01-02"
                , "java,java,1001,20,2019-01-02"
                , "java,java,1001,18,2019-01-02"
                , "java,java,1001,25,2019-01-02"
                , "python,python,1005,65,2019-01-02"
                , "python,python,1005,65,2019-01-02"
                , "hadoop,hadoop1,1006,100,2019-01-02"
                , "hadoop,hadoop1,1006,150,2019-01-02"
                , "hadoop,hadoop2,1007,100,2019-01-02"
                , "hadoop,hadoop1,1006,100,2019-01-02"
                , "hadoop,hadoop1,1006,180,2019-01-02"
                , "hadoop,hadoop1,1006,100,2019-01-02"
                , "hadoop,hadoop2,1007,120,2019-01-02"
        });
        //将集合转化为数据源
        List<Item> collect = list.stream()
                .map(s -> {
                    String[] split = s.split(",");
                    return new Item(split[0],
                            split[1],
                            split[2],
                            Integer.parseInt(split[3]),
                            split[4]);
                })
                .collect(Collectors.toList());
        DataSet<Item> text = env.fromCollection(collect);
        System.out.println("print Items:");
        text.print();


        tEnv.registerDataSet("items", text, "name, desp, code, price, createTime");

//        Table table = tEnv.sqlQuery("select * from items");
//        Table table = tEnv.sqlQuery("select *  from items order by price");
        Table table = tEnv.sqlQuery("select name, ROW_NUMBER() OVER (PARTITION BY code ORDER BY price DESC) AS rownum from items ");
        DataSet<Item> wcDataSet = tEnv.toDataSet(table, Item.class);
        System.out.println("calculate result:");
        wcDataSet.print();

    }
}
