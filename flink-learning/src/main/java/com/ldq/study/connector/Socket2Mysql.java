package com.ldq.study.connector;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class Socket2Mysql {
    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);


        DataStreamSource<String> localhost = env.socketTextStream("localhost", 9000);
        SingleOutputStreamOperator<Item> itemStream = localhost
                .filter(s -> s.length() > 0)
                .map(s -> {
                    String[] split = s.split(",");
                    return new Item(split[0], split[1], Integer.parseInt(split[2]));
                });

        TypeInformation[] types = {BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.STRING_TYPE_INFO,
                BasicTypeInfo.INT_TYPE_INFO};


        SingleOutputStreamOperator<Row> rowStream = localhost.filter(s -> s.length() > 0)
                .map(s -> {
                    String[] split = s.split(",");
                    Row row = new Row(types.length);
                    row.setField(0, split[0]);
                    row.setField(1, split[1]);
                    row.setField(2, Integer.parseInt(split[2]));
                    return row;
                });
        tableEnv.registerDataStream("items", itemStream, "name, code, price");

        String query = "select * from items";
        Table result = tableEnv.sqlQuery(query);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(result, Row.class);


        JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
                .setDrivername("com.mysql.jdbc.Driver")
                .setDBUrl("jdbc:mysql://localhost:3306/flink")
                .setUsername("root")
                .setPassword("mysqlroot")
                //当设置了batchSize为2的时候，只有超过2条的时候才会flush到数据中
                .setBatchSize(2)
                .setParameterTypes(types)
                .setQuery("insert INTO item (name,code,price) VALUES(?,?,?)")
                .build();


        sink.emitDataStream(rowStream);
//        sink.emitDataStream(rowDataStream);

        env.execute();
    }


    public static class Item {
        public String name;
        public String code;
        public Integer price;

        /**
         * nc -l 9000
         * java,java,1001
         * java,java,1001
         * java,java,1001
         * java,java,1001
         * java,java,1001
         * java,java,1001
         * java,java,1001
         * hadoop,hadoop,1006
         */
        //Too many fields referenced from an atomic type
        //必须有无参构造函数，否则报上面的错误
        public Item() {
        }

        public Item(String name, String code, int price) {
            this.name = name;
            this.code = code;
            this.price = price;
        }

        @Override
        public String toString() {
            return "Item{" +
                    "name='" + name + '\'' +
                    ", code='" + code + '\'' +
                    ", price=" + price +
                    '}';
        }
    }
}
