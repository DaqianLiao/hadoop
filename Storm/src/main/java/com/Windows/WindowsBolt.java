package com.Windows;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

/**
 * Created by diligent_leo on 2016/12/19.
 */
public class WindowsBolt extends BaseWindowedBolt {
    private long sum = 0;
    public void execute(TupleWindow tupleWindow) {
        System.out.println("WindowsBolt recevie int list as flow:");
        for (Tuple tuple :tupleWindow.get()) {
            int num = tuple.getInteger(0);
            sum += num;
            System.out.print( num + "\t");
        }
        System.out.println("sum = " + sum);
    }

}
