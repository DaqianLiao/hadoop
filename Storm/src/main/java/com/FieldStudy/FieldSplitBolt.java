package com.FieldStudy;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * Created by diligent_leo on 2016/12/19.
 */
public class FieldSplitBolt extends BaseRichBolt {
    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String string = tuple.getString(0);
        String[] strList = string.split("\t");
        int index = Integer.valueOf(strList[0]);
        String content = strList[1];
        outputCollector.emit(tuple, new Values(index, content));
        System.out.println(string);


    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("id", "string"));
//        outputFieldsDeclarer.declare(new Fields("string"));
    }

}
