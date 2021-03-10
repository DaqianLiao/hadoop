package com.FieldStudy;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by diligent_leo on 2016/12/19.
 */
public class FieldSpout extends BaseRichSpout {
    private SpoutOutputCollector spoutOutputCollector;
    private String baseString = "baidu.com";

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void nextTuple() {
        Random random = new Random();
        int index = random.nextInt(5);
        String string = index + "\t" + baseString + index;
        try {
            Thread.sleep(1*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        spoutOutputCollector.emit(new Values(string));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("new String"));
    }
}
