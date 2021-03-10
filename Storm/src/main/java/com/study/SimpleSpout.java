package com.study;

/**
 * Created by diligent_leo on 2016/11/14.
 */


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;


public class SimpleSpout extends BaseRichSpout {

    /**
     *
     */
    private static final long serialVersionUID = 1L;
    private SpoutOutputCollector spoutOutputCollector;
    long[] clickTimes = {0,0,0};

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
    }

    public void nextTuple() {
        String[] stringList = {"www.baidu.com","www.jindie.com","www.facebook.com"};

        Random random = new Random();
        int index = random.nextInt(3);
        clickTimes[index] += 1 ;
        String string = stringList[index] + "\t" + clickTimes[index];
        spoutOutputCollector.emit(new Values(string));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("source"));
    }
}
