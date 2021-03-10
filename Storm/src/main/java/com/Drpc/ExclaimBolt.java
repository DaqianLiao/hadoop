package com.Drpc;

/**
 * Created by diligent_leo on 2016/12/19.
 */
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

import static jdk.nashorn.internal.runtime.regexp.joni.Config.log;


public class ExclaimBolt  implements IBasicBolt {

    /* (non-Javadoc)
     * @see backtype.storm.topology.IBasicBolt#cleanup()
     */
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IBasicBolt#execute(backtype.storm.tuple.Tuple, backtype.storm.topology.BasicOutputCollector)
     */
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        // TODO Auto-generated method stub
        System.out.println("处理数据");
        String input = tuple.getString(1);
        System.out.println("接收到的数据为:"+input);
        collector.emit(new Values(tuple.getValue(0), input + "!"));
    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IBasicBolt#prepare(java.util.Map, backtype.storm.task.TopologyContext)
     */
    public void prepare(Map stormConf, TopologyContext context) {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        declarer.declare(new Fields("id", "result"));
    }

    /* (non-Javadoc)
     * @see backtype.storm.topology.IComponent#getComponentConfiguration()
     */
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }




}
