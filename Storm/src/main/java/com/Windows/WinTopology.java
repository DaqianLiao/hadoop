package com.Windows;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

/**
 * Created by diligent_leo on 2016/12/19.
 */
public class WinTopology {
    public static void main(String[] args) {
        TopologyBuilder topologyBuilder = new TopologyBuilder();
        topologyBuilder.setSpout("spoutWin", new WindowsSpout());
        topologyBuilder.setBolt("boltWin",new WindowsBolt()
                .withWindow(new BaseWindowedBolt.Count(10),new BaseWindowedBolt.Count(5)))
                .shuffleGrouping("spoutWin");
        Config config = new Config();
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("test",config,topologyBuilder.createTopology());

    }
}
