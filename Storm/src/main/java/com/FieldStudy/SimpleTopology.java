package com.FieldStudy;

/**
 * Created by diligent_leo on 2016/11/14.
 */


import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;

public class SimpleTopology {


    public static void main(String[] args) {
            // 实例化TopologyBuilder类。
            TopologyBuilder topologyBuilder = new TopologyBuilder();
            // 设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数。
            topologyBuilder.setSpout("FieldSpout", new FieldSpout(), 1);
            // 设置数据处理节点并分配并发数。指定该节点接收喷发节点的策略为随机方式。
            topologyBuilder.setBolt("FieldBolt", new FieldSplitBolt()).shuffleGrouping("FieldSpout");
            Config config = new Config();
            config.setDebug(false);

            // 这里是本地模式下运行的启动代码。
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("simple", config, topologyBuilder.createTopology());

    }
}
