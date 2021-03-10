package com.Drpc;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

/**
 * Created by diligent_leo on 2016/12/19.
 */
public class DrpcTest {
    private static Logger logger = LogManager.getLogger(DrpcTest.class) ;

    public static void testDrpc(){
        Config conf = new Config();
        conf.setDebug(true);
        conf.setNumWorkers(2);
        conf.setMaxSpoutPending(1);
        LocalDRPC drpc=new LocalDRPC();
        LocalCluster cluster=new LocalCluster();
        LinearDRPCTopologyBuilder builder  = new LinearDRPCTopologyBuilder("exclamation");
        builder.addBolt(new ExclaimBolt(), 3);

        cluster.submitTopology("DRPCTest", conf, builder.createLocalTopology(drpc));

        logger.info("传入参数返回的结果:"+drpc.execute("exclamation", "hello"));


        cluster.shutdown();
        drpc.shutdown();
    }

    public static void main(String[] args) {
        testDrpc();
    }
}
