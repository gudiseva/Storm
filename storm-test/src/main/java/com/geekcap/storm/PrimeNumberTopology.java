package com.geekcap.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class PrimeNumberTopology
{
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new NumberSpout());
        builder.setBolt( "prime", new PrimeNumberBolt() ).shuffleGrouping("spout");

        Config conf = new Config();

        if (args != null && args.length > 0) {

            // run it in a live cluster

            // set the number of workers for running all spout and bolt tasks
            conf.setNumWorkers(2);

            conf.setMaxSpoutPending(5000);

            // create the topology and submit with config
            StormSubmitter.submitTopology("primenumbertopology", conf, builder.createTopology());

        } else {

            // run it in a simulated local cluster

            // create the local cluster instance
            LocalCluster cluster = new LocalCluster();

            // submit the topology to the local cluster
            cluster.submitTopology("test", conf, builder.createTopology());

            // let the topology run for 10 seconds. Note topologies never terminate!
            Utils.sleep(10000);

            // now kill the topology
            cluster.killTopology("test");

            // we are done, so shutdown the local cluster
            cluster.shutdown();

        }




    }
}