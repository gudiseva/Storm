package storm.twitter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

/**
 * This is a basic example of a Storm topology.
 */
public class TweetTopology {

    public static void main(String[] args) throws Exception {

        Properties prop = new Properties();
        String propFileName = "src/resources/config.properties";
        InputStream input = new FileInputStream(propFileName);
        prop.load(input);

        // In order to create the spout, you need to get twitter credentials
        String consumerKey = prop.getProperty("Consumer_Key");
        String consumerSecret = prop.getProperty("Consumer_Secret");
        String accessToken = prop.getProperty("Access_Token");
        String accessTokenSecret = prop.getProperty("Access_Token_Secret");

        // create the topology
        TopologyBuilder topologyBuilder = new TopologyBuilder();

        // now create the tweet spout with the credentials
        TweetSpout tweetSpout = new TweetSpout(consumerKey, consumerSecret, accessToken, accessTokenSecret);

        // attach the tweet spout to the topology - parallelism of 1
        topologyBuilder.setSpout("tweet-spout", tweetSpout, 1);

        // attach the parse tweet bolt using shuffle grouping
        topologyBuilder.setBolt("parse-tweet-bolt", new ParseTweetBolt(), 2).shuffleGrouping("tweet-spout");

        // attach the count bolt using fields grouping - parallelism of 15
        topologyBuilder.setBolt("count-bolt", new CountBolt(), 3).fieldsGrouping("parse-tweet-bolt", new Fields("tweet-word"));

        // attach the report bolt using global grouping - parallelism of 1
        topologyBuilder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("count-bolt");

        // create the default config object
        Config conf = new Config();

        // set the config in debugging mode
        conf.setDebug(true);

        if (args != null && args.length > 0) {

            // run it in a live cluster

            // set the number of workers for running all spout and bolt tasks
            conf.setNumWorkers(2);

            // create the topology and submit with config
            StormSubmitter.submitTopology(args[0], conf, topologyBuilder.createTopology());

        } else {

            // run it in a simulated local cluster

            // set the number of threads to run - similar to setting number of workers in live cluster
            conf.setMaxTaskParallelism(2);

            // create the local cluster instance
            LocalCluster cluster = new LocalCluster();

            // submit the topology to the local cluster
            cluster.submitTopology("tweet-word-count", conf, topologyBuilder.createTopology());

            // let the topology run for 30 seconds. note topologies never terminate!
            Utils.sleep(30000);

            // now kill the topology
            cluster.killTopology("tweet-word-count");

            // we are done, so shutdown the local cluster
            cluster.shutdown();
        }

    }

}
