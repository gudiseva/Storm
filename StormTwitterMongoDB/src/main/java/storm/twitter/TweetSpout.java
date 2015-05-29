package storm.twitter;

import backtype.storm.Config;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A spout that uses Twitter streaming API for continuously getting tweets
 */
public class TweetSpout extends BaseRichSpout {

    // Twitter API authentication credentials
    String custkey, custsecret, accesstoken, accesssecret;

    // To output tuples from spout to the next stage bolt
    SpoutOutputCollector collector;

    // Twitter4j - twitter stream to get tweets
    TwitterStream twitterStream;

    // Shared queue for getting buffering tweets received
    LinkedBlockingQueue<Status> queue = null;

    public TweetSpout(String key, String secret, String token, String tokensecret)
    {
        custkey = key;
        custsecret = secret;
        accesstoken = token;
        accesssecret = tokensecret;
    }

    @Override
    public void close() {
        // shutdown the stream - when we are going to exit
        twitterStream.shutdown();
    }

    @Override
    public void activate() {
        super.activate();
    }

    @Override
    public void deactivate() {
        super.deactivate();
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet'
        outputFieldsDeclarer.declare(new Fields("tweet"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // create the component config
        Config config = new Config();

        // set the parallelism for this spout to be 1
        config.setMaxTaskParallelism(1);

        return config;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {

        // create the buffer to block tweets
        queue = new LinkedBlockingQueue<>(1000);

        // save the output collector for emitting tuples
        collector = spoutOutputCollector;


        // build the config with credentials for twitter 4j
        ConfigurationBuilder config = new ConfigurationBuilder()
                .setOAuthConsumerKey(custkey)
                .setOAuthConsumerSecret(custsecret)
                .setOAuthAccessToken(accesstoken)
                .setOAuthAccessTokenSecret(accesssecret);

        // create the twitter stream factory with the config
        TwitterStreamFactory fact = new TwitterStreamFactory(config.build());

        // get an instance of twitter stream
        twitterStream = fact.getInstance();

        // provide the handler for twitter stream
        twitterStream.addListener(new TweetListener());

        // start the sampling of tweets
        twitterStream.sample();

    }

    @Override
    public void nextTuple() {
        // try to pick a tweet from the buffer
        Status rawTweet = queue.poll();

        // if no tweet is available, wait for 50 ms and return
        if (rawTweet==null)
        {
            Utils.sleep(50);
            return;
        }

        // now emit the tweet to next stage bolt
        collector.emit(new Values(rawTweet));
    }

    // Class for listening on the tweet stream - for twitter4j
    private class TweetListener implements StatusListener {

        // Implement the callback function when a tweet arrives
        @Override
        public void onStatus(Status status) {
            // add the tweet into the queue buffer
            queue.offer(status);
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

        }

        @Override
        public void onTrackLimitationNotice(int i) {

        }

        @Override
        public void onScrubGeo(long l, long l1) {

        }

        @Override
        public void onStallWarning(StallWarning stallWarning) {

        }

        @Override
        public void onException(Exception e) {
            e.printStackTrace();
        }
    }

}