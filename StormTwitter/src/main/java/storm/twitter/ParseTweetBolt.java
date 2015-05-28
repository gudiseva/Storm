package storm.twitter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * A bolt that parses the tweet into words
 */
public class ParseTweetBolt extends BaseRichBolt {

    // To output tuples from this bolt to the count bolt
    OutputCollector collector;

    public ParseTweetBolt() {
        super();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet-word'
        outputFieldsDeclarer.declare(new Fields("tweet-word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // save the output collector for emitting tuples
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        // get the 1st column 'tweet' from tuple
        String tweet = tuple.getString(0);

        // provide the delimiters for splitting the tweet
        String delims = "[ .,?!]+";

        // now split the tweet into tokens
        String[] tokens = tweet.split(delims);

        // for each token/word, emit it
        for (String token: tokens) {
            collector.emit(new Values(token));
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
