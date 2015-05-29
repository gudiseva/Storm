package storm.twitter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import twitter4j.Status;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A bolt that splits the tweet into words
 */
public class IgnoreTweetBolt extends BaseRichBolt {

    // To output tuples from this bolt to the count bolt
    OutputCollector collector;

    public IgnoreTweetBolt() {
        super();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // tell storm the schema of the output tuple for this spout
        // tuple consists of a single column called 'tweet-word'
        outputFieldsDeclarer.declare(new Fields("ignore-tweet-word"));
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
        String tweet = (String) tuple.getValueByField("tweet-word");

        Pattern pattern = Pattern.compile("[^A-Za-z0-9]");
        Matcher match = pattern.matcher(tweet);

        boolean bool = match.matches();
        // Process tweets that donot have special characters
        if (bool == true)
            System.out.println("Ignore as there is a special character in the tweet \" " + tweet + " \"");
        else{
            String specialChars = "'?'";
            if(specialChars.contains(tweet))
                System.out.println("Ignore question marks \" " + tweet + " \"");
            else
                collector.emit(new Values(tweet));
        }

    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
