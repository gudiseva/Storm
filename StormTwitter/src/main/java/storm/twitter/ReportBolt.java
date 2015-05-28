package storm.twitter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * A bolt that prints the word and count to console
 */
public class ReportBolt extends BaseRichBolt {
    public ReportBolt() {
        super();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        // nothing to add - since it is the final bolt
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return super.getComponentConfiguration();
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        // nothing to add - since it is the final bolt
    }

    @Override
    public void execute(Tuple tuple) {
        // access the first column 'word'
        String word = tuple.getStringByField("word");

        // access the second column 'count'
        Integer count = tuple.getIntegerByField("count");

        // publish the word count using word as the key
        System.out.println(" -> " + word + " | " + Long.toString(count));
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
