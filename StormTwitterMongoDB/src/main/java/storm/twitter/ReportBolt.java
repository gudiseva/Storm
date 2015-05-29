package storm.twitter;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.util.Map;

import static com.mongodb.client.model.Filters.eq;

/**
 * A bolt that prints the word and count to console
 */
public class ReportBolt extends BaseRichBolt {

    //private DB mongoDocument;
    private MongoCollection<Document> mongoDocument;

    private final String mongoURI;
    private final String mongoDatabase;
    private final String mongoCollection;

    public ReportBolt(String mongoURI, String mongoDatabase, String mongoCollection) {
        this.mongoURI = mongoURI;
        this.mongoDatabase = mongoDatabase;
        this.mongoCollection = mongoCollection;
    }

/*
    public ReportBolt() { super();
    }
*/
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

        try {
            //this.mongoDocument = new MongoClient(new MongoClientURI(mongoURI)).getDB(mongoDatabase);
            this.mongoDocument = new MongoClient(new MongoClientURI(mongoURI)).getDatabase(mongoDatabase).getCollection(mongoCollection);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    @Override
    public void execute(Tuple tuple) {
        // access the first column 'word'
        String word = tuple.getStringByField("word");

        // access the second column 'count'
        Integer count = tuple.getIntegerByField("count");

        // publish the word count using word as the key
        System.out.println(" -> " + word + " | " + Integer.toString(count));

        // Insert or Update the word into MongoDB using word as the key
        /*
        BasicDBObjectBuilder dbObjectBuilder = new BasicDBObjectBuilder();
        DBObject dbObject = dbObjectBuilder.append("_id", word).append("value", count).get();
        // Just passing a document will replace the whole document meaning it doesn't set an _id a falls back to ObjectId
        mongoDocument.getCollection(mongoCollection).save(dbObject, new WriteConcern(1));
        */
        //mongoDocument.getCollection(mongoCollection).update(new BasicDBObject("_id", word), new BasicDBObject("$set", new BasicDBObject("value", count)), true, false);

        mongoDocument.updateOne(eq("_id", word), new Document("$set", new Document("value", count)), new UpdateOptions().upsert(true));
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }

}
