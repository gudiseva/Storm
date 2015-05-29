package storm.mongodb;

import com.mongodb.*;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.bson.Document;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static com.mongodb.client.model.Filters.eq;

/**
 * Saving the Tweets count to MongoDB on Cloud.
 *  To connect using the shell:
 *  % mongo ds048537.mongolab.com:48537/m101 -u <dbuser> -p <dbpassword>
 *  To connect using a driver via the standard URI (what's this?):
 *    mongodb://<dbuser>:<dbpassword>@ds048537.mongolab.com:48537/m101
 */
public class MongoLabConnector {

    // MongoDB authentication credentials
    String mongoClientURI, mongoDatabase, mongoCollection;

    public MongoLabConnector(String mongoClientURI, String mongoDatabase, String mongoCollection) {
        this.mongoClientURI = mongoClientURI;
        this.mongoDatabase = mongoDatabase;
        this.mongoCollection = mongoCollection;
    }


    public  MongoCollection<Document> getMongoCollection() {

        MongoClient mongoClient = new MongoClient(new MongoClientURI(mongoClientURI));
        MongoDatabase database = mongoClient.getDatabase(mongoDatabase);
        MongoCollection<Document> collection = database.getCollection(mongoCollection);

        return collection;
    }

    public void mongoDBInsert(MongoCollection<Document> collection) {

        List<Integer> books = Arrays.asList(27464, 747854);

        Document person = new Document("_id", "jo2")
                .append("name", "Jo Bloggs")
                .append("address", new BasicDBObject("street", "123 Fake St")
                        .append("city", "Faketon")
                                .append("state", "MA")
                                .append("zip", 12345))
                                .append("books", books);

        collection.insertOne(person);
    }

    public void mongoDBUpsert(String word, int count, MongoCollection<Document> collection){

        collection.updateOne(eq("_id", word), new Document("$set", new Document("value", count)), new UpdateOptions().upsert(true));
    }

    public static void main (String [] args) throws IOException {

        Properties prop = new Properties();
        String propFileName = "src/resources/config.properties";
        InputStream input = new FileInputStream(propFileName);
        prop.load(input);

        // MongoDB authentication credentials
        String mongoClientURI = prop.getProperty("mongo_client_uri");
        String mongoDatabase = prop.getProperty("mongo_database");
        String mongoCollection = prop.getProperty("mongo_collection");

        MongoLabConnector mongoLabConnector = new MongoLabConnector(mongoClientURI, mongoDatabase, mongoCollection);

        String word = "arvind";
        int count = 9;
        mongoLabConnector.mongoDBUpsert(word, count, mongoLabConnector.getMongoCollection());

        mongoLabConnector.mongoDBInsert(mongoLabConnector.getMongoCollection());

    }

}
