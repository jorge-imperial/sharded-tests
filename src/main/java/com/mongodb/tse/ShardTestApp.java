package com.mongodb.tse;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.*;

import com.mongodb.Block;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.client.*;

import com.mongodb.client.model.Aggregates;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonBoolean;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ShardTestApp {

    private MongoClient client;

    private MongoDatabase database;

    private String LQ_SHARD = "Shard";
    private String _ID = "_id";
    private int BATCH_SIZE = 1000;
    private AtomicInteger id;

    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    static public void main(String[] args) {

        ShardTestApp app = new ShardTestApp();
        app.run();
    }

    private void run() {

        LogManager logmngr = LogManager.getLogManager();

        // lgmngr now contains a reference to the log manager.
        Logger log = logmngr.getLogger(Logger.GLOBAL_LOGGER_NAME);


        log.log(Level.INFO, "Init test");

        id = new AtomicInteger(0);

        try {
            // connect to cluster
            client = MongoClients.create("mongodb://m17.mdb.com:27000,m18.mdb.com:27000,m19.mdb.com:27000");
            String databaseName = "test";
            database = client.getDatabase(databaseName);

            String collectionName = "testCollection";
            MongoCollection<Document> collection = database.getCollection(collectionName);


            try {
                collection.drop();
            } catch ( MongoExecutionTimeoutException e) {
                log.severe("could not drop collection...");
            }
            collection.insertOne(createDoc(id.getAndIncrement()));


            collection.createIndex(new Document(_ID, "hashed"));

            Document cmd = new Document("shardCollection", databaseName + "." + collectionName).append("key", new Document("_id", "hashed"));

            //Document result = client.getDatabase("admin").runCommand(cmd);
            MongoDatabase admin = client.getDatabase("admin");
            Document result = admin.runCommand(cmd);

            // Rapidly insert many documents (to beat the balancer)
            for (int i = 0; i < 100; ++i) {
                log.info("Batch " + i);
                bulkInsert(collection, BATCH_SIZE);
            }



            //Test for returned duplicates:
            //.count()
            //count through agg
            //count through document_count
            //select
            //update

            Document filter = new Document("_id", new Document("$in", Arrays.asList(1, 3, 5, 7)));
           for (int i=0; i<1000000; ++i) {
                long count_doc = collection.countDocuments(filter);
                long count = collection.count(filter);
                long count_aggregation[] = { 0 };
                collection.aggregate(Arrays.asList(
                        Aggregates.match(filter),
                        Aggregates.count( "count")
                        )
                ).forEach(new Block<Document>() {
                    @Override
                    public void apply(Document document) {
                        count_aggregation[0] = document.getInteger("count");
                    }
                });
                log.info( "count: " + count + "   countDocuments: " + count_doc + "   aggregation count: " + count_aggregation[0] );

                if ( (count_doc != count) || (count_doc != count_aggregation[0]) || (count_aggregation[0] != count)) {
                    log.severe("============================================================================");
                }

            }

            client.close();
            log.info("End.");
        } catch (MongoException e) {
            log.info(e.getMessage());
        }


    }

    private void bulkInsert(MongoCollection<Document> collection, int batchSize) {
        List<Document> docs = new ArrayList<>();

        for (int i = 1; i < batchSize; i++) {
            docs.add(createDoc(id.getAndIncrement()));
        }

        collection.insertMany(docs);
    }


    private long updateDocuments(MongoCollection<LQShardDocument> collection, Set<Integer> ids){

        Document updateDate = new Document();
        updateDate.put("$currentDate", new Document("updateTimeStamp", BsonBoolean.TRUE));
        Document update = new Document("$set", updateDate);
        UpdateResult result = collection.updateMany(new Document(_ID, new Document("$in", ids)), updateDate);

        //log.info("Updated {} docs to keep the cluster warm", result.getModifiedCount());
        return result.getModifiedCount();

    }


    private Document createDoc(int index) {
        return new Document(_ID, index)
                .append("payload1", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload2", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload3", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload4", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload5", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload6", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload7", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload8", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload9", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload10", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload11", "Executes the given command in the context of the current database with the given read preference.")
                .append("payload12", "Executes the given command in the context of the current database with the given read preference.")
                ;
    }
}
