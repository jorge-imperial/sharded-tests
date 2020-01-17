package com.mongodb.tse;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.*;

import com.mongodb.Block;
import com.mongodb.MongoException;
import com.mongodb.MongoExecutionTimeoutException;
import com.mongodb.client.*;
import com.mongodb.client.result.UpdateResult;
import org.bson.BsonBoolean;
import org.bson.Document;

import java.util.concurrent.atomic.AtomicInteger;

public class ShardTestApp {

    private String _ID = "_id";
    private AtomicInteger id;

    private final static Logger LOGGER = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);

    static public void main(String[] args) {

        ShardTestApp app = new ShardTestApp();
        app.run();
    }

    private void run() {
        // Set logging
        LogManager logmngr = LogManager.getLogManager();
        Logger log = logmngr.getLogger(Logger.GLOBAL_LOGGER_NAME);
        Logger l0 = Logger.getLogger("");
        l0.removeHandler(l0.getHandlers()[0]);

        try {
            FileHandler fh = new FileHandler("shardtest.log", true);
            log.addHandler(fh);
            fh.setFormatter(new Formatter() {
                @Override
                public String format(LogRecord record) {
                    SimpleDateFormat logTime = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss.SSS");
                    Calendar cal = new GregorianCalendar();
                    cal.setTimeInMillis(record.getMillis());
                    return record.getLevel()
                            + " "
                            + logTime.format(cal.getTime())
                            + " "
                            + record.getSourceClassName().substring(
                            record.getSourceClassName().lastIndexOf(".") + 1,
                            record.getSourceClassName().length())
                            + "."
                            + record.getSourceMethodName()
                            + " "
                            + record.getMessage() + "\n";
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }

        log.log(Level.INFO, "Init test");

        id = new AtomicInteger(0);

        try {
            // connect to cluster
            MongoClient client = MongoClients.create("mongodb://m17.mdb.com:27000,m18.mdb.com:27000,m19.mdb.com:27000");
            String databaseName = "test";
            MongoDatabase database = client.getDatabase(databaseName);

            String collectionName = "test-002";
            MongoCollection<Document> collection = database.getCollection(collectionName);

            boolean create = true;
            if (create) {
                try {
                    collection.drop();
                } catch (MongoExecutionTimeoutException e) {
                    log.severe("could not drop collection...");
                }
                collection.insertOne(createDoc(id.getAndIncrement()));

                collection.createIndex(new Document(_ID, "hashed"));

                Document cmd = new Document("shardCollection", databaseName + "." + collectionName).append("key", new Document("_id", "hashed"));

                MongoDatabase admin = client.getDatabase("admin");
                Document result = admin.runCommand(cmd);

            }
            // Rapidly insert many documents (to beat the balancer)
            int BATCH_SIZE = 1000;
            int BATCH_COUNT = 100;
            for (int i = 0; i < BATCH_COUNT; ++i) {
                log.info("Batch " + i);
                bulkInsert(collection, BATCH_SIZE);
            }

            // continue insertion on collection, at a slower pace
            InsertThread t = new InsertThread(client, databaseName, collectionName, id);
            t.start();

            // update documents, 4 at a time.
            int i = 1;
            while (true) {
                Set<Integer> s = new HashSet<Integer>();
                s.add(i * 1);
                s.add(i * 3);
                s.add(i * 5);
                s.add(i * 7);

                UpdateResult updateResult = updateDocuments(collection, s);
                log.info("Updated " + s.size() + "  docs to keep the cluster warm ( "
                        + i * 1 + "," + i * 3 + "," + i * 5 + "," + i * 7 + ")"
                        + " modified " + updateResult.getModifiedCount()
                        + " matched " + updateResult.getMatchedCount());


                Document filter = new Document("_id", new Document("$in", Arrays.asList(1 * i, 3 * i, 5 * i, 7 * i)));
                collection.find(filter).forEach(new Block<Document>() {
                    @Override
                    public void apply(Document document) {
                        log.info("key: " + document.getInteger(_ID));
                    }
                });

                Thread.sleep(500, 0);

                ++i;
            }

            //client.close();
            //log.info("End.");
        } catch (MongoException | InterruptedException e) {
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


    private UpdateResult updateDocuments(MongoCollection<Document> collection, Set<Integer> ids) {

        Document updateDate = new Document();
        updateDate.put("$currentDate", new Document("updateTimeStamp", BsonBoolean.TRUE));
        Document update = new Document("$set", updateDate);

        return collection.updateMany(new Document(_ID, new Document("$in", ids)), updateDate);

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
