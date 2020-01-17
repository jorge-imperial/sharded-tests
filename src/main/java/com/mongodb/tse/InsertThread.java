package com.mongodb.tse;


import com.mongodb.Mongo;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

class InsertThread extends Thread {
    private final MongoClient client;
    private final AtomicInteger id;
    private String collName;
    private String dbName;


    InsertThread(MongoClient client, String databaseName, String collectionName, AtomicInteger id) {
        this.client = client;
        this.dbName = databaseName;
        this.collName = collectionName;
        this.id = id;
    }

    public void run() {
        // get collection
        MongoDatabase database = client.getDatabase(dbName);
        MongoCollection<Document> collection = database.getCollection(collName);
        // insert
        while (true) {
            bulkInsert(collection, 100);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    private void bulkInsert(MongoCollection<Document> collection, int batchSize) {
        List<Document> docs = new ArrayList<>();
        for (int i = 1; i < batchSize; i++) {
            docs.add(createDoc(id.getAndIncrement()));
        }
        collection.insertMany(docs);
    }


    private Document createDoc(int index) {
        return new Document("_id", index)
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