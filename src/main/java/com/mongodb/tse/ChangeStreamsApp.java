package com.mongodb.tse;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import org.bson.Document;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.function.Consumer;
import java.util.logging.*;


public class ChangeStreamsApp {


    public static void main(String[] args) {
        ChangeStreamsApp app = new ChangeStreamsApp();

        app.run();

    }


    private void run() {


        // Set logging
        LogManager logmngr = LogManager.getLogManager();
        Logger log = logmngr.getLogger(Logger.GLOBAL_LOGGER_NAME);
        Logger l0 = Logger.getLogger("");
        l0.removeHandler(l0.getHandlers()[0]);

        try {
            FileHandler fh = new FileHandler("changestream.log", true);
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


        // connect to cluster
        //MongoClient client = MongoClients.create("mongodb://m17.mdb.com:27000,m18.mdb.com:27000,m19.mdb.com:27000");
        MongoClient client = MongoClients.create("mongodb+srv://user:XXXXXXXX@cluster0-om7f7.mongodb.net/test?retryWrites=true&w=majority");


        String databaseName = "test";
        MongoDatabase database = client.getDatabase(databaseName);

        String collectionName = "test001";
        MongoCollection<Document> collection = database.getCollection(collectionName);

        // Wait for changes. The test drops the collection, and that is an event we can not recover from...
        log.info("Waiting for creation of collection and index.");
        collection.watch().forEach((Consumer<? super ChangeStreamDocument<Document>>) doc -> {
            log.info("Wait for drop(). Op type:" + doc.getOperationType() + ". Doc key " + doc.getDocumentKey());
        });

        // ...

        log.info("Waiting for updates...");
        collection.watch().forEach((Consumer<? super ChangeStreamDocument<Document>>) doc -> {
            log.info("Doc key " + doc.getDocumentKey() + " op:" + doc.getOperationType());

        });


        client.close();
    }
}
