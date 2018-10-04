package com.sda;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Connector {

    private static final Logger LOG = Logger.getLogger(Connector.class);

    private MongoClient mongoClient;

    private PropertyLoader propertyLoader = new PropertyLoader();

    public Connector() throws IOException {
    }

    public MongoDatabase connect() {
        ServerAddress serverAddress = new ServerAddress(propertyLoader.getAddress(), propertyLoader.getPort());
        MongoCredential mongoCredential = MongoCredential.createCredential(propertyLoader.getUser(),
                propertyLoader.getDB(), propertyLoader.getPassword());
        List<MongoCredential> list = new ArrayList<>();
        list.add(mongoCredential);
        mongoClient = new MongoClient(serverAddress, list);
        LOG.info("CONNECTION BEGINS");
        return mongoClient.getDatabase(propertyLoader.getDB());
    }
}
