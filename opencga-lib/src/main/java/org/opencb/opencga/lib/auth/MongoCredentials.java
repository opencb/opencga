package org.opencb.opencga.lib.auth;

import com.mongodb.MongoCredential;

import java.util.Properties;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class MongoCredentials implements OpenCGACredentials {

    private final String mongoHost;
    private final int mongoPort;
    private final String mongoDbName;
    private final MongoCredential mongoCredentials;


    public MongoCredentials(String mongoHost, int mongoPort, String mongoDbName, String mongoUser, String mongoPassword)
            throws IllegalOpenCGACredentialsException {
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.mongoDbName = mongoDbName;
        mongoCredentials = MongoCredential.createMongoCRCredential(mongoUser, mongoDbName, mongoPassword.toCharArray());

        check();
    }

    public MongoCredentials(Properties properties) {
        this.mongoHost = properties.getProperty("mongo_host");
        this.mongoPort = Integer.parseInt(properties.getProperty("mongo_port", "-1"));
        this.mongoDbName = properties.getProperty("mongo_db_name");
        mongoCredentials = MongoCredential.createMongoCRCredential(
                properties.getProperty("mongo_user"),
                mongoDbName,
                properties.getProperty("mongo_password", "").toCharArray());
    }

    @Override
    public boolean check() throws IllegalOpenCGACredentialsException {
        return true;
    }

    @Override
    public String toJson() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public MongoCredential getMongoCredentials() {
        return mongoCredentials;
    }

    public String getMongoDbName() {
        return mongoDbName;
    }

    public String getMongoHost() {
        return mongoHost;
    }

    public int getMongoPort() {
        return mongoPort;
    }


}
