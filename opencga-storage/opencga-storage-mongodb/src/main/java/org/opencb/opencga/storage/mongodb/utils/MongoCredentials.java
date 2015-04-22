package org.opencb.opencga.storage.mongodb.utils;

import com.mongodb.MongoCredential;
import org.opencb.datastore.core.config.DataStoreServerAddress;
import org.opencb.datastore.mongodb.MongoDBConfiguration;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.OpenCGACredentials;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class MongoCredentials implements OpenCGACredentials {

    List<DataStoreServerAddress> dataStoreServerAddresses;
    private final String mongoDbName;
    @Deprecated private MongoCredential mongoCredentials;
    private String authenticationDatabase;


    public MongoCredentials(String mongoHost, int mongoPort, String mongoDbName, String mongoUser, String mongoPassword)
            throws IllegalOpenCGACredentialsException {
        dataStoreServerAddresses = new LinkedList<>();
        dataStoreServerAddresses.add(new DataStoreServerAddress(mongoHost, mongoPort));
        this.mongoDbName = mongoDbName;
        if (mongoUser != null && mongoPassword != null) {
            mongoCredentials = MongoCredential.createMongoCRCredential(mongoUser, mongoDbName, mongoPassword.toCharArray());
        }

        check();
    }

    public MongoCredentials(List<DataStoreServerAddress> dataStoreServerAddresses, String dbName, String mongoUser, String mongoPassword)
            throws IllegalOpenCGACredentialsException {
        this.dataStoreServerAddresses = dataStoreServerAddresses;
        this.mongoDbName = dbName;
        if (mongoUser != null && mongoPassword != null) {
            mongoCredentials = MongoCredential.createMongoCRCredential(mongoUser, mongoDbName, mongoPassword.toCharArray());
        }

        check();
    }

    @Deprecated
    public MongoCredentials(Properties properties) {
        dataStoreServerAddresses = new LinkedList<>();
        String mongoHost = properties.getProperty("mongo_host");
        int mongoPort = Integer.parseInt(properties.getProperty("mongo_port", "-1"));
        dataStoreServerAddresses.add(new DataStoreServerAddress(mongoHost, mongoPort));
        this.mongoDbName = properties.getProperty("mongo_db_name");
        String mongoUser = properties.getProperty("mongo_user", null);
        String mongoPassword = properties.getProperty("mongo_password", null);
        if (mongoUser != null && mongoPassword != null) {
            mongoCredentials = MongoCredential.createMongoCRCredential(mongoUser, mongoDbName, mongoPassword.toCharArray());
        }
    }

    @Override
    public boolean check() throws IllegalOpenCGACredentialsException {
        return true;
    }

    @Override
    public String toJson() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Deprecated
    public MongoCredential getMongoCredentials() {
        return mongoCredentials;
    }

    public String getMongoDbName() {
        return mongoDbName;
    }

    public List<DataStoreServerAddress> getDataStoreServerAddresses() {
        return dataStoreServerAddresses;
    }

    public MongoDBConfiguration getMongoDBConfiguration() {
        MongoDBConfiguration.Builder builder = MongoDBConfiguration.builder()
                .add("username", this.getUsername());
        if (this.getPassword() != null) {
            builder.add("password", new String(this.getPassword()));
        }
        if (authenticationDatabase != null && !authenticationDatabase.isEmpty()) {
            builder.add("authenticationDatabase", authenticationDatabase);
        }
        return builder.build();
    }

    public String getUsername() {
        return mongoCredentials != null ? mongoCredentials.getUserName() : null;
    }
    
    public char[] getPassword() {
        return mongoCredentials != null ? mongoCredentials.getPassword() : null;
    }

    public String getAuthenticationDatabase() {
        return authenticationDatabase;
    }

    public void setAuthenticationDatabase(String authenticationDatabase) {
        this.authenticationDatabase = authenticationDatabase;
    }

    public static List<DataStoreServerAddress> parseDataStoreServerAddresses(String hosts) {
        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String hostPort : hosts.split(",")) {
            if (hostPort.contains(":")) {
                String[] split = hostPort.split(":");
                Integer port = Integer.valueOf(split[1]);
                dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
            } else {
                dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
            }
        }
        return dataStoreServerAddresses;
    }

}
