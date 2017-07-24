/*
 * Copyright 2015-2017 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.mongodb.auth;

import com.mongodb.MongoException;
import com.mongodb.client.MongoIterable;
import org.opencb.commons.datastore.core.DataStoreServerAddress;
import org.opencb.commons.datastore.mongodb.MongoDBConfiguration;
import org.opencb.commons.datastore.mongodb.MongoDataStore;
import org.opencb.commons.datastore.mongodb.MongoDataStoreManager;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.auth.OpenCGACredentials;
import org.opencb.opencga.storage.core.config.DatabaseCredentials;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class MongoCredentials implements OpenCGACredentials {

    private final String mongoDbName;
    private MongoDBConfiguration mongoDBConfiguration;

    public MongoCredentials(String mongoHost, int mongoPort, String mongoDbName, String mongoUser, String mongoPassword,
                            boolean checkConnection)
            throws IllegalOpenCGACredentialsException {
        this.mongoDbName = mongoDbName;
        mongoDBConfiguration = MongoDBConfiguration.builder()
                .setUserPassword(mongoUser, mongoPassword)
                .addServerAddress(new DataStoreServerAddress(mongoHost, mongoPort))
                .build();

        if (checkConnection) {
            check();
        }
    }

    public MongoCredentials(DatabaseCredentials database, String dbName) throws IllegalOpenCGACredentialsException {
        this(MongoCredentials.parseDataStoreServerAddresses(database.getHosts()), dbName,
                database.getUser(), database.getPassword(), database.getOptions(), false);
    }

    public MongoCredentials(List<DataStoreServerAddress> dataStoreServerAddresses, String dbName, String mongoUser, String mongoPassword)
            throws IllegalOpenCGACredentialsException {
        this(dataStoreServerAddresses, dbName, mongoUser, mongoPassword, Collections.emptyMap(), false);
    }

    public MongoCredentials(List<DataStoreServerAddress> dataStoreServerAddresses, String dbName, String mongoUser, String mongoPassword,
                            Map<? extends String, ?> inputOptions, boolean checkConnection)
            throws IllegalOpenCGACredentialsException {
        this.mongoDbName = dbName;
        mongoDBConfiguration = MongoDBConfiguration.builder()
                .setUserPassword(mongoUser, mongoPassword)
                .setServerAddress(dataStoreServerAddresses)
                .load(inputOptions == null ? Collections.emptyMap() : inputOptions)
                .build();

        if (checkConnection) {
            check();
        }
    }


    @Override
    public boolean check() {
        try (MongoDataStoreManager mongoManager = new MongoDataStoreManager(getDataStoreServerAddresses())) {
            MongoDataStore db = mongoManager.get(getMongoDbName(), getMongoDBConfiguration());
            MongoIterable<String> strings = db.getDb().listCollectionNames();
            int count = 0;
            for (String string : strings) {
                count++;
            }
            return true;
        } catch (MongoException e) {
            //FIXME: Throw IllegalOpenCGACredentialsException ??
            return false;
        }
    }

    @Override
    public String toJson() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public String getMongoDbName() {
        return mongoDbName;
    }

    public List<DataStoreServerAddress> getDataStoreServerAddresses() {
        return mongoDBConfiguration.getAsList(MongoDBConfiguration.SERVER_ADDRESS, DataStoreServerAddress.class);
    }

    public MongoDBConfiguration getMongoDBConfiguration() {
        return mongoDBConfiguration;
    }

    public String getUsername() {
        return mongoDBConfiguration.getString(MongoDBConfiguration.USERNAME);
    }

    public char[] getPassword() {
        return mongoDBConfiguration.getString(MongoDBConfiguration.PASSWORD, "").toCharArray();
    }

    public String getAuthenticationDatabase() {
        return mongoDBConfiguration.getString(MongoDBConfiguration.AUTHENTICATION_DATABASE);
    }

    public void setAuthenticationDatabase(String authenticationDatabase) {
        mongoDBConfiguration.put(MongoDBConfiguration.AUTHENTICATION_DATABASE, authenticationDatabase);
    }

    public static List<DataStoreServerAddress> parseDataStoreServerAddresses(String hosts) {
        return parseDataStoreServerAddresses(Collections.singletonList(hosts));
    }

    public static List<DataStoreServerAddress> parseDataStoreServerAddresses(List<String> hosts) {
        List<DataStoreServerAddress> dataStoreServerAddresses = new LinkedList<>();
        for (String host : hosts) {
            for (String hostPort : host.split(",")) {
                if (hostPort.contains(":")) {
                    String[] split = hostPort.split(":");
                    Integer port = Integer.valueOf(split[1]);
                    dataStoreServerAddresses.add(new DataStoreServerAddress(split[0], port));
                } else {
                    dataStoreServerAddresses.add(new DataStoreServerAddress(hostPort, 27017));
                }
            }
        }
        return dataStoreServerAddresses;
    }

}
