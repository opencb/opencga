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

package org.opencb.opencga.core.auth;

//import com.mongodb.MongoCredential;
import java.util.Properties;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
@Deprecated
public class MonbaseCredentials implements OpenCGACredentials {

    private final String hbaseMasterHost;
    private final int hbaseMasterPort;
    private final String hbaseZookeeperQuorum;
    private final int hbaseZookeeperClientPort;
    
    private final String mongoHost;
    private final int mongoPort;
    private final String mongoDbName;
//    private final MongoCredential mongoCredentials;
    
    
    public MonbaseCredentials(String hbaseMasterHost, int hbaseMasterPort, String hbaseZookeeperQuorum, int hbaseZookeeperClientPort, 
                                String mongoHost, int mongoPort, String mongoDbName, String mongoUser, String mongoPassword) 
                                throws IllegalOpenCGACredentialsException {
        this.hbaseMasterHost = hbaseMasterHost;
        this.hbaseMasterPort = hbaseMasterPort;
        this.hbaseZookeeperQuorum = hbaseZookeeperQuorum;
        this.hbaseZookeeperClientPort = hbaseZookeeperClientPort;
        this.mongoHost = mongoHost;
        this.mongoPort = mongoPort;
        this.mongoDbName = mongoDbName;
//        mongoCredentials = MongoCredential.createMongoCRCredential(mongoUser, mongoDbName, mongoPassword.toCharArray());
        
        check();
    }

    public MonbaseCredentials(Properties properties) throws IllegalOpenCGACredentialsException {
        this.hbaseMasterHost = properties.getProperty("hbase_master_host");
        this.hbaseMasterPort = Integer.parseInt(properties.getProperty("hbase_master_port", "-1"));
        this.hbaseZookeeperQuorum = properties.getProperty("hbase_zookeeper_quorum");
        this.hbaseZookeeperClientPort = Integer.parseInt(properties.getProperty("hbase_zookeeper_client_port", "-1"));
        this.mongoHost = properties.getProperty("mongo_host");
        this.mongoPort = Integer.parseInt(properties.getProperty("mongo_port", "-1"));
        this.mongoDbName = properties.getProperty("mongo_db_name");
//        mongoCredentials = MongoCredential.createMongoCRCredential(
//                properties.getProperty("mongo_user"),
//                mongoDbName,
//                properties.getProperty("mongo_password", "").toCharArray());
        check();
    }
    
    
    @Override
    public boolean check() throws IllegalOpenCGACredentialsException {
        if (hbaseMasterHost == null || hbaseMasterHost.length() == 0) {
            throw new IllegalOpenCGACredentialsException("HBase hostname or address is not valid");
        }
        if (hbaseMasterPort < 0 && hbaseMasterPort > 65535) {
            throw new IllegalOpenCGACredentialsException("HBase port number is not valid");
        }
        if (hbaseZookeeperQuorum == null || hbaseZookeeperQuorum.length() == 0) {
            throw new IllegalOpenCGACredentialsException("HBase Zookeper hostname or address is not valid");
        }
        if (hbaseZookeeperClientPort < 0 && hbaseZookeeperClientPort > 65535) {
            throw new IllegalOpenCGACredentialsException("HBase Zookeper port number is not valid");
        }
        
        return true;
    }

    @Override
    public String toJson() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    
    public String getHbaseMasterHost() {
        return hbaseMasterHost;
    }

    public int getHbaseMasterPort() {
        return hbaseMasterPort;
    }

    public int getHbaseZookeeperClientPort() {
        return hbaseZookeeperClientPort;
    }

    public String getHbaseZookeeperQuorum() {
        return hbaseZookeeperQuorum;
    }

//    public MongoCredential getMongoCredentials() {
//        return mongoCredentials;
//    }

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
