package org.opencb.opencga.lib.auth;

import com.mongodb.MongoCredential;
import java.util.Map;
import java.util.Properties;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public class MonbaseCredentials implements OpenCGACredentials {

    private final String hbaseMasterHost;
    private final int hbaseMasterPort;
    private final String hbaseZookeeperQuorum;
    private final int hbaseZookeeperClientPort;
    
    private final String mongoHost;
    private final int mongoPort;
    private final String mongoDbName;
    private final MongoCredential mongoCredentials;
    
    
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
        mongoCredentials = MongoCredential.createMongoCRCredential(mongoUser, mongoDbName, mongoPassword.toCharArray());
        
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
