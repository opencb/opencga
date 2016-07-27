package org.opencb.opencga.storage.hadoop.auth;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.auth.OpenCGACredentials;

import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by mh719 on 16/06/15.
 */
public class HBaseCredentials implements OpenCGACredentials {

    private static final Integer DEFAULT_PORT = 60000;
    private static final String DEFAULT_ZOOKEEPER_PATH = "hbase";
    private static final Integer DEFAULT_ZOOKEEPER_CLIENT_PORT = 2181;
    private final String host;
    private final int hbasePort;
    private final String table;
    private final String pass;
    private final String user;
    private final String zookeeperPath;
    private Integer hbaseZookeeperClientPort = DEFAULT_ZOOKEEPER_CLIENT_PORT;

    public HBaseCredentials(String host, String table, String user, String pass) {
        this(host, table, user, pass, DEFAULT_PORT, DEFAULT_ZOOKEEPER_PATH);
    }

    public HBaseCredentials(String host, String table, String user, String pass, Integer hbasePort) {
        this(host, table, user, pass, hbasePort, DEFAULT_ZOOKEEPER_PATH);
    }

    public HBaseCredentials(String host, String table, String user, String pass, Integer hbasePort,
                            String zookeeperPath) {
        this.host = host;
        this.hbasePort = hbasePort;
        this.table = table;
        this.user = user;
        this.pass = pass;
        this.zookeeperPath = zookeeperPath;
    }

    public String getZookeeperPath() {
        return this.zookeeperPath;
    }

    public String getPass() {
        return pass;
    }

    public String getUser() {
        return user;
    }

    public String getTable() {
        return table;
    }

    public String getHost() {
        return host;
    }

    public void setHbaseZookeeperClientPort(Integer hbaseZookeeperClientPort) {
        this.hbaseZookeeperClientPort = hbaseZookeeperClientPort;
    }

    public Integer getHbaseZookeeperClientPort() {
        return this.hbaseZookeeperClientPort;
    }

    public int getHbasePort() {
        return hbasePort;
    }

    public String getHostAndPort() {
        return String.join(":", getHost(), Integer.toString(getHbasePort()));
    }

    @Override
    public boolean check() throws IllegalOpenCGACredentialsException {
        return true; // TODO not sure how to check
    }

    @Override
    public String toJson() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public static HBaseCredentials fromURI(URI uri, String table, String user, String pass) {
        String server = uri.getHost();
        Integer port = uri.getPort() > 0 ? uri.getPort() : 60000;
        String zookeeperPath = uri.getPath();
        zookeeperPath = zookeeperPath.startsWith("/") ? zookeeperPath.substring(1) : zookeeperPath;
        if (!StringUtils.isBlank(zookeeperPath)) {
            return new HBaseCredentials(server, table, user, pass, port, zookeeperPath);
        }
        return new HBaseCredentials(server, table, user, pass, port);
    }

    public URI getHostUri() {
        String zooPath = StringUtils.equals(DEFAULT_ZOOKEEPER_PATH, getZookeeperPath()) ? null : getZookeeperPath();
        try {
            return new URI("hbase", null, getHost(), getHbasePort(), zooPath, null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }
}
