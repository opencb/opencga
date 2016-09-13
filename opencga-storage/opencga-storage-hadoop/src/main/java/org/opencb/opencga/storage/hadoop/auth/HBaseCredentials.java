package org.opencb.opencga.storage.hadoop.auth;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.auth.OpenCGACredentials;

import java.net.URI;
import java.net.URISyntaxException;

import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZOOKEEPER_ZNODE_PARENT;
import static org.apache.hadoop.hbase.HConstants.DEFAULT_ZOOKEPER_CLIENT_PORT;

/**
 * Created by mh719 on 16/06/15.
 */
public class HBaseCredentials implements OpenCGACredentials {

    @Deprecated
    /**
     * @deprecated Use default value from {@link org.apache.hadoop.hbase.HConstants}
     */
    private static final Integer DEFAULT_PORT = 60000;
    private static final String DEFAULT_HOST = "auto";
    private final String host;
    private final int hbasePort;
    private final String table;
    private final String pass;
    private final String user;
    /**
     * The ZookeeperZnodeParent is an absolute path. MUST START with '/'.
     * @see #checkAbsoluteZookeeperZnode
     **/
    private final String zookeeperZnode;
    private Integer hbaseZookeeperClientPort = DEFAULT_ZOOKEPER_CLIENT_PORT;

    public HBaseCredentials(String host, String table, String user, String pass) {
        this(host, table, user, pass, DEFAULT_PORT, DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    }

    public HBaseCredentials(String host, String table, String user, String pass, Integer hbasePort) {
        this(host, table, user, pass, hbasePort, DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    }

    public HBaseCredentials(String host, String table, String user, String pass, Integer hbasePort,
                            String zookeeperZnode) {
        if (host.equals(DEFAULT_HOST)) {
            host = "";
        }
        this.host = host;
        this.hbasePort = hbasePort;
        this.table = table;
        this.user = user;
        this.pass = pass;
        this.zookeeperZnode = checkAbsoluteZookeeperZnode(zookeeperZnode);
    }

    public boolean isDefaultZookeeperZnode() {
        return zookeeperZnode == null || zookeeperZnode.equals(DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    }

    public String getZookeeperZnode() {
        return this.zookeeperZnode;
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

    public boolean isDefaultZookeeperClientPort() {
        return hbaseZookeeperClientPort == null || hbaseZookeeperClientPort == DEFAULT_ZOOKEPER_CLIENT_PORT;
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
        Integer port = uri.getPort() > 0 ? uri.getPort() : DEFAULT_PORT;
        String zookeeperPath = checkAbsoluteZookeeperZnode(uri.getPath());
        if (!StringUtils.isBlank(zookeeperPath)) {
            return new HBaseCredentials(server, table, user, pass, port, zookeeperPath);
        }
        return new HBaseCredentials(server, table, user, pass, port);
    }

    public URI getHostUri() {
        String zooPath = StringUtils.equals(DEFAULT_ZOOKEEPER_ZNODE_PARENT, getZookeeperZnode()) ? null : getZookeeperZnode();
        try {
            String host = StringUtils.defaultIfEmpty(getHost(), DEFAULT_HOST);
            return new URI("hbase", null, host, getHbasePort(), zooPath, null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    /**
     * Checks if the znode path field is absolute, i.e. starts with '/'
     * @param znode Znode parent
     * @return      Fixed znode
     */
    public static String checkAbsoluteZookeeperZnode(String znode) {
        if (StringUtils.isEmpty(znode) || znode.equals("/")) {
            return null;
        } else if (!znode.startsWith("/")) {
            return "/" + znode;
        } else {
            return znode;
        }
    }

}
