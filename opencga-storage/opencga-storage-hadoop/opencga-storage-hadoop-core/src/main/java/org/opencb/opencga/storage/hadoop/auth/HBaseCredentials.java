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

package org.opencb.opencga.storage.hadoop.auth;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.core.auth.OpenCGACredentials;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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

    private final String table;
    private final String pass;
    private final String user;
    /**
     * The ZookeeperZnodeParent is an absolute path. MUST START with '/'.
     * @see #checkAbsoluteZookeeperZnode
     **/
    private final String zookeeperZnode;
    private final List<String> zookeeperQuorumList;
    private final String zookeeperQuorums;

    private Integer hbaseZookeeperClientPort = DEFAULT_ZOOKEPER_CLIENT_PORT;

    public HBaseCredentials(String host, String table, String user, String pass) {
        this(host, table, user, pass, DEFAULT_PORT, DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    }

    public HBaseCredentials(String host, String table, String user, String pass, Integer hbasePort) {
        this(host, table, user, pass, hbasePort, DEFAULT_ZOOKEEPER_ZNODE_PARENT);
    }

    public HBaseCredentials(String zookeeperQuorum, String table, String user, String pass, Integer hbasePort,
                            String zookeeperZnode) {
        if (zookeeperQuorum.equals(DEFAULT_HOST)) {
            zookeeperQuorum = "";
        }

        if (StringUtils.isNotBlank(zookeeperQuorum)) {
            zookeeperQuorumList = Collections.unmodifiableList(Arrays.asList(zookeeperQuorum.split(",")));
            zookeeperQuorums = zookeeperQuorum;
        } else {
            zookeeperQuorumList = Collections.emptyList();
            zookeeperQuorums = "";
        }
        this.table = table;
        this.user = user;
        this.pass = pass;
        this.zookeeperZnode = checkAbsoluteZookeeperZnode(zookeeperZnode);
    }

    public HBaseCredentials(String string) {
        this(Arrays.asList(string.split(",")));
    }

    /**
     *
     * @param credentials List of credentials as URIs : hbase://zooqeeperQuorum:port/znodeParent
     */
    public HBaseCredentials(List<String> credentials) {
        List<String> list = new LinkedList<>();
        String znodeParent = DEFAULT_ZOOKEEPER_ZNODE_PARENT;
        for (String s : credentials) {
            URI uri = URI.create(s);
            znodeParent = uri.getPath();
            list.add(uri.getAuthority());
        }
        zookeeperQuorumList = Collections.unmodifiableList(list);
        zookeeperQuorums = String.join(",", list);
        table = null;
        user = null;
        pass = null;
        zookeeperZnode = znodeParent;
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
//
//    public String getHost() {
//        return host;
//    }

    public List<String> getZookeeperQuorumList() {
        return zookeeperQuorumList;
    }

    public String getZookeeperQuorums() {
        return zookeeperQuorums;
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

//    public int getHbasePort() {
//        return hbasePort;
//    }

//    public String getHostAndPort() {
//        return String.join(":", getHost(), Integer.toString(getHbasePort()));
//    }

    @Override
    public boolean check() throws IllegalOpenCGACredentialsException {
        return true; // TODO not sure how to check
    }

    @Override
    public String toJson() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Deprecated
    public static HBaseCredentials fromURI(URI uri, String table, String user, String pass) {
        String server = uri.getHost();
        Integer port = uri.getPort() > 0 ? uri.getPort() : DEFAULT_PORT;
        String zookeeperPath = checkAbsoluteZookeeperZnode(uri.getPath());
        if (!StringUtils.isBlank(zookeeperPath)) {
            return new HBaseCredentials(server, table, user, pass, port, zookeeperPath);
        }
        return new HBaseCredentials(server, table, user, pass, port);
    }

    @Deprecated
    public URI getHostUri() {
        String zooPath = StringUtils.equals(DEFAULT_ZOOKEEPER_ZNODE_PARENT, getZookeeperZnode()) ? null : getZookeeperZnode();
        try {
            String host = StringUtils.defaultIfEmpty(getZookeeperQuorumList().get(0), DEFAULT_HOST);
            return new URI("hbase", null, host, -1, zooPath, null, null);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e.getMessage(), e);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
//        String zooPath = StringUtils.equals(DEFAULT_ZOOKEEPER_ZNODE_PARENT, getZookeeperZnode()) ? null : getZookeeperZnode();
        String zooPath = getZookeeperZnode();
        if (zookeeperQuorumList.isEmpty()) {
            sb.append(DEFAULT_HOST);
        } else {
            for (String zookeeperQuorum : zookeeperQuorumList) {
                if (sb.length() > 0) {
                    sb.append(',');
                }
                sb.append("hbase://").append(zookeeperQuorum);
                if (zooPath != null) {
                    sb.append(zooPath);
                }
            }
        }
        return sb.toString();
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof HBaseCredentials)) {
            return false;
        }

        HBaseCredentials that = (HBaseCredentials) o;

        if (table != null ? !table.equals(that.table) : that.table != null) {
            return false;
        }
        if (pass != null ? !pass.equals(that.pass) : that.pass != null) {
            return false;
        }
        if (user != null ? !user.equals(that.user) : that.user != null) {
            return false;
        }
        if (zookeeperZnode != null ? !zookeeperZnode.equals(that.zookeeperZnode) : that.zookeeperZnode != null) {
            return false;
        }
        if (zookeeperQuorumList != null ? !zookeeperQuorumList.equals(that.zookeeperQuorumList) : that.zookeeperQuorumList != null) {
            return false;
        }
        return zookeeperQuorums != null ? zookeeperQuorums.equals(that.zookeeperQuorums) : that.zookeeperQuorums == null;

    }

    @Override
    public int hashCode() {
        int result = table != null ? table.hashCode() : 0;
        result = 31 * result + (pass != null ? pass.hashCode() : 0);
        result = 31 * result + (user != null ? user.hashCode() : 0);
        result = 31 * result + (zookeeperZnode != null ? zookeeperZnode.hashCode() : 0);
        result = 31 * result + (zookeeperQuorumList != null ? zookeeperQuorumList.hashCode() : 0);
        result = 31 * result + (zookeeperQuorums != null ? zookeeperQuorums.hashCode() : 0);
        return result;
    }
}
