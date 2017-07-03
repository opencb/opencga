package org.opencb.opencga.storage.core.config;


/**
 * Created by wasim on 09/11/16.
 */
public class SearchConfiguration {

    private String host;
    private String mode;
    @Deprecated
    private String collection;
    private String user;
    private String password;
    private boolean active;
    private int timeout;
    private int rows;

    private static final String DEFAULT_HOST = "localhost:8983/solr/";
    private static final String DEFAULT_MODE = "cloud";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_USER = "";
    private static final boolean DEFAULT_ACTIVE = true;
    private static final int DEFAULT_TIMEOUT = 30000;
    private static final int DEFAULT_ROWS = 100000;

    public SearchConfiguration() {
        this(DEFAULT_HOST, DEFAULT_MODE, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_ACTIVE, DEFAULT_TIMEOUT, DEFAULT_ROWS);
    }

    public SearchConfiguration(String host, String mode, String user, String password, boolean active, int timeout, int rows) {
        this.host = host;
        this.mode = mode;
        this.user = user;
        this.password = password;
        this.active = active;
        this.timeout = timeout;
        this.rows = rows;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SearchConfiguration{");
        sb.append("host='").append(host).append('\'');
        sb.append(", mode='").append(mode).append('\'');
        sb.append(", collection='").append(collection).append('\'');
        sb.append(", user='").append(user).append('\'');
        sb.append(", password='").append(password).append('\'');
        sb.append(", active=").append(active);
        sb.append(", timeout=").append(timeout);
        sb.append(", rows=").append(rows);
        sb.append('}');
        return sb.toString();
    }

    public String getHost() {
        return host;
    }

    public SearchConfiguration setHost(String host) {
        this.host = host;
        return this;
    }

    public String getMode() {
        return mode;
    }

    public SearchConfiguration setMode(String mode) {
        this.mode = mode;
        return this;
    }

    @Deprecated
    public String getCollection() {
        return collection;
    }

    @Deprecated
    public SearchConfiguration setCollection(String collection) {
        this.collection = collection;
        return this;
    }

    public String getUser() {
        return user;
    }

    public SearchConfiguration setUser(String user) {
        this.user = user;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public SearchConfiguration setPassword(String password) {
        this.password = password;
        return this;
    }

    public boolean getActive() {
        return active;
    }

    public SearchConfiguration setActive(boolean active) {
        this.active = active;
        return this;
    }

    public int getTimeout() {
        return timeout;
    }

    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    public int getRows() {
        return rows;
    }

    public SearchConfiguration setRows(int rows) {
        this.rows = rows;
        return this;
    }
}
