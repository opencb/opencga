package org.opencb.opencga.storage.core.config;


/**
 * Created by wasim on 09/11/16.
 */
public class SearchConfiguration {

    private String host;
    private String collection;
    private String user;
    private String password;
    private boolean active;

    public static final boolean DEFAULT_ACTVE = true;
    public static final String DEFAULT_HOST = "localhost:8983/solr/";
    public static final String DEFAULT_COLLECTION = "variants";
    public static final String DEFAULT_PASSWORD = "";
    public static final String DEFAULT_USER = "";

    public SearchConfiguration() {
        this(DEFAULT_HOST, DEFAULT_COLLECTION, DEFAULT_USER, DEFAULT_PASSWORD, DEFAULT_ACTVE);
    }

    public SearchConfiguration(String host, String collection, String user, String password, boolean active) {
        this.host = host;
        this.collection = collection;
        this.user = user;
        this.password = password;
        this.active = active;
    }

    public String getHost() {
        return host;
    }

    public SearchConfiguration setHost(String host) {
        this.host = host;
        return this;
    }

    public String getCollection() {
        return collection;
    }

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

    @Override
    public String toString() {
        return "SearchConfiguration{"
                + "host='" + host + '\''
                + ", collection='" + collection + '\''
                + ", user='" + user + '\''
                + ", active='" + active + '\''
                + '}';
    }

}
