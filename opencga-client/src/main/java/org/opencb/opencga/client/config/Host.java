package org.opencb.opencga.client.config;

public class Host {

    private String name;
    private String url;
    private boolean defaultHost = false;

    public Host() {
    }

    public Host(String name, String url, boolean defaultHost) {
        this.name = name;
        this.url = url;
        this.defaultHost = defaultHost;
    }

    public String getName() {
        return name;
    }

    public Host setName(String name) {
        this.name = name;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public Host setUrl(String url) {
        this.url = url;
        return this;
    }

    public boolean isDefaultHost() {
        return defaultHost;
    }

    public Host setDefaultHost(boolean defaultHost) {
        this.defaultHost = defaultHost;
        return this;
    }
}
