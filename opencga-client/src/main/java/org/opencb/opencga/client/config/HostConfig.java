package org.opencb.opencga.client.config;

public class HostConfig {

    private String name;
    private String url;
    private boolean defaultHost;

    public HostConfig() {
    }

    public HostConfig(String name, String url, boolean defaultHost) {
        this.name = name;
        this.url = url;
        this.defaultHost = defaultHost;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("HostConfig{");
        sb.append("name='").append(name).append('\'');
        sb.append(", url='").append(url).append('\'');
        sb.append(", defaultHost=").append(defaultHost);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public HostConfig setName(String name) {
        this.name = name;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public HostConfig setUrl(String url) {
        this.url = url;
        return this;
    }

    public boolean isDefaultHost() {
        return defaultHost;
    }

    public HostConfig setDefaultHost(boolean defaultHost) {
        this.defaultHost = defaultHost;
        return this;
    }
}
