package com.zettagenomics.opencga.enterprise.core.configuration;

public class SsoConfiguration extends AbstractModuleConfiguration {

    private String casServerPrefixUrl;
    private String serverName;

    public SsoConfiguration() {
        super();
    }

    public SsoConfiguration(boolean active, String casServerPrefixUrl, String serverName) {
        super(active);
        this.casServerPrefixUrl = casServerPrefixUrl;
        this.serverName = serverName;
    }

    public String getCasServerPrefixUrl() {
        return casServerPrefixUrl;
    }

    public SsoConfiguration setCasServerPrefixUrl(String casServerPrefixUrl) {
        this.casServerPrefixUrl = casServerPrefixUrl;
        return this;
    }

    public String getServerName() {
        return serverName;
    }

    public SsoConfiguration setServerName(String serverName) {
        this.serverName = serverName;
        return this;
    }
}
