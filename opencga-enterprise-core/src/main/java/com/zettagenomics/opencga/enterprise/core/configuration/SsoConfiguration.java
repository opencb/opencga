package com.zettagenomics.opencga.enterprise.core.configuration;

public class SsoConfiguration extends AbstractModuleConfiguration {

    private String casServerPrefixUrl;
    private String serverName;
    private String pythonBin;

    public SsoConfiguration() {
        super();
    }

    public SsoConfiguration(boolean active, String casServerPrefixUrl, String serverName, String pythonBin) {
        super(active);
        this.casServerPrefixUrl = casServerPrefixUrl;
        this.serverName = serverName;
        this.pythonBin = pythonBin;
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

    public String getPythonBin() {
        return pythonBin;
    }

    public SsoConfiguration setPythonBin(String pythonBin) {
        this.pythonBin = pythonBin;
        return this;
    }
}
