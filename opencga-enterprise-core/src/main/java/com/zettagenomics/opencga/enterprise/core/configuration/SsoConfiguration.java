package com.zettagenomics.opencga.enterprise.core.configuration;

import java.util.Map;

public class SsoConfiguration extends AbstractModuleConfiguration {

    private String casServerPrefixUrl;
    private String serverName;

    private String protocol; // CAS, SAML1 values supported
    private SsoPrincipalAttributesConfiguration attributes;
    private Map<String, String> initParameters;

    public SsoConfiguration() {
        super();
    }

    public SsoConfiguration(boolean active, String casServerPrefixUrl, String serverName, String protocol,
                            SsoPrincipalAttributesConfiguration attributes, Map<String, String> initParameters) {
        super(active);
        this.casServerPrefixUrl = casServerPrefixUrl;
        this.serverName = serverName;
        this.protocol = protocol;
        this.attributes = attributes;
        this.initParameters = initParameters;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SsoConfiguration{");
        sb.append("casServerPrefixUrl='").append(casServerPrefixUrl).append('\'');
        sb.append(", serverName='").append(serverName).append('\'');
        sb.append(", protocol='").append(protocol).append('\'');
        sb.append(", attributes=").append(attributes);
        sb.append(", active=").append(active);
        sb.append(", initParameters=").append(initParameters);
        sb.append('}');
        return sb.toString();
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

    public String getProtocol() {
        return protocol;
    }

    public SsoConfiguration setProtocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    public SsoPrincipalAttributesConfiguration getAttributes() {
        return attributes;
    }

    public SsoConfiguration setAttributes(SsoPrincipalAttributesConfiguration attributes) {
        this.attributes = attributes;
        return this;
    }

    public Map<String, String> getInitParameters() {
        return initParameters;
    }

    public SsoConfiguration setInitParameters(Map<String, String> initParameters) {
        this.initParameters = initParameters;
        return this;
    }
}
