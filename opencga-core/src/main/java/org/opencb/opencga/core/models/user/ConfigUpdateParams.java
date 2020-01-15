package org.opencb.opencga.core.models.user;

import java.util.Map;

public class ConfigUpdateParams {

    private String id;
    private Map<String, Object> configuration;

    public ConfigUpdateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ConfigUpdateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", configuration=").append(configuration);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ConfigUpdateParams setId(String id) {
        this.id = id;
        return this;
    }

    public Map<String, Object> getConfiguration() {
        return configuration;
    }

    public ConfigUpdateParams setConfiguration(Map<String, Object> configuration) {
        this.configuration = configuration;
        return this;
    }
}
