package org.opencb.opencga.server.generator.config;

import java.util.ArrayList;
import java.util.List;

public class CommandLineConfiguration {

    private Options options;
    private ApiConfig apiConfig;

    public CommandLineConfiguration() {}

    public CommandLineConfiguration(Options options, ApiConfig apiConfig) {
        this.options = options;
        this.apiConfig = apiConfig;
    }

    public void initialize() {
        List<String> aux = new ArrayList<>();
        for (String type : getOptions().getIgnoreTypes()) {
            if (!type.endsWith(";")) {
                aux.add(type + ";");
            }
        }
        getOptions().getIgnoreTypes().addAll(aux);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CommandLineConfiguration{");
        sb.append("options=").append(options);
        sb.append(", apiConfig=").append(apiConfig);
        sb.append('}');
        return sb.toString();
    }

    public Options getOptions() {
        return options;
    }

    public CommandLineConfiguration setOptions(Options options) {
        this.options = options;
        return this;
    }

    public ApiConfig getApiConfig() {
        return apiConfig;
    }

    public CommandLineConfiguration setApiConfig(ApiConfig apiConfig) {
        this.apiConfig = apiConfig;
        return this;
    }
}
