package org.opencb.opencga.server.json.config;

import java.util.ArrayList;
import java.util.List;

public class CommandLineConfiguration {
    private Options options;
    private ApiConfig apiConfig;

    public CommandLineConfiguration() {
    }

    public CommandLineConfiguration(Options options, ApiConfig apiConfig) {
        this.options = options;
        this.apiConfig = apiConfig;
    }

    public ApiConfig getApiConfig() {
        return apiConfig;
    }

    public CommandLineConfiguration setApiConfig(ApiConfig apiConfig) {
        this.apiConfig = apiConfig;
        return this;
    }

    public Options getOptions() {
        return options;
    }

    public CommandLineConfiguration setOptions(Options options) {
        this.options = options;
        return this;
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
}
