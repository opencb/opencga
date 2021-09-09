package org.opencb.opencga.server.json.config;

public class Configuration {
    private Options options;
    private ApiConfig apiConfig;

    public Configuration() {
    }

    public Configuration(Options options, ApiConfig apiConfig) {
        this.options = options;
        this.apiConfig = apiConfig;
    }

    public ApiConfig getApiConfig() {
        return apiConfig;
    }

    public Configuration setApiConfig(ApiConfig apiConfig) {
        this.apiConfig = apiConfig;
        return this;
    }

    public Options getOptions() {
        return options;
    }

    public Configuration setOptions(Options options) {
        this.options = options;
        return this;
    }
}
