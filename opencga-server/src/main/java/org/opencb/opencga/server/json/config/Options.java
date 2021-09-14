package org.opencb.opencga.server.json.config;

import java.util.List;

public class Options {

    private String serverUrl;
    private String version;
    private String parserOutputDir;
    private String optionsOutputDir;
    private String executorsOutputDir;
    private String executorsPackage;
    private String optionsPackage;
    private String parserPackage;
    private List<String> ignoreTypes;

    public Options() {
    }

    public Options(String serverUrl, String parserOutputDir, String optionsOutputDir, String executorsOutputDir,
                   String executorsPackage, String optionsPackage, String parserPackage, List<String> ignoreTypes) {
        this.serverUrl = serverUrl;
        this.parserOutputDir = parserOutputDir;
        this.optionsOutputDir = optionsOutputDir;
        this.executorsOutputDir = executorsOutputDir;
        this.executorsPackage = executorsPackage;
        this.optionsPackage = optionsPackage;
        this.parserPackage = parserPackage;
        this.ignoreTypes = ignoreTypes;
    }

    public String getServerUrl() {
        return serverUrl;
    }

    public Options setServerUrl(String serverUrl) {
        this.serverUrl = serverUrl;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public Options setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getParserOutputDir() {
        return parserOutputDir;
    }

    public Options setParserOutputDir(String parserOutputDir) {
        this.parserOutputDir = parserOutputDir;
        return this;
    }

    public String getOptionsOutputDir() {
        return optionsOutputDir;
    }

    public Options setOptionsOutputDir(String optionsOutputDir) {
        this.optionsOutputDir = optionsOutputDir;
        return this;
    }

    public String getExecutorsOutputDir() {
        return executorsOutputDir;
    }

    public Options setExecutorsOutputDir(String executorsOutputDir) {
        this.executorsOutputDir = executorsOutputDir;
        return this;
    }

    public String getExecutorsPackage() {
        return executorsPackage;
    }

    public Options setExecutorsPackage(String executorsPackage) {
        this.executorsPackage = executorsPackage;
        return this;
    }

    public String getOptionsPackage() {
        return optionsPackage;
    }

    public Options setOptionsPackage(String optionsPackage) {
        this.optionsPackage = optionsPackage;
        return this;
    }

    public String getParserPackage() {
        return parserPackage;
    }

    public Options setParserPackage(String parserPackage) {
        this.parserPackage = parserPackage;
        return this;
    }

    public List<String> getIgnoreTypes() {
        return ignoreTypes;
    }

    public Options setIgnoreTypes(List<String> ignoreTypes) {
        this.ignoreTypes = ignoreTypes;
        return this;
    }
}
