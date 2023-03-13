package org.opencb.opencga.server.generator.config;

import java.util.List;

public class ApiConfig {


    private List<CategoryConfig> categoryConfigList;
    private List<Shortcut> shortcuts;
    private String executorsParentClass;
    private String optionsParserParentClass;

    public ApiConfig() {
    }

    public ApiConfig(List<CategoryConfig> categoryConfigList) {
        this.categoryConfigList = categoryConfigList;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ApiConfig{");
        sb.append("categoryConfigList=").append(categoryConfigList);
        sb.append(", shortcuts=").append(shortcuts);
        sb.append(", executorsParentClass='").append(executorsParentClass).append('\'');
        sb.append(", optionsParserParentClass='").append(optionsParserParentClass).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public List<CategoryConfig> getCategoryConfigList() {
        return categoryConfigList;
    }

    public ApiConfig setCategoryConfigList(List<CategoryConfig> categoryConfigList) {
        this.categoryConfigList = categoryConfigList;
        return this;
    }

    public List<Shortcut> getShortcuts() {
        return shortcuts;
    }

    public ApiConfig setShortcuts(List<Shortcut> shortcuts) {
        this.shortcuts = shortcuts;
        return this;
    }

    public String getExecutorsParentClass() {
        return executorsParentClass;
    }

    public ApiConfig setExecutorsParentClass(String executorsParentClass) {
        this.executorsParentClass = executorsParentClass;
        return this;
    }

    public String getOptionsParserParentClass() {
        return optionsParserParentClass;
    }

    public ApiConfig setOptionsParserParentClass(String optionsParserParentClass) {
        this.optionsParserParentClass = optionsParserParentClass;
        return this;
    }
}
