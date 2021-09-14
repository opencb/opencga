package org.opencb.opencga.server.json.config;

import java.util.List;

public class ApiConfig {
    private List<CategoryConfig> categoryConfigList;

    public ApiConfig() {
    }

    public ApiConfig(List<CategoryConfig> categoryConfigList) {
        this.categoryConfigList = categoryConfigList;
    }

    public List<CategoryConfig> getCategoryConfigList() {
        return categoryConfigList;
    }

    public ApiConfig setCategoryConfigList(List<CategoryConfig> categoryConfigList) {
        this.categoryConfigList = categoryConfigList;
        return this;
    }
}
