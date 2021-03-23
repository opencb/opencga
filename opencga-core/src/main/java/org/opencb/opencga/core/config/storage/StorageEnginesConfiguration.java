package org.opencb.opencga.core.config.storage;

import org.opencb.commons.datastore.core.ObjectMap;

import java.util.ArrayList;
import java.util.List;

public class StorageEnginesConfiguration {

    private String defaultEngine;
    private ObjectMap options;
    private List<StorageEngineConfiguration> engines;

    public StorageEnginesConfiguration() {
        options = new ObjectMap();
        engines = new ArrayList<>(2);
    }

    public String getDefaultEngine() {
        return defaultEngine;
    }

    public StorageEnginesConfiguration setDefaultEngine(String defaultEngine) {
        this.defaultEngine = defaultEngine;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public StorageEnginesConfiguration setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }

    public List<StorageEngineConfiguration> getEngines() {
        return engines;
    }

    public StorageEnginesConfiguration setEngines(List<StorageEngineConfiguration> engines) {
        this.engines = engines;
        return this;
    }
}
