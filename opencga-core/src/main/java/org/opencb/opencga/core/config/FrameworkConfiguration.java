package org.opencb.opencga.core.config;

import org.opencb.commons.datastore.core.ObjectMap;

public class FrameworkConfiguration {

    private String id;
    private boolean available;
    private String queue;
    private ObjectMap options;

    public FrameworkConfiguration() {
        options = new ObjectMap();
    }

    public String getId() {
        return id;
    }

    public FrameworkConfiguration setId(String id) {
        this.id = id;
        return this;
    }

    public boolean isAvailable() {
        return available;
    }

    public FrameworkConfiguration setAvailable(boolean available) {
        this.available = available;
        return this;
    }

    public String getQueue() {
        return queue;
    }

    public FrameworkConfiguration setQueue(String queue) {
        this.queue = queue;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public FrameworkConfiguration setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }
}
