package org.opencb.opencga.core.config.storage;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.config.DatabaseCredentials;

public class StorageEngineConfiguration {

    /**
     * Engine ID.
     */
    private String id;
    /**
     * Engine class.
     */
    private String engine;
    /**
     * Options parameter defines database-specific parameters.
     */
    private ObjectMap options;
    /**
     * Database credentials.
     */
    private DatabaseCredentials database;

    public StorageEngineConfiguration() {
        options = new ObjectMap();
        database = new DatabaseCredentials();
    }

    public String getId() {
        return id;
    }

    public StorageEngineConfiguration setId(String id) {
        this.id = id;
        return this;
    }

    public String getEngine() {
        return engine;
    }

    public StorageEngineConfiguration setEngine(String engine) {
        this.engine = engine;
        return this;
    }

    public ObjectMap getOptions() {
        return options;
    }

    public StorageEngineConfiguration setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }

    public DatabaseCredentials getDatabase() {
        return database;
    }

    public StorageEngineConfiguration setDatabase(DatabaseCredentials database) {
        this.database = database;
        return this;
    }
}
