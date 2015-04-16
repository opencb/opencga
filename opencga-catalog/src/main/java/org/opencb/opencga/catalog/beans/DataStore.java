package org.opencb.opencga.catalog.beans;

/**
 * Created by jacobo on 14/04/15.
 *
 *
 */
public class DataStore {
    private String storageEngine;
    private String dbName;

    public DataStore() {
    }

    public DataStore(String storageEngine, String dbName) {
        this.storageEngine = storageEngine;
        this.dbName = dbName;
    }

    @Override
    public String toString() {
        return "DataStore{" +
                "storageEngine='" + storageEngine + '\'' +
                ", dbName='" + dbName + '\'' +
                '}';
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }
}
