package org.opencb.opencga.catalog.config;

/**
 * Created by pfurio on 01/02/17.
 */
public class Catalog {

    private long offset;
    private CatalogDBCredentials database;

    public Catalog() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Catalog{");
        sb.append("offset=").append(offset);
        sb.append(", database=").append(database);
        sb.append('}');
        return sb.toString();
    }

    public long getOffset() {
        return offset;
    }

    public Catalog setOffset(long offset) {
        this.offset = offset;
        return this;
    }

    public CatalogDBCredentials getDatabase() {
        return database;
    }

    public Catalog setDatabase(CatalogDBCredentials database) {
        this.database = database;
        return this;
    }
}
