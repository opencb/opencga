package org.opencb.opencga.core.models.organizations;

public class OrganizationSummary {

    private String id;
    private String database;
    private String status;
    private OrganizationMigrationSummary migration;

    public OrganizationSummary() {
    }

    public OrganizationSummary(String id, String database, String status, OrganizationMigrationSummary migration) {
        this.id = id;
        this.database = database;
        this.status = status;
        this.migration = migration;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OrganizationSummary{");
        sb.append("id='").append(id).append('\'');
        sb.append(", database='").append(database).append('\'');
        sb.append(", status='").append(status).append('\'');
        sb.append(", migration=").append(migration);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public OrganizationSummary setId(String id) {
        this.id = id;
        return this;
    }

    public String getDatabase() {
        return database;
    }

    public OrganizationSummary setDatabase(String database) {
        this.database = database;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public OrganizationSummary setStatus(String status) {
        this.status = status;
        return this;
    }

    public OrganizationMigrationSummary getMigration() {
        return migration;
    }

    public OrganizationSummary setMigration(OrganizationMigrationSummary migration) {
        this.migration = migration;
        return this;
    }
}
