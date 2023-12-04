package org.opencb.opencga.core.models.organizations;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.migration.MigrationRun;

import java.util.ArrayList;
import java.util.List;

public class OrganizationInternal extends Internal {

    @DataField(id = "version", description = "OpenCGA version")
    private String version;

    // TODO: REname to MigrationExecution
    @DataField(id = "migrations", description = "List of migrations run")
    private List<MigrationRun> migrations;

    // TODO: REMOVE FIELD
    private List<String> organizationIds;

    public OrganizationInternal() {
    }

    public OrganizationInternal(InternalStatus status, String registrationDate, String lastModified, String version,
                                List<MigrationRun> migrations) {
        super(status, registrationDate, lastModified);
        this.version = version;
        this.migrations = migrations;
        this.organizationIds = new ArrayList<>();
    }

    public String getVersion() {
        return version;
    }

    public OrganizationInternal setVersion(String version) {
        this.version = version;
        return this;
    }

    public List<MigrationRun> getMigrations() {
        return migrations;
    }

    public OrganizationInternal setMigrations(List<MigrationRun> migrations) {
        this.migrations = migrations;
        return this;
    }

    public List<String> getOrganizationIds() {
        return organizationIds;
    }

    public OrganizationInternal setOrganizationIds(List<String> organizationIds) {
        this.organizationIds = organizationIds;
        return this;
    }
}
