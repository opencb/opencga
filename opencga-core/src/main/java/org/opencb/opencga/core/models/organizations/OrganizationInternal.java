package org.opencb.opencga.core.models.organizations;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.migration.MigrationRun;

import java.util.List;

public class OrganizationInternal extends Internal {

    @DataField(id = "version", description = "OpenCGA version")
    private String version;

    @DataField(id = "migrations", description = "List of migrations run")
    private List<MigrationRun> migrations;

    public OrganizationInternal() {
    }

    public OrganizationInternal(InternalStatus status, String registrationDate, String lastModified, String version,
                                List<MigrationRun> migrations) {
        super(status, registrationDate, lastModified);
        this.version = version;
        this.migrations = migrations;
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
}
