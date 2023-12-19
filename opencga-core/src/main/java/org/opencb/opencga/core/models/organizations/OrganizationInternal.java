package org.opencb.opencga.core.models.organizations;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.models.common.Internal;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.migration.MigrationRun;

import java.util.List;

public class OrganizationInternal extends Internal {

    @DataField(id = "version", description = "OpenCGA version")
    private String version;

    @DataField(id = "migrationExecutions", description = "List of migrations executions")
    private List<MigrationRun> migrationExecutions;

    public OrganizationInternal() {
    }

    public OrganizationInternal(InternalStatus status, String registrationDate, String lastModified, String version,
                                List<MigrationRun> migrationExecutions) {
        super(status, registrationDate, lastModified);
        this.version = version;
        this.migrationExecutions = migrationExecutions;
    }

    public String getVersion() {
        return version;
    }

    public OrganizationInternal setVersion(String version) {
        this.version = version;
        return this;
    }

    public List<MigrationRun> getMigrationExecutions() {
        return migrationExecutions;
    }

    public OrganizationInternal setMigrationExecutions(List<MigrationRun> migrationExecutions) {
        this.migrationExecutions = migrationExecutions;
        return this;
    }

}
