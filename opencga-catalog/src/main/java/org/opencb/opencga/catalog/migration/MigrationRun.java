package org.opencb.opencga.catalog.migration;

import java.util.Date;

public class MigrationRun {

    /**
     * Migration id.
     */
    private String id;

    /**
     * Field describing the migration.
     */
    private String description;

    /**
     * OpenCGA version where it has to be applied.
     */
    private String version;

    /**
     * Date when migration started.
     */
    private Date start;

    /**
     * Date when migration finished.
     */
    private Date end;

    /**
     * Migration patch.
     */
    private int patch;

    /**
     * Migration status.
     */
    private MigrationStatus status;

    /**
     * Exception message raised if the migration fails.
     */
    private String exception;

    public enum MigrationStatus {
        DONE,
        ERROR,
        REDUNDANT
    }

    public MigrationRun() {
    }

    public MigrationRun(String id, String description, String version, Date start, int patch) {
        this.id = id;
        this.description = description;
        this.version = version;
        this.start = start;
        this.patch = patch;
    }

    public MigrationRun(String id, String description, String version, Date start, Date end, int patch, MigrationStatus status,
                        String exception) {
        this.id = id;
        this.description = description;
        this.version = version;
        this.start = start;
        this.end = end;
        this.patch = patch;
        this.status = status;
        this.exception = exception;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MigrationRun{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", start=").append(start);
        sb.append(", end=").append(end);
        sb.append(", patch=").append(patch);
        sb.append(", status=").append(status);
        sb.append(", exception='").append(exception).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public MigrationRun setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public MigrationRun setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public MigrationRun setVersion(String version) {
        this.version = version;
        return this;
    }

    public Date getStart() {
        return start;
    }

    public MigrationRun setStart(Date start) {
        this.start = start;
        return this;
    }

    public Date getEnd() {
        return end;
    }

    public MigrationRun setEnd(Date end) {
        this.end = end;
        return this;
    }

    public int getPatch() {
        return patch;
    }

    public MigrationRun setPatch(int patch) {
        this.patch = patch;
        return this;
    }

    public MigrationStatus getStatus() {
        return status;
    }

    public MigrationRun setStatus(MigrationStatus status) {
        this.status = status;
        return this;
    }

    public String getException() {
        return exception;
    }

    public MigrationRun setException(String exception) {
        this.exception = exception;
        return this;
    }
}
