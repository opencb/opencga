package org.opencb.opencga.catalog.migration;

import java.util.Date;

public class MigrationRun {

    private String id;

    private Date date;

    private int patch;

    private MigrationStatus status;

    enum MigrationStatus {
        DONE,
//        PENDING,
//        RUNNING,
        ERROR
    }

    public MigrationRun() {
    }

    public MigrationRun(Date date, String id, int patch, MigrationStatus status) {
        this.date = date;
        this.id = id;
        this.patch = patch;
        this.status = status;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MigrationRun{");
        sb.append("date=").append(date);
        sb.append(", id='").append(id).append('\'');
        sb.append(", patch=").append(patch);
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public Date getDate() {
        return date;
    }

    public MigrationRun setDate(Date date) {
        this.date = date;
        return this;
    }

    public String id() {
        return id;
    }

    public MigrationRun setId(String id) {
        this.id = id;
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
}
