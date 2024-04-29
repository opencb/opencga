package org.opencb.opencga.core.models.organizations;

import java.util.List;

public class OrganizationMigrationSummary {

    private int run;
    private int skipped;
    private int failed;
    private int pending;
    private List<String> failedMigrations;
    private List<String> pendingMigrations;
    private String version;
    private String lastAttempt; // Date

    public OrganizationMigrationSummary() {
    }

    public OrganizationMigrationSummary(int run, int skipped, int failed, int pending, List<String> failedMigrations,
                                        List<String> pendingMigrations, String lastAttempt, String version) {
        this.run = run;
        this.skipped = skipped;
        this.failed = failed;
        this.pending = pending;
        this.failedMigrations = failedMigrations;
        this.pendingMigrations = pendingMigrations;
        this.lastAttempt = lastAttempt;
        this.version = version;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("OrganizationMigrationSummary{");
        sb.append("run=").append(run);
        sb.append(", skipped=").append(skipped);
        sb.append(", failed=").append(failed);
        sb.append(", pending=").append(pending);
        sb.append(", failedMigrations=").append(failedMigrations);
        sb.append(", pendingMigrations=").append(pendingMigrations);
        sb.append(", lastAttempt='").append(lastAttempt).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public int getRun() {
        return run;
    }

    public OrganizationMigrationSummary setRun(int run) {
        this.run = run;
        return this;
    }

    public int getSkipped() {
        return skipped;
    }

    public OrganizationMigrationSummary setSkipped(int skipped) {
        this.skipped = skipped;
        return this;
    }

    public int getFailed() {
        return failed;
    }

    public OrganizationMigrationSummary setFailed(int failed) {
        this.failed = failed;
        return this;
    }

    public int getPending() {
        return pending;
    }

    public OrganizationMigrationSummary setPending(int pending) {
        this.pending = pending;
        return this;
    }

    public List<String> getFailedMigrations() {
        return failedMigrations;
    }

    public OrganizationMigrationSummary setFailedMigrations(List<String> failedMigrations) {
        this.failedMigrations = failedMigrations;
        return this;
    }

    public List<String> getPendingMigrations() {
        return pendingMigrations;
    }

    public OrganizationMigrationSummary setPendingMigrations(List<String> pendingMigrations) {
        this.pendingMigrations = pendingMigrations;
        return this;
    }

    public String getVersion() {
        return version;
    }

    public OrganizationMigrationSummary setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getLastAttempt() {
        return lastAttempt;
    }

    public OrganizationMigrationSummary setLastAttempt(String lastAttempt) {
        this.lastAttempt = lastAttempt;
        return this;
    }
}
