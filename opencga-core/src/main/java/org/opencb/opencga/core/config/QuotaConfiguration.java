package org.opencb.opencga.core.config;

public class QuotaConfiguration {

    private int maxNumUsers;
    private int maxNumProjects;
    private int maxNumVariantIndexSamples;
    private int maxNumJobHours;

    public QuotaConfiguration() {
    }

    public QuotaConfiguration(int maxNumUsers, int maxNumProjects, int maxNumVariantIndexSamples, int maxNumJobHours) {
        this.maxNumUsers = maxNumUsers;
        this.maxNumProjects = maxNumProjects;
        this.maxNumVariantIndexSamples = maxNumVariantIndexSamples;
        this.maxNumJobHours = maxNumJobHours;
    }

    public static QuotaConfiguration init() {
        return new QuotaConfiguration(0, 0, 0, 0);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("QuotaConfiguration{");
        sb.append("maxNumUsers=").append(maxNumUsers);
        sb.append(", maxNumProjects=").append(maxNumProjects);
        sb.append(", maxNumVariantIndexSamples=").append(maxNumVariantIndexSamples);
        sb.append(", maxNumJobHours=").append(maxNumJobHours);
        sb.append('}');
        return sb.toString();
    }

    public int getMaxNumUsers() {
        return maxNumUsers;
    }

    public QuotaConfiguration setMaxNumUsers(int maxNumUsers) {
        this.maxNumUsers = maxNumUsers;
        return this;
    }

    public int getMaxNumProjects() {
        return maxNumProjects;
    }

    public QuotaConfiguration setMaxNumProjects(int maxNumProjects) {
        this.maxNumProjects = maxNumProjects;
        return this;
    }

    public int getMaxNumVariantIndexSamples() {
        return maxNumVariantIndexSamples;
    }

    public QuotaConfiguration setMaxNumVariantIndexSamples(int maxNumVariantIndexSamples) {
        this.maxNumVariantIndexSamples = maxNumVariantIndexSamples;
        return this;
    }

    public int getMaxNumJobHours() {
        return maxNumJobHours;
    }

    public QuotaConfiguration setMaxNumJobHours(int maxNumJobHours) {
        this.maxNumJobHours = maxNumJobHours;
        return this;
    }
}
