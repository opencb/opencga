package org.opencb.opencga.core.config;

public class QuotaConfiguration {

    private int maxNumUsers;
    private int maxNumProjects;
    private int maxNumIndexedSamples;
    private int maxNumJobHours;

    public QuotaConfiguration() {
    }

    public QuotaConfiguration(int maxNumUsers, int maxNumProjects, int maxNumIndexedSamples, int maxNumJobHours) {
        this.maxNumUsers = maxNumUsers;
        this.maxNumProjects = maxNumProjects;
        this.maxNumIndexedSamples = maxNumIndexedSamples;
        this.maxNumJobHours = maxNumJobHours;
    }

    public static QuotaConfiguration init() {
        return new QuotaConfiguration(15, 5, 15000, 100);
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

    public int getMaxNumIndexedSamples() {
        return maxNumIndexedSamples;
    }

    public QuotaConfiguration setMaxNumIndexedSamples(int maxNumIndexedSamples) {
        this.maxNumIndexedSamples = maxNumIndexedSamples;
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
