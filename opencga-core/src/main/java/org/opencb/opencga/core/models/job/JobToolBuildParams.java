package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.tools.ToolParams;

public class JobToolBuildParams extends ToolParams {

    private String gitRepository;
    private String aptGet;
    private Boolean installR;
    private JobToolBuildDockerParams docker;

    public JobToolBuildParams() {
    }

    public JobToolBuildParams(String gitRepository, String aptGet, Boolean installR, JobToolBuildDockerParams docker) {
        this.gitRepository = gitRepository;
        this.aptGet = aptGet;
        this.installR = installR;
        this.docker = docker;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobToolBuildParams{");
        sb.append("gitRepository='").append(gitRepository).append('\'');
        sb.append(", aptGet='").append(aptGet).append('\'');
        sb.append(", installR=").append(installR);
        sb.append(", docker=").append(docker);
        sb.append('}');
        return sb.toString();
    }

    public String getGitRepository() {
        return gitRepository;
    }

    public JobToolBuildParams setGitRepository(String gitRepository) {
        this.gitRepository = gitRepository;
        return this;
    }

    public String getAptGet() {
        return aptGet;
    }

    public JobToolBuildParams setAptGet(String aptGet) {
        this.aptGet = aptGet;
        return this;
    }

    public Boolean getInstallR() {
        return installR;
    }

    public JobToolBuildParams setInstallR(Boolean installR) {
        this.installR = installR;
        return this;
    }

    public JobToolBuildDockerParams getDocker() {
        return docker;
    }

    public JobToolBuildParams setDocker(JobToolBuildDockerParams docker) {
        this.docker = docker;
        return this;
    }
}
