package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.tools.ToolParams;

public class JobRunParams extends ToolParams {

    private String commandLine;
    private JobRunDockerParams docker;
    private JobRunGitParams git;

    public JobRunParams() {
    }

    public JobRunParams(String commandLine, JobRunDockerParams docker, JobRunGitParams git) {
        this.commandLine = commandLine;
        this.docker = docker;
        this.git = git;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobRunParams{");
        sb.append("commandLine='").append(commandLine).append('\'');
        sb.append(", docker=").append(docker);
        sb.append(", git=").append(git);
        sb.append('}');
        return sb.toString();
    }

    public String getCommandLine() {
        return commandLine;
    }

    public JobRunParams setCommandLine(String commandLine) {
        this.commandLine = commandLine;
        return this;
    }

    public JobRunDockerParams getDocker() {
        return docker;
    }

    public JobRunParams setDocker(JobRunDockerParams docker) {
        this.docker = docker;
        return this;
    }

    public JobRunGitParams getGit() {
        return git;
    }

    public JobRunParams setGit(JobRunGitParams git) {
        this.git = git;
        return this;
    }
}
