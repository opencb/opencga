package org.opencb.opencga.core.models.job;

import org.opencb.opencga.core.tools.ToolParams;

public class JobRunGitParams extends ToolParams {

    private String repository;
    private String reference;

    public JobRunGitParams() {
    }

    public JobRunGitParams(String repository, String reference) {
        this.repository = repository;
        this.reference = reference;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("JobRunGitParams{");
        sb.append("repository='").append(repository).append('\'');
        sb.append(", reference='").append(reference).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getRepository() {
        return repository;
    }

    public JobRunGitParams setRepository(String repository) {
        this.repository = repository;
        return this;
    }

    public String getReference() {
        return reference;
    }

    public JobRunGitParams setReference(String reference) {
        this.reference = reference;
        return this;
    }
}
