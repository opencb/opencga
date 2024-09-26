package org.opencb.opencga.core.models.clinical;

public class ClinicalStatus extends ClinicalStatusValue {

    private String author;
    private String version;
    private String commit;
    private String date;

    public ClinicalStatus() {
    }

    public ClinicalStatus(String id, String description, ClinicalStatusType type, String author, String version, String commit,
                          String date) {
        super(id, description, type);
        this.author = author;
        this.version = version;
        this.commit = commit;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalStatus{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", type=").append(type);
        sb.append(", author='").append(author).append('\'');
        sb.append(", version='").append(version).append('\'');
        sb.append(", commit='").append(commit).append('\'');
        sb.append(", date='").append(date).append('\'');
        sb.append('}');
        return sb.toString();
    }

    @Override
    public ClinicalStatus setId(String id) {
        super.setId(id);
        return this;
    }

    public String getVersion() {
        return version;
    }

    public ClinicalStatus setVersion(String version) {
        this.version = version;
        return this;
    }

    public String getCommit() {
        return commit;
    }

    public ClinicalStatus setCommit(String commit) {
        this.commit = commit;
        return this;
    }

    public String getAuthor() {
        return author;
    }

    public ClinicalStatus setAuthor(String author) {
        this.author = author;
        return this;
    }

    public String getDate() {
        return date;
    }

    public ClinicalStatus setDate(String date) {
        this.date = date;
        return this;
    }
}
