package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.AclParams;

public class FileAclUpdateParams extends AclParams {

    private String file;
    private String sample;

    public FileAclUpdateParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileAclUpdateParams{");
        sb.append("file='").append(file).append('\'');
        sb.append(", sample='").append(sample).append('\'');
        sb.append(", permissions='").append(permissions).append('\'');
        sb.append(", action=").append(action);
        sb.append('}');
        return sb.toString();
    }

    public String getFile() {
        return file;
    }

    public FileAclUpdateParams setFile(String file) {
        this.file = file;
        return this;
    }

    public String getSample() {
        return sample;
    }

    public FileAclUpdateParams setSample(String sample) {
        this.sample = sample;
        return this;
    }
}
