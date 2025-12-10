package org.opencb.opencga.core.models.file;

public class FileUriChangeParam {

    private String original;
    private String updated;

    public FileUriChangeParam() {
    }

    public FileUriChangeParam(String original, String updated) {
        this.original = original;
        this.updated = updated;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileUriChangeParam{");
        sb.append("original='").append(original).append('\'');
        sb.append(", updated='").append(updated).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getOriginal() {
        return original;
    }

    public FileUriChangeParam setOriginal(String original) {
        this.original = original;
        return this;
    }

    public String getUpdated() {
        return updated;
    }

    public FileUriChangeParam setUpdated(String updated) {
        this.updated = updated;
        return this;
    }
}
