package org.opencb.opencga.core.models.file;

public class FileMoveParams {

    private String path;

    public FileMoveParams() {
    }

    public String getPath() {
        return path;
    }

    public FileMoveParams setPath(String path) {
        this.path = path;
        return this;
    }
}
