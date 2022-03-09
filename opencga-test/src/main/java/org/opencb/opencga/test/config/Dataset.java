package org.opencb.opencga.test.config;

public class Dataset {

    private String path;

    public Dataset() {
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Dataset{\n");
        sb.append("path='").append(path).append('\'').append("\n");
        sb.append("}\n");
        return sb.toString();
    }

    public String getPath() {
        return path;
    }

    public Dataset setPath(String path) {
        this.path = path;
        return this;
    }
}
