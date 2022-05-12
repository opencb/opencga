package org.opencb.opencga.test.config;

public class Reference {


    private String path;
    private String index;

    public Reference() {
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Reference{\n");
        sb.append("path='").append(path).append('\'').append("\n");
        sb.append("index='").append(index).append('\'').append("\n");
        sb.append("}\n");
        return sb.toString();
    }


    public String getPath() {
        return path;
    }

    public Reference setPath(String path) {
        this.path = path;
        return this;
    }

    public String getIndex() {
        return index;
    }

    public Reference setIndex(String index) {
        this.index = index;
        return this;
    }
}
