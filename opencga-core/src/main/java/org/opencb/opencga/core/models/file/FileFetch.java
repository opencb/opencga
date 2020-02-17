package org.opencb.opencga.core.models.file;

public class FileFetch {

    /**
     * Folder path where the file will be downloaded.
     */
    private String path;
    /**
     * External url where the file to be registered can be downloaded from.
     */
    private String url;

    public FileFetch() {
    }

    public FileFetch(String path, String url) {
        this.path = path;
        this.url = url;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileFetch{");
        sb.append("path='").append(path).append('\'');
        sb.append(", url='").append(url).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getPath() {
        return path;
    }

    public FileFetch setPath(String path) {
        this.path = path;
        return this;
    }

    public String getUrl() {
        return url;
    }

    public FileFetch setUrl(String url) {
        this.url = url;
        return this;
    }
}
