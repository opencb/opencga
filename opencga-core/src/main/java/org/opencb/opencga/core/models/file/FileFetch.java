package org.opencb.opencga.core.models.file;

public class FileFetch {

    /**
     * External url where the file to be registered can be downloaded from.
     */
    private String url;

    /**
     * Folder path where the file will be downloaded.
     */
    private String path;

    public FileFetch() {
    }

    public FileFetch(String url, String path) {
        this.path = path;
        this.url = url;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileFetch{");
        sb.append("url='").append(url).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }
}
