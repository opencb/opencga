package org.opencb.opencga.core.models.file;

import java.util.List;

public class FileUriChangeRestParam {

    private List<FileUriChangeParam> uri;

    public FileUriChangeRestParam() {
    }

    public FileUriChangeRestParam(List<FileUriChangeParam> uri) {
        this.uri = uri;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileUriChangeRestParam{");
        sb.append("uri=").append(uri);
        sb.append('}');
        return sb.toString();
    }

    public List<FileUriChangeParam> getUri() {
        return uri;
    }

    public FileUriChangeRestParam setUri(List<FileUriChangeParam> uri) {
        this.uri = uri;
        return this;
    }
}
