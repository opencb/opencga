package org.opencb.opencga.core.models.file;

import org.opencb.commons.utils.ListUtils;

import java.util.ArrayList;
import java.util.List;

public class FileLinkParams {
    private String uri;
    private String path;
    private String description;
    private List<FileLinkRelatedFileParams> relatedFiles;

    public FileLinkParams() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileLinkParams{");
        sb.append("uri='").append(uri).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", relatedFiles=").append(relatedFiles);
        sb.append('}');
        return sb.toString();
    }

    public List<File.RelatedFile> getRelatedFiles() {
        if (ListUtils.isEmpty(relatedFiles)) {
            return null;
        }
        List<File.RelatedFile> relatedFileList = new ArrayList<>(relatedFiles.size());
        for (FileLinkRelatedFileParams relatedFile : relatedFiles) {
            relatedFileList.add(new File.RelatedFile(new File().setId(relatedFile.getFile()), relatedFile.getRelation()));
        }
        return relatedFileList;
    }

    public String getUri() {
        return uri;
    }

    public FileLinkParams setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FileLinkParams setPath(String path) {
        this.path = path;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FileLinkParams setDescription(String description) {
        this.description = description;
        return this;
    }

    public FileLinkParams setRelatedFiles(List<FileLinkRelatedFileParams> relatedFiles) {
        this.relatedFiles = relatedFiles;
        return this;
    }

}
