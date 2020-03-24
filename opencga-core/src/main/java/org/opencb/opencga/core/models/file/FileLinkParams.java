package org.opencb.opencga.core.models.file;

import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.core.models.common.CustomStatusParams;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class FileLinkParams {
    private String uri;
    private String path;
    private String description;
    private List<SmallRelatedFileParams> relatedFiles;
    private CustomStatusParams status;
    private FileLinkInternalParams internal;

    public FileLinkParams() {
    }

    public FileLinkParams(String uri, String path, String description, List<SmallRelatedFileParams> relatedFiles,
                          CustomStatusParams status, FileLinkInternalParams internal) {
        this.uri = uri;
        this.path = path;
        this.description = description;
        this.relatedFiles = relatedFiles;
        this.status = status;
        this.internal = internal;
    }

    public static FileLinkParams of(File file) {
        return new FileLinkParams(file.getUri().toString(), file.getPath(), file.getDescription(),
                file.getRelatedFiles() != null
                        ? file.getRelatedFiles().stream().map(SmallRelatedFileParams::of).collect(Collectors.toList())
                        : Collections.emptyList(), CustomStatusParams.of(file.getStatus()),
                new FileLinkInternalParams(file.getInternal().getSampleMap()));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileLinkParams{");
        sb.append("uri='").append(uri).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", relatedFiles=").append(relatedFiles);
        sb.append(", status=").append(status);
        sb.append(", internal=").append(internal);
        sb.append('}');
        return sb.toString();
    }

    public List<FileRelatedFile> getRelatedFiles() {
        if (ListUtils.isEmpty(relatedFiles)) {
            return null;
        }
        List<FileRelatedFile> relatedFileList = new ArrayList<>(relatedFiles.size());
        for (SmallRelatedFileParams relatedFile : relatedFiles) {
            relatedFileList.add(new FileRelatedFile(new File().setId(relatedFile.getFile()), relatedFile.getRelation()));
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

    public FileLinkParams setRelatedFiles(List<SmallRelatedFileParams> relatedFiles) {
        this.relatedFiles = relatedFiles;
        return this;
    }

    public CustomStatusParams getStatus() {
        return status;
    }

    public FileLinkParams setStatus(CustomStatusParams status) {
        this.status = status;
        return this;
    }

    public FileLinkInternalParams getInternal() {
        return internal;
    }

    public FileLinkParams setInternal(FileLinkInternalParams internal) {
        this.internal = internal;
        return this;
    }

}
