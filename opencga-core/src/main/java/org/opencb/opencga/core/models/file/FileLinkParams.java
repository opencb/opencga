package org.opencb.opencga.core.models.file;

import org.opencb.commons.utils.ListUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class FileLinkParams {
    private String uri;
    private String path;
    private String description;
    private List<SmallRelatedFileParams> relatedFiles;
    private Map<String, String> sampleMap;

    public FileLinkParams() {
    }

    public FileLinkParams(String uri, String path, String description, List<SmallRelatedFileParams> relatedFiles,
                          Map<String, String> sampleMap) {
        this.uri = uri;
        this.path = path;
        this.description = description;
        this.relatedFiles = relatedFiles;
        this.sampleMap = sampleMap;
    }

    public static FileLinkParams of(File file) {
        return new FileLinkParams(file.getUri().toString(), file.getPath(), file.getDescription(),
                file.getRelatedFiles() != null
                        ? file.getRelatedFiles().stream().map(SmallRelatedFileParams::of).collect(Collectors.toList())
                        : Collections.emptyList(),
                file.getInternal().getSampleMap());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileLinkParams{");
        sb.append("uri='").append(uri).append('\'');
        sb.append(", path='").append(path).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", relatedFiles=").append(relatedFiles);
        sb.append(", sampleMap=").append(sampleMap);
        sb.append('}');
        return sb.toString();
    }

    public List<File.RelatedFile> getRelatedFiles() {
        if (ListUtils.isEmpty(relatedFiles)) {
            return null;
        }
        List<File.RelatedFile> relatedFileList = new ArrayList<>(relatedFiles.size());
        for (SmallRelatedFileParams relatedFile : relatedFiles) {
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

    public FileLinkParams setRelatedFiles(List<SmallRelatedFileParams> relatedFiles) {
        this.relatedFiles = relatedFiles;
        return this;
    }

    public Map<String, String> getSampleMap() {
        return sampleMap;
    }

    public FileLinkParams setSampleMap(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
        return this;
    }
}
