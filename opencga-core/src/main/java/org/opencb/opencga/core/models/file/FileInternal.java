package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.Status;

import java.util.HashMap;
import java.util.Map;

public class FileInternal {

    private FileStatus status;
    private FileIndex index;
    private Map<String, String> sampleMap;

    public FileInternal() {
    }

    public FileInternal(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
    }

    public FileInternal(FileStatus status, FileIndex index, Map<String, String> sampleMap) {
        this.status = status;
        this.index = index;
        this.sampleMap = sampleMap;
    }

    public static FileInternal initialize() {
        return new FileInternal(new FileStatus(FileStatus.READY), FileIndex.initialize(), new HashMap<>());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternal{");
        sb.append("status=").append(status);
        sb.append(", index=").append(index);
        sb.append(", sampleMap=").append(sampleMap);
        sb.append('}');
        return sb.toString();
    }

    public FileStatus getStatus() {
        return status;
    }

    public FileInternal setStatus(FileStatus status) {
        this.status = status;
        return this;
    }

    public FileIndex getIndex() {
        return index;
    }

    public FileInternal setIndex(FileIndex index) {
        this.index = index;
        return this;
    }

    public Map<String, String> getSampleMap() {
        return sampleMap;
    }

    public FileInternal setSampleMap(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
        return this;
    }
}
