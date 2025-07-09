package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.InternalStatus;

import java.util.Objects;

public class FileInternalCoverageIndex {

    private InternalStatus status;
    private String fileId;
    private String indexer;
    private int windowSize;

    public FileInternalCoverageIndex() {
    }

    public FileInternalCoverageIndex(InternalStatus status, String fileId, String indexer, int windowSize) {
        this.status = status;
        this.fileId = fileId;
        this.indexer = indexer;
        this.windowSize = windowSize;
    }

    public static FileInternalCoverageIndex init() {
        return new FileInternalCoverageIndex(null, "", "", 0);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternalCoverageIndex{");
        sb.append("status=").append(status);
        sb.append(", fileId='").append(fileId).append('\'');
        sb.append(", indexer='").append(indexer).append('\'');
        sb.append(", windowSize=").append(windowSize);
        sb.append('}');
        return sb.toString();
    }

    public InternalStatus getStatus() {
        return status;
    }

    public FileInternalCoverageIndex setStatus(InternalStatus status) {
        this.status = status;
        return this;
    }

    public String getFileId() {
        return fileId;
    }

    public FileInternalCoverageIndex setFileId(String fileId) {
        this.fileId = fileId;
        return this;
    }

    public String getIndexer() {
        return indexer;
    }

    public FileInternalCoverageIndex setIndexer(String indexer) {
        this.indexer = indexer;
        return this;
    }

    public int getWindowSize() {
        return windowSize;
    }

    public FileInternalCoverageIndex setWindowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FileInternalCoverageIndex that = (FileInternalCoverageIndex) o;
        return Objects.equals(status, that.status) &&
                Objects.equals(fileId, that.fileId) &&
                Objects.equals(indexer, that.indexer) &&
                Objects.equals(windowSize, that.windowSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(status, fileId, indexer, windowSize);
    }
}
