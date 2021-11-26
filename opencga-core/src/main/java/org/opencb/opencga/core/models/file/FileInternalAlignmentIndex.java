package org.opencb.opencga.core.models.file;

import org.opencb.opencga.core.models.common.InternalStatus;

public class FileInternalAlignmentIndex {

    private InternalStatus status;
    private String fileId;
    private String indexer;

    public FileInternalAlignmentIndex() {
    }

    public FileInternalAlignmentIndex(InternalStatus status, String fileId, String indexer) {
        this.status = status;
        this.fileId = fileId;
        this.indexer = indexer;
    }

    public static FileInternalAlignmentIndex init() {
        return new FileInternalAlignmentIndex(null, "", "");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternalAlignmentIndex{");
        sb.append("status=").append(status);
        sb.append(", fileId='").append(fileId).append('\'');
        sb.append(", indexer='").append(indexer).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public InternalStatus getStatus() {
        return status;
    }

    public FileInternalAlignmentIndex setStatus(InternalStatus status) {
        this.status = status;
        return this;
    }

    public String getFileId() {
        return fileId;
    }

    public FileInternalAlignmentIndex setFileId(String fileId) {
        this.fileId = fileId;
        return this;
    }

    public String getIndexer() {
        return indexer;
    }

    public FileInternalAlignmentIndex setIndexer(String indexer) {
        this.indexer = indexer;
        return this;
    }
}
