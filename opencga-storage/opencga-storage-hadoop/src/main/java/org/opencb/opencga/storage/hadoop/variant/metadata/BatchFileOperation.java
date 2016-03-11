package org.opencb.opencga.storage.hadoop.variant.metadata;

import java.util.Date;
import java.util.List;
import java.util.TreeMap;

/**
 * Batch file operation information.
 *
 * Information about some operation with a set of files
 * Includes the list of processed files, the timestamp used for the HBase Puts and the current status.
 *
 *  Created on 11/03/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class BatchFileOperation {

    public enum Status {
        RUNNING,
        READY,
        ERROR
    }

    private List<Integer> fileIds;
    private long timestamp;
    private final TreeMap<Date, Status> status = new TreeMap<>(Date::compareTo);

    public BatchFileOperation() {
    }

    public BatchFileOperation(List<Integer> fileIds, long timestamp, TreeMap<Date, Status> status) {
        this.fileIds = fileIds;
        this.timestamp = timestamp;
    }

    public BatchFileOperation(List<Integer> fileIds, long timestamp) {
        this.fileIds = fileIds;
        this.timestamp = timestamp;
    }

    public Status currentStatus() {
        return status.isEmpty() ? null : status.lastEntry().getValue();
    }

    public List<Integer> getFileIds() {
        return fileIds;
    }

    public BatchFileOperation setFileIds(List<Integer> fileIds) {
        this.fileIds = fileIds;
        return this;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public BatchFileOperation setTimestamp(long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public TreeMap<Date, Status> getStatus() {
        return status;
    }

    public BatchFileOperation addStatus(Date date, Status status) {
        this.status.put(date, status);
        return this;
    }

    @Override
    public String toString() {
        return "BatchLoad{"
                + "fileIds=" + fileIds
                + ", timestamp=" + timestamp
                + ", status=" + status + '}';
    }
}
