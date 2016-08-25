package org.opencb.opencga.storage.core.metadata;

import java.util.Calendar;
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
        DONE,       // Finished, but some work still needed (optional)
        READY,
        ERROR
    }

    private String operationName;
    private List<Integer> fileIds;
    private long timestamp;
    private final TreeMap<Date, Status> status = new TreeMap<>(Date::compareTo);

    public BatchFileOperation() {
    }

    public BatchFileOperation(List<Integer> fileIds, long timestamp, TreeMap<Date, Status> status, String operationName) {
        this.fileIds = fileIds;
        this.timestamp = timestamp;
        this.operationName = operationName;
    }

    public BatchFileOperation(String operationName, List<Integer> fileIds, long timestamp) {
        this.operationName = operationName;
        this.fileIds = fileIds;
        this.timestamp = timestamp;
    }

    public Status currentStatus() {
        return status.isEmpty() ? null : status.lastEntry().getValue();
    }

    public String getOperationName() {
        return operationName;
    }

    public BatchFileOperation setOperationName(String operationName) {
        this.operationName = operationName;
        return this;
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

    public BatchFileOperation addStatus(Status status) {
        return addStatus(Calendar.getInstance().getTime(), status);
    }

    public BatchFileOperation addStatus(Date date, Status status) {
        this.status.put(date, status);
        return this;
    }

    @Override
    public String toString() {
        return "BatchFileOperation{"
                + "operationName='" + operationName + '\''
                + ", fileIds=" + fileIds
                + ", timestamp=" + timestamp
                + ", status=" + status
                + '}';
    }
}
