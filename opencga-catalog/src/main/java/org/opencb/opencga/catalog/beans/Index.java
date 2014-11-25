package org.opencb.opencga.catalog.beans;

import java.util.Map;

/**
 * Created by jacobo on 17/10/14.
 */
@Deprecated
public class Index {

//    private String userId;
    private String status;
    private String dbName;
    private String storageEngine;

//    private List<Integer> output;

//    private Integer outDirId;     // outDirid
//    @Deprecated
//    private String outDirName;

//    private String tmpOutDirUri;

    private String jobId;

    private Map<String, Object> stats;
    private Map<String, Object> attributes;

    /**
     * States
     */
    public static final String INDEXED = "indexed";
    public static final String PENDING = "pending";

    public Index() {
    }

    public Index(String status, String dbName, String storageEngine, String jobId,
                 Map<String, Object> stats, Map<String, Object> attributes) {
//        this.userId = userId;
        this.status = status;
        this.dbName = dbName;
        this.storageEngine = storageEngine;
//        this.output = new LinkedList<>();
//        this.outDirId = outDirId;
//        this.outDirName = outDirName;
//        this.tmpOutDirUri = tmpOutDirName;
        this.jobId = jobId;
        this.stats = stats;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Index{" +
//                "userId='" + userId + '\'' +
                ", status='" + status + '\'' +
                ", dbName='" + dbName + '\'' +
                ", storageEngine='" + storageEngine + '\'' +
//                ", output=" + output +
//                ", outDirId=" + outDirId +
//                ", outDirName='" + outDirName + '\'' +
//                ", tmpOutDirUri='" + tmpOutDirUri + '\'' +
                ", jobId='" + jobId + '\'' +
                ", stats=" + stats +
                ", attributes=" + attributes +
                '}';
    }


    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Map<String, Object> getStats() {
        return stats;
    }

    public void setStats(Map<String, Object> stats) {
        this.stats = stats;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

}
