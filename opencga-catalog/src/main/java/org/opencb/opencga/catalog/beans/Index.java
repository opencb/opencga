package org.opencb.opencga.catalog.beans;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * Created by jacobo on 17/10/14.
 */
public class Index {

    private String userId;
    private String state;
    private String dbName;
    private String backend;
    private List<Integer> output;
    private int outDir;
    private String outDirName;
    private String tmpOutDirUri;
    private String jobId;

    private Map<String, Object> attributes;

    /**
     * States
     */
    public static final String INDEXED = "indexed";
    public static final String PENDING = "pending";

    public Index() {
    }

    public Index(String userId, String state, String dbName, String backend, int outDir, String outDirName, String tmpOutDirName, String jobId, Map<String, Object> attributes) {
        this.userId = userId;
        this.state = state;
        this.dbName = dbName;
        this.backend = backend;
        this.output = new LinkedList<>();
        this.outDir = outDir;
        this.outDirName = outDirName;
        this.tmpOutDirUri = tmpOutDirName;
        this.jobId = jobId;
        this.attributes = attributes;
    }

    @Override
    public String toString() {
        return "Index{" +
                "userId='" + userId + '\'' +
                ", state='" + state + '\'' +
                ", dbName='" + dbName + '\'' +
                ", backend='" + backend + '\'' +
                ", output=" + output +
                ", outDir=" + outDir +
                ", outDirName='" + outDirName + '\'' +
                ", tmpOutDirUri='" + tmpOutDirUri + '\'' +
                ", jobId='" + jobId + '\'' +
                ", attributes=" + attributes +
                '}';
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getBackend() {
        return backend;
    }

    public void setBackend(String backend) {
        this.backend = backend;
    }

    public List<Integer> getOutput() {
        return output;
    }

    public void setOutput(List<Integer> output) {
        this.output = output;
    }

    public int getOutDir() {
        return outDir;
    }

    public void setOutDir(int outDir) {
        this.outDir = outDir;
    }

    public String getOutDirName() {
        return outDirName;
    }

    public void setOutDirName(String outDirName) {
        this.outDirName = outDirName;
    }

    public String getTmpOutDirUri() {
        return tmpOutDirUri;
    }

    public void setTmpOutDirUri(String tmpOutDirUri) {
        this.tmpOutDirUri = tmpOutDirUri;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Map<String, Object> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, Object> attributes) {
        this.attributes = attributes;
    }

}
