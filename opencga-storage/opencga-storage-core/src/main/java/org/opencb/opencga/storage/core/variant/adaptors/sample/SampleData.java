package org.opencb.opencga.storage.core.variant.adaptors.sample;

import java.util.Map;

/**
 * Created by jacobo on 27/03/19.
 */
public class SampleData {
    private String id;
    private Map<String, String> sampleData;
    private String fileId;

    public SampleData() {
    }

    public SampleData(String id, Map<String, String> sampleData, String fileId) {
        this.id = id;
        this.sampleData = sampleData;
        this.fileId = fileId;
    }

    public String getId() {
        return id;
    }

    public SampleData setId(String id) {
        this.id = id;
        return this;
    }

    public Map<String, String> getSampleData() {
        return sampleData;
    }

    public SampleData setSampleData(Map<String, String> sampleData) {
        this.sampleData = sampleData;
        return this;
    }

    public String getFileId() {
        return fileId;
    }

    public SampleData setFileId(String fileId) {
        this.fileId = fileId;
        return this;
    }
}
