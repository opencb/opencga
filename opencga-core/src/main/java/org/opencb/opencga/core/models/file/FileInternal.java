package org.opencb.opencga.core.models.file;

import java.util.Map;

public class FileInternal {

    private Map<String, String> sampleMap;

    public FileInternal() {
    }

    public FileInternal(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileInternal{");
        sb.append("sampleMap=").append(sampleMap);
        sb.append('}');
        return sb.toString();
    }

    public Map<String, String> getSampleMap() {
        return sampleMap;
    }

    public FileInternal setSampleMap(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
        return this;
    }
}
