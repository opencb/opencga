package org.opencb.opencga.core.models.file;

import java.util.Map;

public class FileLinkInternalParams {

    private Map<String, String> sampleMap;

    public FileLinkInternalParams() {
    }

    public FileLinkInternalParams(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FileLinkInternalParams{");
        sb.append("sampleMap=").append(sampleMap);
        sb.append('}');
        return sb.toString();
    }

    public Map<String, String> getSampleMap() {
        return sampleMap;
    }

    public FileLinkInternalParams setSampleMap(Map<String, String> sampleMap) {
        this.sampleMap = sampleMap;
        return this;
    }
}
