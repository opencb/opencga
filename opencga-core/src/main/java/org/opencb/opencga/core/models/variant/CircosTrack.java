package org.opencb.opencga.core.models.variant;

import java.util.List;
import java.util.Map;

public class CircosTrack {
    private String id;
    private String type;
    private List<Map<String, String>> filters;

    public CircosTrack() {
    }

    public CircosTrack(String id, String type, List<Map<String, String>> filters) {
        this.id = id;
        this.type = type;
        this.filters = filters;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CircosTrack{");
        sb.append("id='").append(id).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", filters=").append(filters);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public CircosTrack setId(String id) {
        this.id = id;
        return this;
    }

    public String getType() {
        return type;
    }

    public CircosTrack setType(String type) {
        this.type = type;
        return this;
    }

    public List<Map<String, String>> getFilters() {
        return filters;
    }

    public CircosTrack setFilters(List<Map<String, String>> filters) {
        this.filters = filters;
        return this;
    }
}
