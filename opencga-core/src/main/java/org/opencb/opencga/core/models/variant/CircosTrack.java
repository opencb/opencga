package org.opencb.opencga.core.models.variant;

import java.util.Map;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class CircosTrack {
    @DataField(description = ParamConstants.CIRCOS_TRACK_ID_DESCRIPTION)
    private String id;
    @DataField(description = ParamConstants.CIRCOS_TRACK_TYPE_DESCRIPTION)
    private String type;
    @DataField(description = ParamConstants.CIRCOS_TRACK_QUERY_DESCRIPTION)
    private Map<String, String> query;

    public CircosTrack() {
    }

    public CircosTrack(String id, String type, Map<String, String> query) {
        this.id = id;
        this.type = type;
        this.query = query;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CircosTrack{");
        sb.append("id='").append(id).append('\'');
        sb.append(", type='").append(type).append('\'');
        sb.append(", query=").append(query);
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

    public Map<String, String> getQuery() {
        return query;
    }

    public CircosTrack setQuery(Map<String, String> query) {
        this.query = query;
        return this;
    }
}
