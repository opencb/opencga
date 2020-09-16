package org.opencb.opencga.core.models.variant;

import java.util.List;
import java.util.Map;

public class CircosTrack {

    // TODO do we really need an ID?
    private String id;
    private TrackType type;
    private Map<String, String> query;
    private List<String> include;
    private String file;
    private String data;
    private int position;
    private Map<String, String> display;

    enum TrackType {
        SNV, INDEL, CNV, INSERTION, DELETION, REARRANGEMENT, RAINPLOT, GENE, COVERAGE, COVERAGE_RATIO
    }

    public CircosTrack() {
    }

    public CircosTrack(String id, TrackType type, Map<String, String> query, List<String> include, String file, int position, Map<String, String> display) {
        this.id = id;
        this.type = type;
        this.query = query;
        this.include = include;
        this.file = file;
        this.position = position;
        this.display = display;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CircosTrack{");
        sb.append("id='").append(id).append('\'');
        sb.append(", type=").append(type);
        sb.append(", query=").append(query);
        sb.append(", include=").append(include);
        sb.append(", file='").append(file).append('\'');
        sb.append(", data='").append(data).append('\'');
        sb.append(", position=").append(position);
        sb.append(", display=").append(display);
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

    public TrackType getType() {
        return type;
    }

    public CircosTrack setType(TrackType type) {
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

    public List<String> getInclude() {
        return include;
    }

    public CircosTrack setInclude(List<String> include) {
        this.include = include;
        return this;
    }

    public String getFile() {
        return file;
    }

    public CircosTrack setFile(String file) {
        this.file = file;
        return this;
    }

    public String getData() {
        return data;
    }

    public CircosTrack setData(String data) {
        this.data = data;
        return this;
    }

    public int getPosition() {
        return position;
    }

    public CircosTrack setPosition(int position) {
        this.position = position;
        return this;
    }

    public Map<String, String> getDisplay() {
        return display;
    }

    public CircosTrack setDisplay(Map<String, String> display) {
        this.display = display;
        return this;
    }
}
