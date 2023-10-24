package org.opencb.opencga.core.models.settings;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SettingsCreateParams {

    private String id;
    private List<String> tags;
    private Map<String, Object> value;

    public SettingsCreateParams() {
    }

    public SettingsCreateParams(String id, List<String> tags, Map<String, Object> value) {
        this.id = id;
        this.tags = tags;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SettingsCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public Settings toSettings(String userId) {
        return new Settings(id, 1, userId, tags != null ? tags : Collections.emptyList(), TimeUtils.getTime(), TimeUtils.getTime(),
                    value != null ? value : Collections.emptyMap());
    }

    public String getId() {
        return id;
    }

    public SettingsCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public SettingsCreateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public Map<String, Object> getValue() {
        return value;
    }

    public SettingsCreateParams setValue(Map<String, Object> value) {
        this.value = value;
        return this;
    }
}
