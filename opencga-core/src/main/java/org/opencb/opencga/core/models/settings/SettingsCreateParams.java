package org.opencb.opencga.core.models.settings;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.List;

public class SettingsCreateParams {

    private String id;
    private List<String> tags;
    private Settings.Type type;
    private Object value;

    public SettingsCreateParams() {
    }

    public SettingsCreateParams(String id, List<String> tags, Settings.Type type, Object value) {
        this.id = id;
        this.tags = tags;
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SettingsCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", type=").append(type);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public Settings toSettings(String userId) {
        return new Settings(id, null, 1, userId, tags != null ? tags : Collections.emptyList(), TimeUtils.getTime(), TimeUtils.getTime(),
                    type, value != null ? value : Collections.emptyMap());
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

    public Settings.Type getType() {
        return type;
    }

    public SettingsCreateParams setType(Settings.Type type) {
        this.type = type;
        return this;
    }

    public Object getValue() {
        return value;
    }

    public SettingsCreateParams setValue(Object value) {
        this.value = value;
        return this;
    }
}
