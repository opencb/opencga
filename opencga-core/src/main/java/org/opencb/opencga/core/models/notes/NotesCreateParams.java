package org.opencb.opencga.core.models.notes;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.List;

public class NotesCreateParams {

    private String id;
    private List<String> tags;
    private Notes.Type type;
    private Object value;

    public NotesCreateParams() {
    }

    public NotesCreateParams(String id, List<String> tags, Notes.Type type, Object value) {
        this.id = id;
        this.tags = tags;
        this.type = type;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotesCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", type=").append(type);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public Notes toSettings(String userId) {
        return new Notes(id, null, 1, userId, tags != null ? tags : Collections.emptyList(), TimeUtils.getTime(), TimeUtils.getTime(),
                    type, value != null ? value : Collections.emptyMap());
    }

    public String getId() {
        return id;
    }

    public NotesCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public NotesCreateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public Notes.Type getType() {
        return type;
    }

    public NotesCreateParams setType(Notes.Type type) {
        this.type = type;
        return this;
    }

    public Object getValue() {
        return value;
    }

    public NotesCreateParams setValue(Object value) {
        this.value = value;
        return this;
    }
}
