package org.opencb.opencga.core.models.notes;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.List;

public class NoteCreateParams {

    private String id;
    private NoteType type;
    private List<String> tags;
    private Note.Visibility visibility;
    private Note.Type valueType;
    private Object value;

    public NoteCreateParams() {
    }

    public NoteCreateParams(String id, NoteType type, List<String> tags, Note.Visibility visibility, Note.Type valueType, Object value) {
        this.id = id;
        this.type = type;
        this.tags = tags;
        this.visibility = visibility;
        this.valueType = valueType;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NoteCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", type=").append(type);
        sb.append(", tags=").append(tags);
        sb.append(", visibility=").append(visibility);
        sb.append(", valueType=").append(valueType);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public Note toNote(Note.Scope scope, String userId) {
        return new Note(id, null, scope, null, type, tags != null ? tags : Collections.emptyList(), userId, visibility, 1,
                TimeUtils.getTime(), TimeUtils.getTime(), valueType, value != null ? value : Collections.emptyMap());
    }

    public String getId() {
        return id;
    }

    public NoteCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public NoteType getType() {
        return type;
    }

    public NoteCreateParams setType(NoteType type) {
        this.type = type;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public NoteCreateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public Note.Visibility getVisibility() {
        return visibility;
    }

    public NoteCreateParams setVisibility(Note.Visibility visibility) {
        this.visibility = visibility;
        return this;
    }

    public Note.Type getValueType() {
        return valueType;
    }

    public NoteCreateParams setValueType(Note.Type type) {
        this.valueType = type;
        return this;
    }

    public Object getValue() {
        return value;
    }

    public NoteCreateParams setValue(Object value) {
        this.value = value;
        return this;
    }
}
