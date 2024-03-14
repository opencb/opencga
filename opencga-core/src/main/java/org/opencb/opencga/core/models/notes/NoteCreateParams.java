package org.opencb.opencga.core.models.notes;

import org.opencb.opencga.core.common.TimeUtils;

import java.util.Collections;
import java.util.List;

public class NoteCreateParams {

    private String id;
    private Note.Scope scope;
    private String study;
    private List<String> tags;
    private Note.Visibility visibility;
    private Note.Type valueType;
    private Object value;

    public NoteCreateParams() {
    }

    public NoteCreateParams(String id, Note.Scope scope, String study, List<String> tags, Note.Visibility visibility, Note.Type valueType,
                            Object value) {
        this.id = id;
        this.scope = scope;
        this.study = study;
        this.tags = tags;
        this.visibility = visibility;
        this.valueType = valueType;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotesCreateParams{");
        sb.append("id='").append(id).append('\'');
        sb.append(", scope=").append(scope);
        sb.append(", study='").append(study).append('\'');
        sb.append(", tags=").append(tags);
        sb.append(", visibility=").append(visibility);
        sb.append(", valueType=").append(valueType);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public Note toNote(String userId) {
        return new Note(id, null, scope, study, tags != null ? tags : Collections.emptyList(), userId, visibility, 1, TimeUtils.getTime(),
                TimeUtils.getTime(), valueType, value != null ? value : Collections.emptyMap());
    }

    public String getId() {
        return id;
    }

    public NoteCreateParams setId(String id) {
        this.id = id;
        return this;
    }

    public Note.Scope getScope() {
        return scope;
    }

    public NoteCreateParams setScope(Note.Scope scope) {
        this.scope = scope;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public NoteCreateParams setStudy(String study) {
        this.study = study;
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
