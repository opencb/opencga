package org.opencb.opencga.core.models.notes;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.List;

import static org.opencb.opencga.core.common.JacksonUtils.getUpdateObjectMapper;

public class NoteUpdateParams {

    private List<String> tags;
    private Note.Visibility visibility;
    private Object value;

    public NoteUpdateParams() {
    }

    public NoteUpdateParams(List<String> tags, Note.Visibility visibility, Object value) {
        this.tags = tags;
        this.visibility = visibility;
        this.value = value;
    }

    @JsonIgnore
    public ObjectMap getUpdateMap() throws JsonProcessingException {
        return new ObjectMap(getUpdateObjectMapper().writeValueAsString(this));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("NotesUpdateParams{");
        sb.append("tags=").append(tags);
        sb.append(", visibility=").append(visibility);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public List<String> getTags() {
        return tags;
    }

    public NoteUpdateParams setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public Note.Visibility getVisibility() {
        return visibility;
    }

    public NoteUpdateParams setVisibility(Note.Visibility visibility) {
        this.visibility = visibility;
        return this;
    }

    public Object getValue() {
        return value;
    }

    public NoteUpdateParams setValue(Object value) {
        this.value = value;
        return this;
    }
}
