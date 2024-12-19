package org.opencb.opencga.core.models.notes;

import org.opencb.commons.annotations.DataClass;
import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.models.PrivateStudyUid;

import java.util.List;

@DataClass(id = "Notes", since = "3.0",
        description = "Notes data model hosts information about any kind of annotation or configuration that affects the whole "
                + "Organization or Study.")
public class Note extends PrivateStudyUid {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.NOTES_ID_DESCRIPTION)
    private String id;

    @DataField(id = "uuid", indexed = true, unique = true, immutable = true, managed = true,
            description = FieldConstants.GENERIC_UUID_DESCRIPTION)
    private String uuid;

    @DataField(id = "scope", required = true, indexed = true, immutable = true, description = FieldConstants.NOTES_SCOPE_DESCRIPTION)
    private Scope scope;

    @DataField(id = "study", indexed = true, immutable = true, description = FieldConstants.NOTES_STUDY_DESCRIPTION)
    private String study;

    @DataField(id = "type", indexed = true, description = FieldConstants.NOTES_TYPE_DESCRIPTION)
    private NoteType type;

    @DataField(id = "tags", indexed = true, description = FieldConstants.NOTES_TAGS_DESCRIPTION)
    private List<String> tags;

    @DataField(id = "userId", managed = true, immutable = true, description = FieldConstants.NOTES_USER_ID_DESCRIPTION)
    private String userId;

    @DataField(id = "visibility", required = true, description = FieldConstants.NOTES_VISIBILITY_DESCRIPTION)
    private Visibility visibility;

    @DataField(id = "version", indexed = true, managed = true, immutable = true, description = FieldConstants.GENERIC_VERSION_DESCRIPTION)
    private int version;

    @DataField(id = "creationDate", indexed = true, managed = true, immutable = true,
            description = FieldConstants.GENERIC_CREATION_DATE_DESCRIPTION)
    private String creationDate;

    @DataField(id = "modificationDate", indexed = true, managed = true, immutable = true,
            description = FieldConstants.GENERIC_MODIFICATION_DATE_DESCRIPTION)
    private String modificationDate;

    @DataField(id = "valueType", required = true, description = FieldConstants.NOTES_VALUE_TYPE_DESCRIPTION)
    private Type valueType;

    @DataField(id = "value", required = true, description = FieldConstants.NOTES_VALUE_DESCRIPTION)
    private Object value;

    public enum Scope {
        ORGANIZATION,
        STUDY
    }

    public enum Visibility {
        PUBLIC,
        PRIVATE
    }

    public enum Type {
        OBJECT,
        ARRAY,
        STRING,
        INTEGER,
        DOUBLE
    }

    public Note() {
    }

    public Note(String id, String uuid, Scope scope, String study, NoteType type, List<String> tags, String userId, Visibility visibility,
                int version, String creationDate, String modificationDate, Type valueType, Object value) {
        this.id = id;
        this.uuid = uuid;
        this.scope = scope;
        this.study = study;
        this.type = type;
        this.tags = tags;
        this.userId = userId;
        this.visibility = visibility;
        this.version = version;
        this.creationDate = creationDate;
        this.modificationDate = modificationDate;
        this.valueType = valueType;
        this.value = value;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Note{");
        sb.append("id='").append(id).append('\'');
        sb.append(", uuid='").append(uuid).append('\'');
        sb.append(", scope=").append(scope);
        sb.append(", study='").append(study).append('\'');
        sb.append(", type=").append(type);
        sb.append(", tags=").append(tags);
        sb.append(", userId='").append(userId).append('\'');
        sb.append(", visibility=").append(visibility);
        sb.append(", version=").append(version);
        sb.append(", creationDate='").append(creationDate).append('\'');
        sb.append(", modificationDate='").append(modificationDate).append('\'');
        sb.append(", valueType=").append(valueType);
        sb.append(", value=").append(value);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Note setId(String id) {
        this.id = id;
        return this;
    }

    public String getUuid() {
        return uuid;
    }

    public Note setUuid(String uuid) {
        this.uuid = uuid;
        return this;
    }

    public Scope getScope() {
        return scope;
    }

    public Note setScope(Scope scope) {
        this.scope = scope;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public Note setStudy(String study) {
        this.study = study;
        return this;
    }

    public NoteType getType() {
        return type;
    }

    public Note setType(NoteType type) {
        this.type = type;
        return this;
    }

    public List<String> getTags() {
        return tags;
    }

    public Note setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public String getUserId() {
        return userId;
    }

    public Note setUserId(String userId) {
        this.userId = userId;
        return this;
    }

    public Visibility getVisibility() {
        return visibility;
    }

    public Note setVisibility(Visibility visibility) {
        this.visibility = visibility;
        return this;
    }

    public int getVersion() {
        return version;
    }

    public Note setVersion(int version) {
        this.version = version;
        return this;
    }

    public String getCreationDate() {
        return creationDate;
    }

    public Note setCreationDate(String creationDate) {
        this.creationDate = creationDate;
        return this;
    }

    public String getModificationDate() {
        return modificationDate;
    }

    public Note setModificationDate(String modificationDate) {
        this.modificationDate = modificationDate;
        return this;
    }

    public Type getValueType() {
        return valueType;
    }

    public Note setValueType(Type valueType) {
        this.valueType = valueType;
        return this;
    }

    public Object getValue() {
        return value;
    }

    public Note setValue(Object value) {
        this.value = value;
        return this;
    }
}
