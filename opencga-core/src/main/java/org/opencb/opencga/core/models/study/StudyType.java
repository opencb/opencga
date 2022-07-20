package org.opencb.opencga.core.models.study;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import org.opencb.opencga.core.api.ParamConstants;

public class StudyType {

    @DataField(id = "id", required = true, indexed = true, unique = true, immutable = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", defaultValue = "No description available",
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    public StudyType() {
    }

    public StudyType(String id, String description) {
        this.id = id;
        this.description = description;
    }

    public static StudyType init() {
        return new StudyType("", "");
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StudyType{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public StudyType setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public StudyType setDescription(String description) {
        this.description = description;
        return this;
    }
}
