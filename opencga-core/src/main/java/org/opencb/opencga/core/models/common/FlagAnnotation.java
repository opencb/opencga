package org.opencb.opencga.core.models.common;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

public class FlagAnnotation {

    @DataField(id = "id", indexed = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", indexed = true,
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "date", indexed = true,
            description = FieldConstants.FLAG_ANNOTATION_DATE_DESCRIPTION)
    private String date;

    public FlagAnnotation() {
    }

    public FlagAnnotation(String id, String description, String date) {
        this.id = id;
        this.description = description;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("FlagAnnotation{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", date=").append(date);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public FlagAnnotation setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FlagAnnotation setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getDate() {
        return date;
    }

    public FlagAnnotation setDate(String date) {
        this.date = date;
        return this;
    }
}
