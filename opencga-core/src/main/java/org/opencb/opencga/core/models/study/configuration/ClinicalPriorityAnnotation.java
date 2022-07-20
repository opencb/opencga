package org.opencb.opencga.core.models.study.configuration;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;

import org.opencb.opencga.core.api.ParamConstants;

public class ClinicalPriorityAnnotation {

    @DataField(id = "id", indexed = true,
            description = FieldConstants.GENERIC_ID_DESCRIPTION)
    private String id;

    @DataField(id = "description", indexed = true,
            description = FieldConstants.GENERIC_DESCRIPTION_DESCRIPTION)
    private String description;

    @DataField(id = "rank", indexed = true,
            description = FieldConstants.CLINICAL_PRIORITY_ANNOTATION_RANK_DESCRIPTION)
    private int rank;

    @DataField(id = "date", indexed = true,
            description = FieldConstants.CLINICAL_PRIORITY_DATE)
    private String date;

    public ClinicalPriorityAnnotation() {
    }

    public ClinicalPriorityAnnotation(String id, String description, int rank, String date) {
        this.id = id;
        this.description = description;
        this.rank = rank;
        this.date = date;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalPriorityAnnotation{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", rank=").append(rank);
        sb.append(", date=").append(date);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalPriorityAnnotation setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalPriorityAnnotation setDescription(String description) {
        this.description = description;
        return this;
    }

    public int getRank() {
        return rank;
    }

    public ClinicalPriorityAnnotation setRank(int rank) {
        this.rank = rank;
        return this;
    }

    public String getDate() {
        return date;
    }

    public ClinicalPriorityAnnotation setDate(String date) {
        this.date = date;
        return this;
    }
}
