package org.opencb.opencga.core.models.study.configuration;

import java.util.Date;

public class ClinicalPriorityAnnotation {

    private String id;
    private String description;
    private int rank;
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
