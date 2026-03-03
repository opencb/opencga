package org.opencb.opencga.core.models.study.configuration;

import org.opencb.commons.annotations.DataField;

public class ClinicalTierConfiguration {

    @DataField(id = "id", description = "Unique identifier for the clinical tier")
    private String id;

    @DataField(id = "description", description = "Description of the clinical tier")
    private String description;

    @DataField(id = "rank", description = "Rank of the clinical tier, where lower values indicate higher priority")
    private int rank;

    public ClinicalTierConfiguration() {
    }

    public ClinicalTierConfiguration(String id, String description, int rank) {
        this.id = id;
        this.description = description;
        this.rank = rank;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalTierConfiguration{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", rank=").append(rank);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalTierConfiguration setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalTierConfiguration setDescription(String description) {
        this.description = description;
        return this;
    }

    public int getRank() {
        return rank;
    }

    public ClinicalTierConfiguration setRank(int rank) {
        this.rank = rank;
        return this;
    }
}
