package org.opencb.opencga.core.models.study.configuration;

public class ClinicalPriorityValue {

    private String id;
    private String description;
    /**
     * Highest priority: 1
     */
    private int rank;
    /**
     * Flag indicating the default priority that will be applied when empty.
     */
    private boolean defaultPriority;

    public ClinicalPriorityValue() {
    }

    public ClinicalPriorityValue(String id, String description, int rank, boolean defaultPriority) {
        this.id = id;
        this.description = description;
        this.rank = rank;
        this.defaultPriority = defaultPriority;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ClinicalPriorityValue{");
        sb.append("id='").append(id).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", rank=").append(rank);
        sb.append(", defaultPriority=").append(defaultPriority);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public ClinicalPriorityValue setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public ClinicalPriorityValue setDescription(String description) {
        this.description = description;
        return this;
    }

    public int getRank() {
        return rank;
    }

    public ClinicalPriorityValue setRank(int rank) {
        this.rank = rank;
        return this;
    }

    public boolean isDefaultPriority() {
        return defaultPriority;
    }

    public ClinicalPriorityValue setDefaultPriority(boolean defaultPriority) {
        this.defaultPriority = defaultPriority;
        return this;
    }
}
