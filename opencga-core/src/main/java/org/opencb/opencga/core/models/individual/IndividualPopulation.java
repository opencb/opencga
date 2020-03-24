package org.opencb.opencga.core.models.individual;

public class IndividualPopulation {

    private String name;
    private String subpopulation;
    private String description;


    public IndividualPopulation() {
    }

    public IndividualPopulation(String name, String subpopulation, String description) {
        this.name = name;
        this.subpopulation = subpopulation;
        this.description = description;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("IndividualPopulation{");
        sb.append("name='").append(name).append('\'');
        sb.append(", subpopulation='").append(subpopulation).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public IndividualPopulation setName(String name) {
        this.name = name;
        return this;
    }

    public String getSubpopulation() {
        return subpopulation;
    }

    public IndividualPopulation setSubpopulation(String subpopulation) {
        this.subpopulation = subpopulation;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public IndividualPopulation setDescription(String description) {
        this.description = description;
        return this;
    }
}
