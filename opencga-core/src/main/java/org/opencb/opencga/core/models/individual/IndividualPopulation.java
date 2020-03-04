package org.opencb.opencga.core.models.individual;

public class IndividualPopulation {

    private String name;
    private String subpopulation;
    private String description;


    public IndividualPopulation() {
        this.name = "";
        this.subpopulation = "";
        this.description = "";
    }

    public IndividualPopulation(String name, String subpopulation, String description) {
        this.name = name;
        this.subpopulation = subpopulation;
        this.description = description;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Population{");
        sb.append("name='").append(name).append('\'');
        sb.append(", subpopulation='").append(subpopulation).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSubpopulation() {
        return subpopulation;
    }

    public void setSubpopulation(String subpopulation) {
        this.subpopulation = subpopulation;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
