package org.opencb.opencga.core.models.project;

import java.util.Objects;

public class ProjectOrganism {

    private String scientificName;
    private String commonName;
    private String assembly;

    public ProjectOrganism() {
    }

    public ProjectOrganism(String scientificName, String assembly) {
        this(scientificName, "", assembly);
    }

    public ProjectOrganism(String scientificName, String commonName, String assembly) {
        this.scientificName = scientificName != null ? scientificName : "";
        this.commonName = commonName != null ? commonName : "";
        this.assembly = assembly != null ? assembly : "";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ProjectOrganism organism = (ProjectOrganism) o;
        return Objects.equals(scientificName, organism.scientificName)
                && Objects.equals(commonName, organism.commonName)
                && Objects.equals(assembly, organism.assembly);
    }

    @Override
    public int hashCode() {
        return Objects.hash(scientificName, commonName, assembly);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Organism{");
        sb.append("scientificName='").append(scientificName).append('\'');
        sb.append(", commonName='").append(commonName).append('\'');
        sb.append(", assembly='").append(assembly).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getScientificName() {
        return scientificName;
    }

    public ProjectOrganism setScientificName(String scientificName) {
        this.scientificName = scientificName;
        return this;
    }

    public String getCommonName() {
        return commonName;
    }

    public ProjectOrganism setCommonName(String commonName) {
        this.commonName = commonName;
        return this;
    }

    public String getAssembly() {
        return assembly;
    }

    public ProjectOrganism setAssembly(String assembly) {
        this.assembly = assembly;
        return this;
    }
}
