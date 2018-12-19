package org.opencb.opencga.core.models;

import org.opencb.biodata.models.commons.OntologyTerm;
import org.opencb.biodata.models.commons.Phenotype;

import java.util.List;
import java.util.Map;

public class Disorder extends OntologyTerm {

    private String description;
    private List<Phenotype> evidences;

    public Disorder() {
    }

    public Disorder(String id, String name, String source, String description, List<Phenotype> evidences, Map<String, String> attributes) {
        super(id, name, source, attributes);
        this.description = description;
        this.evidences = evidences;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Disorder{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", source='").append(source).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", evidences=").append(evidences);
        sb.append('}');
        return sb.toString();
    }

    public String getDescription() {
        return description;
    }

    public Disorder setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<Phenotype> getEvidences() {
        return evidences;
    }

    public Disorder setEvidences(List<Phenotype> evidences) {
        this.evidences = evidences;
        return this;
    }
}
