package org.opencb.opencga.catalog.models;

import java.util.List;

public class Disease {

    private String id;
    private String name;
    private List<OntologyTerm> ontologyTerms;

    public Disease() {
    }

    public Disease(String id, String name, List<OntologyTerm> ontologyTerms) {
        this.id = id;
        this.name = name;
        this.ontologyTerms = ontologyTerms;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Disease{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", ontologyTerms=").append(ontologyTerms);
        sb.append('}');
        return sb.toString();
    }


    public String getId() {
        return id;
    }

    public Disease setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Disease setName(String name) {
        this.name = name;
        return this;
    }

    public List<OntologyTerm> getOntologyTerms() {
        return ontologyTerms;
    }

    public Disease setOntologyTerms(List<OntologyTerm> ontologyTerms) {
        this.ontologyTerms = ontologyTerms;
        return this;
    }
}
