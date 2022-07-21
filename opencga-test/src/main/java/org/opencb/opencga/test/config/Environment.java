package org.opencb.opencga.test.config;

import java.util.List;

public class Environment {


    private String id;
    private String description;
    private Data data;
    private Dataset dataset;
    private Reference reference;
    private Aligner aligner;
    private List<Caller> callers;

    public Environment() {
    }

    @Override
    public String toString() {
        final StringBuffer sb = new StringBuffer("Env{\n");
        sb.append("id='").append(id).append('\'').append("\n");
        sb.append("description='").append(description).append('\'').append("\n");
        sb.append("dataset=").append(dataset).append("\n");
        sb.append("reference=").append(reference).append("\n");
        sb.append("aligner=").append(aligner).append("\n");
        sb.append("callers=").append(callers).append("\n");
        sb.append("}\n");
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Environment setId(String id) {
        this.id = id;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Environment setDescription(String description) {
        this.description = description;
        return this;
    }

    public Dataset getDataset() {
        return dataset;
    }

    public Environment setDatasets(Dataset dataset) {
        this.dataset = dataset;
        return this;
    }

    public Reference getReference() {
        return reference;
    }

    public Environment setReference(Reference reference) {
        this.reference = reference;
        return this;
    }

    public Aligner getAligner() {
        return aligner;
    }

    public Environment setAligner(Aligner aligner) {
        this.aligner = aligner;
        return this;
    }

    public List<Caller> getCallers() {
        return callers;
    }

    public Environment setCallers(List<Caller> callers) {
        this.callers = callers;
        return this;
    }

    public Data getData() {
        return data;
    }

    public Environment setData(Data data) {
        this.data = data;
        return this;
    }

    public Environment setDataset(Dataset dataset) {
        this.dataset = dataset;
        return this;
    }
}
