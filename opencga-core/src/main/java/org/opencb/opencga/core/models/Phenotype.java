package org.opencb.opencga.core.models;

public class Phenotype {

    private String id;
    private String name;
    private String source;
    private Status status;

    public enum Status {
        OBSERVED,
        NOT_OBSERVED,
        UNKNOWN
    }

    public Phenotype() {
    }

    public Phenotype(String id, String name, String source) {
        this(id, name, source, Status.UNKNOWN);
    }

    public Phenotype(String id, String name, String source, Status status) {
        this.id = id;
        this.name = name;
        this.source = source;
        this.status = status;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Phenotype{");
        sb.append("id='").append(id).append('\'');
        sb.append(", name='").append(name).append('\'');
        sb.append(", source='").append(source).append('\'');
        sb.append(", status=").append(status);
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public Phenotype setId(String id) {
        this.id = id;
        return this;
    }

    public String getName() {
        return name;
    }

    public Phenotype setName(String name) {
        this.name = name;
        return this;
    }

    public String getSource() {
        return source;
    }

    public Phenotype setSource(String source) {
        this.source = source;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public Phenotype setStatus(Status status) {
        this.status = status;
        return this;
    }
}
