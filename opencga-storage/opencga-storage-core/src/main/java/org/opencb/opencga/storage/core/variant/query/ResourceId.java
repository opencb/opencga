package org.opencb.opencga.storage.core.variant.query;

import java.util.Objects;

public class ResourceId implements QueryElement {

    private final Type type;
    private final int id;
    private final String name;

    public enum Type {
        STUDY,
        FILE,
        SAMPLE,
        COHORT
    }

    public ResourceId(Type type, int id, String name) {
        this.type = type;
        this.id = id;
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "ResourceId{"
                + "type='" + type + '\''
                + ", id=" + id
                + ", name='" + name + '\''
                + '}';
    }

    @Override
    public void toQuery(StringBuilder sb) {
        sb.append(name);
    }

    @Override
    public void describe(StringBuilder sb) {
        sb.append("'").append(name).append("' (").append(type).append(" id: ").append(id).append(")");
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ResourceId that = (ResourceId) o;
        return id == that.id && type == that.type && Objects.equals(name, that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, id, name);
    }
}
