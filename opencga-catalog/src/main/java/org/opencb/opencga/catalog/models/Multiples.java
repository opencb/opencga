package org.opencb.opencga.catalog.models;

import java.util.List;

public class Multiples {

    private String type;
    private List<String> siblings;


    public Multiples() {
    }

    public Multiples(String type, List<String> siblings) {
        this.type = type;
        this.siblings = siblings;
    }


    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Multiples{");
        sb.append("type='").append(type).append('\'');
        sb.append(", siblings=").append(siblings);
        sb.append('}');
        return sb.toString();
    }


    public String getType() {
        return type;
    }

    public Multiples setType(String type) {
        this.type = type;
        return this;
    }

    public List<String> getSiblings() {
        return siblings;
    }

    public Multiples setSiblings(List<String> siblings) {
        this.siblings = siblings;
        return this;
    }
}
