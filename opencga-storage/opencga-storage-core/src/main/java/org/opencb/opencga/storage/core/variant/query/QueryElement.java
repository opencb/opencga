package org.opencb.opencga.storage.core.variant.query;

public interface QueryElement {

    default String toQuery() {
        StringBuilder sb = new StringBuilder();
        toQuery(sb);
        return sb.toString();
    }

    void toQuery(StringBuilder sb);

    default String describe() {
        StringBuilder sb = new StringBuilder();
        describe(sb);
        return sb.toString();
    }

    void describe(StringBuilder sb);
}
