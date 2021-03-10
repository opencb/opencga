package org.opencb.opencga.storage.core.variant.query;

import org.opencb.opencga.storage.core.metadata.models.StudyResourceMetadata;

import java.util.Collection;
import java.util.stream.Collectors;

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


    static <T> String objectToString(T v) {
        if (v instanceof StudyResourceMetadata<?>) {
            return ((StudyResourceMetadata<?>) v).getName();
        } else if (v instanceof Collection) {
            return ((Collection<?>) v).stream().map(QueryElement::objectToString).collect(Collectors.joining(","));
        } else {
            return v.toString();
        }
    }

    static <T> String objectToDescriptionString(T v) {
        if (v instanceof StudyResourceMetadata<?>) {
            return ((StudyResourceMetadata<?>) v).getName();
        } else {
            return v.toString();
        }
    }
}
