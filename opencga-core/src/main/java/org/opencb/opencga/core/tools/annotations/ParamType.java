package org.opencb.opencga.core.tools.annotations;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ParamType {
    QUERY,
    BODY,
    PATH;

    // Use this to serialize in lowercase
    @JsonValue
    protected String lowercase() {
        return name().toLowerCase();
    }
}
