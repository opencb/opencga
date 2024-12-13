package org.opencb.opencga.server.generator.models.openapi;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Path {

    private Map<String,Method> method; // GET, POST, PUT, DELETE

    public Path() {
    }

    public Map<String, Method> getMethod() {
        return method;
    }

    public Path setMethod(Map<String, Method> method) {
        this.method = method;
        return this;
    }
}
