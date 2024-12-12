package org.opencb.opencga.server.generator.models.openapi;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.HashMap;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Path {

    private Map<String, Operation> operations;

    public Path() {
        this.operations = new HashMap<String, Operation>();
    }

    public Path(Map<String, Operation> operations) {
        this.operations = operations;
    }

    // Getters y Setters
    public Map<String, Operation> getOperations() {
        return operations;
    }

    public void setOperations(Map<String, Operation> operations) {
        this.operations = operations;
    }
}
