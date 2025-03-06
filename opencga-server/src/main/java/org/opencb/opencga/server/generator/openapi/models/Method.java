package org.opencb.opencga.server.generator.openapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Method {

    private List<String> tags;
    private String summary;
    private String description;
    private String operationId;
    private List<String> consumes;
    private List<String> produces;
    private List<Parameter> parameters;
    private Map<String,Response> responses;
    private List<Map<String, List<String>>> security;


    public Method() {
        tags= new ArrayList<>();
        consumes = new ArrayList<>();
        produces = new ArrayList<>();
        parameters = new ArrayList<>();
        responses = new HashMap<>();
    }

    public List<String> getTags() {
        return tags;
    }

    public Method setTags(List<String> tags) {
        this.tags = tags;
        return this;
    }

    public String getSummary() {
        return summary;
    }

    public Method setSummary(String summary) {
        this.summary = summary;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Method setDescription(String description) {
        this.description = description;
        return this;
    }

    public String getOperationId() {
        return operationId;
    }

    public Method setOperationId(String operationId) {
        this.operationId = operationId;
        return this;
    }

    public List<String> getConsumes() {
        return consumes;
    }

    public Method setConsumes(List<String> consumes) {
        this.consumes = consumes;
        return this;
    }

    public List<String> getProduces() {
        return produces;
    }

    public Method setProduces(List<String> produces) {
        this.produces = produces;
        return this;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public Method setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
        return this;
    }

    public Map<String,Response> getResponses() {
        return responses;
    }

    public Method setResponses(Map<String,Response> responses) {
        this.responses = responses;
        return this;
    }

    public List<Map<String, List<String>>> getSecurity() {
        return security;
    }

    public Method setSecurity(List<Map<String, List<String>>> security) {
        this.security = security;
        return this;
    }
}

