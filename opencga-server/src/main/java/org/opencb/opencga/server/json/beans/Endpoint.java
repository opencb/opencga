package org.opencb.opencga.server.json.beans;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.opencga.server.json.config.CategoryConfig;
import org.opencb.opencga.server.json.utils.CommandLineUtils;

import java.util.List;

public class Endpoint {

    private String path;
    private String method;
    private String response;
    private String responseClass;
    private String notes;
    private String description;
    private List<Parameter> parameters;

    public boolean hasPrimitiveBodyParams(CategoryConfig config) {

        for (Parameter parameter : getParameters()) {
            if (parameter.getData() != null && !parameter.getData().isEmpty()) {
                for (Parameter bodyParam : parameter.getData()) {
                    if ((config.isAvailableSubCommand(bodyParam.getName()) && !bodyParam.isComplex()) || (bodyParam.isStringList())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean hasQueryParams() {
        for (Parameter parameter : getParameters()) {
            if ("query".equals(parameter.getParam()) && !parameter.isRequired() && (!parameter.isComplex() || parameter.isStringList())) {
                return true;
            }
        }
        return false;
    }

    public String getBodyParamsObject() {
        for (Parameter parameter : getParameters()) {
            if (parameter.getData() != null) {
                return parameter.getTypeClass().substring(parameter.getTypeClass().lastIndexOf('.') + 1).replaceAll(";", "").trim();
            }
        }
        return "";
    }

    public String getPathParams() {
        StringBuilder sb = new StringBuilder();
        String endpointPath = path.substring(path.lastIndexOf("/{apiVersion}/") + 1);
        String[] saux = endpointPath.split("\\{");
        for (String aux : saux) {
            if (aux.contains("}") && !aux.contains("apiVersion")) {
                sb.append("commandOptions." + aux.substring(0, aux.lastIndexOf("}")) + ", ");
            }
        }
        return sb.toString();
    }

    public String getMandatoryQueryParams(CategoryConfig config) {
        StringBuilder sb = new StringBuilder();
        for (Parameter parameter : getParameters()) {
            if (parameter.getParam().equals("query")) {
                if (config.isAvailableSubCommand(parameter.getName()) && (!parameter.isComplex() || parameter.isStringList())
                        && parameter.isRequired()) {
                    sb.append("commandOptions.").append(CommandLineUtils.getAsVariableName(parameter.getName())).append(", ");
                }
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Endpoint{");
        sb.append("path='").append(path).append('\'');
        sb.append(", method='").append(method).append('\'');
        sb.append(", response='").append(response).append('\'');
        sb.append(", responseClass='").append(responseClass).append('\'');
        sb.append(", notes='").append(notes).append('\'');
        sb.append(", description='").append(description).append('\'');
        sb.append(", parameters=").append(parameters);
        sb.append('}');
        return sb.toString();
    }

    public String getPath() {
        return path;
    }

    public Endpoint setPath(String path) {
        this.path = path;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public Endpoint setMethod(String method) {
        this.method = method;
        return this;
    }

    public String getResponse() {
        return response;
    }

    public Endpoint setResponse(String response) {
        this.response = response;
        return this;
    }

    public String getResponseClass() {
        return responseClass;
    }

    public Endpoint setResponseClass(String responseClass) {
        this.responseClass = responseClass;
        return this;
    }

    public String getNotes() {
        return notes;
    }

    public Endpoint setNotes(String notes) {
        this.notes = notes;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public Endpoint setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<Parameter> getParameters() {
        return parameters;
    }

    public Endpoint setParameters(List<Parameter> parameters) {
        this.parameters = parameters;
        return this;
    }

    public boolean hasParameters() {
        return !CollectionUtils.isEmpty(parameters);
    }
}
