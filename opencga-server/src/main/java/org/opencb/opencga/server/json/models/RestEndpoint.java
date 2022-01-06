package org.opencb.opencga.server.json.models;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.opencga.server.json.config.CategoryConfig;
import org.opencb.opencga.server.json.utils.CommandLineUtils;

import java.util.List;

public class RestEndpoint {

    private String path;
    private String method;
    private String response;
    private String responseClass;
    private String notes;
    private String description;
    private List<RestParameter> parameters;

    public boolean hasPrimitiveBodyParams(CategoryConfig config, String commandName) {
        for (RestParameter restParameter : getParameters()) {
            if (restParameter.getData() != null && !restParameter.getData().isEmpty()) {
                for (RestParameter bodyParam : restParameter.getData()) {
                    //     if ((config.isAvailableSubCommand(bodyParam.getName()) && !bodyParam.isComplex()) || (bodyParam.isStringList()
                    //     )) {
                    if (config.isAvailableSubCommand(bodyParam.getName(), commandName)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean hasQueryParams() {
        for (RestParameter restParameter : getParameters()) {
            if ("query".equals(restParameter.getParam()) && !restParameter.isRequired() && (!restParameter.isComplex() || restParameter.isStringList())) {
                return true;
            }
        }
        return false;
    }

    public String getBodyParamsObject() {
        for (RestParameter restParameter : getParameters()) {
            if (restParameter.getData() != null) {
                return restParameter.getTypeClass().substring(restParameter.getTypeClass().lastIndexOf('.') + 1).replaceAll(";", "").trim();
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

    public String getMandatoryQueryParams(CategoryConfig config, String commandName) {
        StringBuilder sb = new StringBuilder();
        for (RestParameter restParameter : getParameters()) {
            if (restParameter.getParam().equals("query")) {
                if (config.isAvailableSubCommand(restParameter.getName(), commandName) && (!restParameter.isComplex() || restParameter.isStringList())
                        && restParameter.isRequired()) {
                    sb.append("commandOptions.").append(CommandLineUtils.getAsVariableName(restParameter.getName())).append(", ");
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

    public RestEndpoint setPath(String path) {
        this.path = path;
        return this;
    }

    public String getMethod() {
        return method;
    }

    public RestEndpoint setMethod(String method) {
        this.method = method;
        return this;
    }

    public String getResponse() {
        return response;
    }

    public RestEndpoint setResponse(String response) {
        this.response = response;
        return this;
    }

    public String getResponseClass() {
        return responseClass;
    }

    public RestEndpoint setResponseClass(String responseClass) {
        this.responseClass = responseClass;
        return this;
    }

    public String getNotes() {
        return notes;
    }

    public RestEndpoint setNotes(String notes) {
        this.notes = notes;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public RestEndpoint setDescription(String description) {
        this.description = description;
        return this;
    }

    public List<RestParameter> getParameters() {
        return parameters;
    }

    public RestEndpoint setParameters(List<RestParameter> restParameters) {
        this.parameters = restParameters;
        return this;
    }

    public boolean hasParameters() {
        return !CollectionUtils.isEmpty(parameters);
    }
}
