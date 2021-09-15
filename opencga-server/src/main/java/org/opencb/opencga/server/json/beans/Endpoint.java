package org.opencb.opencga.server.json.beans;

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

    @Override
    public String toString() {
        return "Endpoint{" +
                "path='" + path + '\'' +
                ", method='" + method + '\'' +
                ", response='" + response + '\'' +
                ", responseClass='" + responseClass + '\'' +
                ", notes='" + notes + '\'' +
                ", description='" + description + '\'' +
                ", parameters=" + parameters +
                "}\n";
    }

    public boolean hasPrimitiveBodyParams(CategoryConfig config) {

        for (Parameter parameter : getParameters()) {
            if (parameter.getData() != null && !parameter.getData().isEmpty()) {
                for (Parameter body_param : parameter.getData()) {
                    if (config.isAvailableSubCommand(body_param.getName()) && CommandLineUtils.isPrimitive(body_param.getType())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    public boolean hasQueryParams() {
        for (Parameter parameter : getParameters()) {
            if ("query".equals(parameter.getParam()) && !parameter.isRequired() && CommandLineUtils.isPrimitive(parameter.getType())) {
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

                  /*  if (body_param.getName().equals("action")) {
                        System.out.println("action :::: config.isAvailableSubCommand(body_param.getName()) " + config
                        .isAvailableSubCommand(body_param.getName()));
                        System.out.println("action :::: CLIUtils.isPrimitive(body_param.getType()) " + CLIUtils.isPrimitive(body_param
                        .getType()));
                        System.out.println("action :::: body_param.isRequired() " + body_param.isRequired());
                    }
*/
                if (config.isAvailableSubCommand(parameter.getName()) && CommandLineUtils.isPrimitive(parameter.getType()) && parameter.isRequired()) {
                    sb.append("commandOptions." + CommandLineUtils.getAsVariableName(parameter.getName()) + ", ");
                }
            }
        }
        return sb.toString();
    }
}
