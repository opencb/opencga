package org.opencb.opencga.server.generator.openapi.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Swagger {
    // https://github.com/OAI/OpenAPI-Specification/blob/main/versions/2.0.md#swagger-object
    private String swagger = "2.0";
    private Info info;
    private String host;
    private String basePath;
    private List<Tag> tags;
    private List<String> schemes;
    private Map<String, Map<String,Method>> paths;
    private Map<String, Map<String, Object>> securityDefinitions;
    // https://github.com/OAI/OpenAPI-Specification/blob/main/versions/2.0.md#definitions-object
    private Map<String, Definition> definitions;

    public Swagger() {
    }

    public String getSwagger() {
        return swagger;
    }

    public Swagger setSwagger(String swagger) {
        this.swagger = swagger;
        return this;
    }

    public Info getInfo() {
        return info;
    }

    public Swagger setInfo(Info info) {
        this.info = info;
        return this;
    }

    public String getHost() {
        return host;
    }

    public Swagger setHost(String host) {
        this.host = host;
        return this;
    }

    public String getBasePath() {
        return basePath;
    }

    public Swagger setBasePath(String basePath) {
        this.basePath = basePath;
        return this;
    }

    public List<Tag> getTags() {
        return tags;
    }

    public Swagger setTags(List<Tag> tags) {
        this.tags = tags;
        return this;
    }

    public List<String> getSchemes() {
        return schemes;
    }

    public Swagger setSchemes(List<String> schemes) {
        this.schemes = schemes;
        return this;
    }

    public Map<String, Map<String, Method>> getPaths() {
        return paths;
    }

    public Swagger setPaths(Map<String, Map<String, Method>> paths) {
        this.paths = paths;
        return this;
    }

    public Map<String, Map<String, Object>> getSecurityDefinitions() {
        return securityDefinitions;
    }

    public Swagger setSecurityDefinitions(Map<String, Map<String, Object>> securityDefinitions) {
        this.securityDefinitions = securityDefinitions;
        return this;
    }

    public Map<String, Definition> getDefinitions() {
        return definitions;
    }

    public Swagger setDefinitions(Map<String, Definition> definitions) {
        this.definitions = definitions;
        return this;
    }
}
