package org.opencb.opencga.server.generator.models;

import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.server.generator.config.CategoryConfig;
import org.opencb.opencga.server.generator.config.CommandLineConfiguration;
import org.opencb.opencga.server.generator.config.Shortcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RestParameterManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(RestParameterManager.class);
    private List<RestParameter> queryParams = new ArrayList<>();
    private List<RestParameter> bodyParams = new ArrayList<>();
    private List<RestParameter> pathParams = new ArrayList<>();

    private List<RestParameter> enumParams = new ArrayList<>();

    private static final Map<String, String> validTypes = new HashMap<>();
    private static final Map<String, String> validNames = new HashMap<>();
    private CommandLineConfiguration config;
    private CategoryConfig categoryConfig;

    private String commandName;

    public RestParameterManager(RestEndpoint endpoint, CommandLineConfiguration config, CategoryConfig categoryConfig, String commandName) {
        this.config = config;
        this.categoryConfig = categoryConfig;
        this.commandName = commandName;
        for (RestParameter parameter : endpoint.getParameters()) {
            normalizeParameters(parameter);
        }

    }


    private void normalizeParameters(RestParameter parameter) {

        parameter.setDescription(parameter.getDescription().replaceAll("\"", "'"));
        parameter.setName(getValidName(parameter.getName()));
        //System.out.println("Type = " + parameter.getType());
        if ("enum".equals(parameter.getType())) {
            enumParams.add(parameter);
            return;
        }
        switch (parameter.getParam()) {
            case BODY:
                manageBodyParam(parameter);
                break;
            case PATH:
                pathParams.add(parameter);
                break;
            case QUERY:
                queryParams.add(parameter);
                break;
        }

    }

    private void manageBodyParam(RestParameter parameter) {
        if (parameter.getData() != null) {
            for (RestParameter param : parameter.getData()) {
                param.setDescription(param.getDescription().replaceAll("\"", "'"));
                if (param.getData() != null) {
                    for (RestParameter paramData : param.getData()) {
                        paramData.setDescription(paramData.getDescription().replaceAll("\"", "'"));
                        if (StringUtils.isNotEmpty(paramData.getParentName())) {
                            paramData.setName(paramData.getParentName() + "-" + paramData.getName());
                        }
                        if ("enum".equals(paramData.getType())) {
                            enumParams.add(paramData);
                        } else if (isAvailable(paramData)) {
                            bodyParams.add(paramData);
                        }
                    }
                }
                if ("enum".equals(param.getType())) {
                    enumParams.add(param);
                } else if (isAvailable(param)) {
                    bodyParams.add(param);
                }
            }
        }
    }

    private boolean isAvailable(RestParameter param) {
        return categoryConfig.isAvailableSubCommand(param.getName(), commandName) && param.isAvailableType();
    }

    public String getVariableName(RestParameter parameter) {
        return getAsCamelCase(parameter.getName());
    }

    private String getAsCamelCase(String s) {
        if (s.contains("-")) {
            s = s.replaceAll("-", "_");
        }
        if (s.contains(" ")) {
            s = s.replaceAll(" ", "_");
        }
        return getAsCamelCase(s, "_");
    }

    protected static String getAsCamelCase(String s, String separator) {
        String[] words = s.split(separator);
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < words.length; i++) {
            String word = words[i].trim();
            if (i == 0) {
                word = word.isEmpty() ? word : Character.toLowerCase(word.charAt(0)) + word.substring(1);
            } else {
                word = word.isEmpty() ? word : Character.toUpperCase(word.charAt(0)) + word.substring(1);
            }
            builder.append(word);
        }
        return builder.toString();
    }


    public String getJCommanderOptions() {
        List<RestParameter> params = new ArrayList<>();
        params.addAll(pathParams);
        params.addAll(queryParams);
        params.addAll(bodyParams);
        params.addAll(enumParams);
        StringBuilder sb = new StringBuilder();
        Set<String> names = new HashSet<>();
        for (RestParameter param : params) {
            String variableName = getVariableName(param);
            if (!names.contains(variableName)) {
                names.add(variableName);
                String shortcut = getShortCuts(param);
                if (param.getType().equals("Map") || param.getType().equals("ObjectMap")) {
                    sb.append("        @DynamicParameter(names = {" + shortcut + "}, " +
                            "description"
                            + " = \"" + param.getDescription() + ". Use: " + shortcut.split(", ")[0].replace("\"", "") + " key=value\", required = "
                            + param.isRequired() + ")\n");
                    sb.append("        public Map<String, ?> " + variableName + " = new HashMap<>(); //Dynamic parameters must be initialized;\n");
                    sb.append("    \n");
                } else {
                    sb.append("        @Parameter(names = {" + shortcut + "}, description = " +
                            "\"" + param.getDescription() + "\", required = " + param.isRequired() + ", arity = 1)\n");
                    sb.append("        public " + getValidValue(param.getType()) + " " + variableName + ";" +
                            " " +
                            "\n");
                    sb.append("    \n");
                }
            } else {
                System.out.println("El comando esta repetido " + variableName);
            }
        }
        return sb.toString();
    }


    private String getShortCuts(RestParameter restParameter) {
        if (restParameter.isInnerParam()) {
            return "\"--" + getKebabCase(restParameter.getParentName()) + "-" + getKebabCase(restParameter.getName()) + "\""
                    + getStringShortcuts(getKebabCase(restParameter.getParentName()) + "-" + getKebabCase(restParameter.getName()), categoryConfig);
        } else {
            return "\"--" + getKebabCase(restParameter.getName()) + "\"" + getStringShortcuts(restParameter.getName(), categoryConfig);
        }
    }

    public String getStringShortcuts(String parameter, CategoryConfig categoryConfig) {
        String res = "";
        Set<String> scut = new HashSet<>();

        //Generic shortcuts
        if (config.getApiConfig().getShortcuts() != null) {
            for (Shortcut sc : config.getApiConfig().getShortcuts()) {
                if (parameter.equals(sc.getName()) && !scut.contains(sc.getShortcut())) {
                    scut.add(sc.getShortcut());
                    String dash = "-";
                    if (sc.getShortcut().length() > 1) {
                        dash = "--";
                    }
                    res += ", \"" + dash + "" + sc.getShortcut() + "\"";
                }
            }
        }

        // category shortcuts
        if (categoryConfig.getShortcuts() != null) {
            for (Shortcut sc : categoryConfig.getShortcuts()) {
                if (parameter.equals(sc.getName()) && !scut.contains(sc.getShortcut())) {
                    scut.add(sc.getShortcut());
                    String dash = "-";
                    if (sc.getShortcut().length() > 1) {
                        dash = "--";
                    }
                    res += ", \"" + dash + "" + sc.getShortcut() + "\"";
                }
            }
        }
        return res;
    }

    private String getKebabCase(String camelStr) {
        String ret = camelStr.replaceAll("([A-Z]+)([A-Z][a-z])", "$1-$2").replaceAll("([a-z])([A-Z])", "$1-$2");
        return ret.toLowerCase();
    }


    static {
        validTypes.put("String", "String");
        validTypes.put("Map", "Map<String, ?>");
        validTypes.put("string", "String");
        validTypes.put("object", "Object");
        validTypes.put("Object", "Object");
        validTypes.put("integer", "Integer");
        validTypes.put("int", "Integer");
        validTypes.put("map", "ObjectMap");
        validTypes.put("boolean", "Boolean");
        validTypes.put("enum", "String");
        validTypes.put("long", "Long");
        validTypes.put("Long", "Long");
        validTypes.put("ObjectMap", "Map<String, ?>");
        validTypes.put("java.lang.String", "String");
        validTypes.put("java.lang.Boolean", "Boolean");
        validTypes.put("java.lang.Integer", "Integer");
        validTypes.put("java.lang.Long", "Integer");
        validTypes.put("java.lang.Short", "Integer");
        validTypes.put("java.lang.Double", "Integer");
        validTypes.put("java.lang.Float", "Integer");
        validTypes.put("List", "String");
        validTypes.put("java.util.List", "String");
    }

    static {
        validNames.put("default", "defaultStats");
        validNames.put("class", "className");

    }

    public String getValidValue(String key) {
        String res = key;

        if (validTypes.containsKey(key)) {
            res = validTypes.get(key);
        }
        return res;
    }

    public String getValidName(String key) {
        String res = key;

        if (validNames.containsKey(key)) {
            res = validNames.get(key);
        }
        return res;
    }


    public List<RestParameter> getQueryParams() {
        return queryParams;
    }

    public void setQueryParams(List<RestParameter> queryParams) {
        this.queryParams = queryParams;
    }

    public List<RestParameter> getBodyParams() {
        return bodyParams;
    }

    public void setBodyParams(List<RestParameter> bodyParams) {
        this.bodyParams = bodyParams;
    }

    public List<RestParameter> getPathParams() {
        return pathParams;
    }

    public void setPathParams(List<RestParameter> pathParams) {
        this.pathParams = pathParams;
    }

}
