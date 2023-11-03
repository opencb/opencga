/*
 * Copyright 2015-2020 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.server.generator.writers.cli;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.opencga.core.tools.annotations.RestParamType;
import org.opencb.opencga.server.generator.config.CategoryConfig;
import org.opencb.opencga.server.generator.config.Command;
import org.opencb.opencga.server.generator.config.CommandLineConfiguration;
import org.opencb.opencga.server.generator.models.RestApi;
import org.opencb.opencga.server.generator.models.RestCategory;
import org.opencb.opencga.server.generator.models.RestEndpoint;
import org.opencb.opencga.server.generator.models.RestParameter;
import org.opencb.opencga.server.generator.utils.CommandLineUtils;
import org.opencb.opencga.server.generator.writers.ParentClientRestApiWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ExecutorsCliRestApiWriter extends ParentClientRestApiWriter {

    protected static Logger logger = LoggerFactory.getLogger(ExecutorsCliRestApiWriter.class);

    public ExecutorsCliRestApiWriter(RestApi restApi, CommandLineConfiguration config) {
        super(restApi, config);
    }

    @Override
    protected String getClassImports(String key) {
        RestCategory restCategory = availableCategories.get(key);
        CategoryConfig categoryConfig = availableCategoryConfigs.get(key);
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Set<String> imports = new TreeSet<>();
        imports.add("com.fasterxml.jackson.databind.DeserializationFeature");

        if (StringUtils.isEmpty(config.getApiConfig().getExecutorsParentClass())) {
            imports.add("org.opencb.opencga.app.cli.main.executors.OpencgaCommandExecutor");
        } else {
            imports.add(config.getApiConfig().getExecutorsParentClass());
        }

        imports.add("com.fasterxml.jackson.databind.DeserializationFeature");
        imports.add("org.opencb.opencga.app.cli.main.*");
        imports.add("org.opencb.opencga.core.response.RestResponse");
        imports.add("org.opencb.opencga.client.exceptions.ClientException");
        imports.add("org.opencb.commons.datastore.core.ObjectMap");
        imports.add("org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException");
        imports.add("org.opencb.opencga.core.common.JacksonUtils");
        imports.add("com.fasterxml.jackson.databind.ObjectMapper");
        imports.add("java.util.List");
        imports.add("java.util.HashMap");
        imports.add("org.opencb.opencga.core.response.QueryType");
        imports.add("org.opencb.commons.utils.PrintUtils");


        // Add custom parent class
        imports.add(config.getOptions().getOptionsPackage() + "." + getCommandOptionsClassName(restCategory));
        if (categoryConfig.isExecutorExtended()) {
            for (RestEndpoint restEndpoint : restCategory.getEndpoints()) {
                String commandName = getCommandName(restCategory, restEndpoint);
                Command command = categoryConfig.getCommand(commandName);
                if (command != null && StringUtils.isNotEmpty(command.getExecutorExtendedClassName())) {
                    imports.add(command.getExecutorExtendedClassName());
                } else {
                    imports.add("org.opencb.opencga.app.cli.main.custom.Custom" + getAsClassName(restCategory.getName()) + "CommandExecutor");
                }
                if (categoryConfig.isExtendedOptionCommand(commandName)) {
                    imports.add("org.opencb.opencga.app.cli.main.custom.Custom" + getAsClassName(restCategory.getName()) + "CommandOptions");
                }
            }
        }
        for (RestEndpoint restEndpoint : restCategory.getEndpoints()) {
            if (isValidImport(restEndpoint.getResponseClass())) {
                imports.add(restEndpoint.getResponseClass().replaceAll("\\$", "\\."));
            }
            for (RestParameter restParameter : restEndpoint.getParameters()) {
                if (isValidImport(restParameter.getTypeClass())) {
                    imports.add(restParameter.getTypeClass().replaceAll("\\$", "\\."));
                }
                if (restParameter.getData() != null && !restParameter.getData().isEmpty()) {
                    for (RestParameter bodyParam : restParameter.getData()) {
                        if (bodyParam.isComplex() && !bodyParam.isCollection()) {
                            if (bodyParam.getTypeClass() != null) {
                                if (bodyParam.getTypeClass().contains("$")) {
                                    imports.add(bodyParam.getTypeClass().substring(0, bodyParam.getTypeClass().lastIndexOf("$")));
                                } else {
                                    imports.add(bodyParam.getTypeClass().replaceAll("\\$", "\\."));
                                }
                            }
                        }
                        if (bodyParam.getType().equals("enum")) {
                            if (bodyParam.getTypeClass().contains("$")) {
                                imports.add(getEnumImport(bodyParam.getTypeClass().replaceAll("\\$", "\\.")));
                            }
                        }
                    }
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("package ").append(config.getOptions().getExecutorsPackage()).append(";\n\n");

        for (String string : imports) {
            if (string.endsWith(";")) {
                string = string.substring(0, string.length() - 1);
                if (imports.contains(string)) {
                    continue;
                }
            }
            sb.append("import ").append(string).append(";\n");
        }

        sb.append("\n");
        sb.append("\n");
        sb.append("/*\n");
        sb.append("* WARNING: AUTOGENERATED CODE\n");
        sb.append("*\n");
        sb.append("* This code was generated by a tool.\n");
        sb.append("*\n");
        sb.append("* Manual changes to this file may cause unexpected behavior in your application.\n");
        sb.append("* Manual changes to this file will be overwritten if the code is regenerated.\n");
        sb.append("*  \n");
        sb.append("*/\n");
        sb.append("/**\n");
        sb.append(" * This class contains methods for the ").append(restCategory.getName()).append(" command line.\n");
        sb.append(" *    PATH: ").append(restCategory.getPath()).append("\n");
        sb.append(" */\n");
        return sb.toString();
    }

    private String getEnumImport(String importClass) {
        boolean enc = false;
        String res = importClass;
        for (int i = 0; i < importClass.length(); i++) {
            String c = String.valueOf(importClass.charAt(i));
            if (c.equals(c.toUpperCase())) {
                enc = true;
            }
            if (enc && c.equals(".")) {
                res = importClass.substring(0, i) + ";";
            }
        }
        return res;
    }

    public boolean isValidImport(String string) {
        if (string.endsWith(";")) {
            string = string.substring(0, string.length() - 1);
        }
        if (CommandLineUtils.isPrimitiveType(string)) {
            return false;
        }
        String[] excluded = new String[]{"java.lang.Object", "java.lang."};
        return !Arrays.asList(excluded).contains(string);
    }

    @Override
    protected String getClassHeader(String key) {
        StringBuilder sb = new StringBuilder();
        RestCategory restCategory = availableCategories.get(key);
        CategoryConfig config = availableCategoryConfigs.get(key);
        sb.append("public class " + getAsClassName(restCategory.getName()) + "CommandExecutor extends "
                + getExtendedClass() + " {\n\n");
        sb.append("    public String categoryName = \"" + getCategoryRestName(restCategory, config) + "\";\n");
        sb.append("    public " + getCommandOptionsClassName(restCategory) + " " + getCommandOptionsVarName(restCategory) + ";\n\n");
        sb.append("    public " + getAsClassName(restCategory.getName()) + "CommandExecutor(" + getAsClassName(restCategory.getName())
                + "CommandOptions " + getAsVariableName(getAsCamelCase(restCategory.getName()))
                + "CommandOptions) throws CatalogAuthenticationException {\n");
        sb.append("        super(" + getAsVariableName(getAsCamelCase(restCategory.getName())) + "CommandOptions.commonCommandOptions);\n");
        sb.append("        this." + getAsVariableName(getAsCamelCase(restCategory.getName())) + "CommandOptions = "
                + getAsVariableName(getAsCamelCase(restCategory.getName())) + "CommandOptions;\n");
        sb.append("    }\n\n");

        return sb.toString();
    }

    private String methodExecute(RestCategory restCategory, CategoryConfig categoryConfig) {
        StringBuilder sb = new StringBuilder();
        sb.append("    @Override\n");
        sb.append("    public void execute() throws Exception {\n\n");
        sb.append("        logger.debug(\"Executing " + restCategory.getName() + " command line\");\n\n");

        sb.append("        String subCommandString = getParsedSubCommand(" + getAsVariableName(getAsCamelCase(restCategory.getName()))
                + "CommandOptions.jCommander);\n\n");
        sb.append("        RestResponse queryResponse = null;\n\n");
        sb.append("        switch (subCommandString) {\n");
        for (RestEndpoint restEndpoint : restCategory.getEndpoints()) {
            String commandName = getCommandName(restCategory, restEndpoint);
            //  if ("POST".equals(restEndpoint.getMethod()) || restEndpoint.hasParameters()) {
            if (categoryConfig.isAvailableCommand(commandName)) {
                sb.append("            case \"" + reverseCommandName(commandName) + "\":\n");
                sb.append("                queryResponse = " + getJavaMethodName(categoryConfig, commandName) + "();\n");
                sb.append("                break;\n");
            }
            //    }
        }
        if (CollectionUtils.isNotEmpty(categoryConfig.getAddedMethods())) {
            for (String commandName : categoryConfig.getAddedMethods()) {

                String executorClassName = getExecutorClassName(restCategory, categoryConfig, commandName);
                String executorVarName = getExecutorVarName(restCategory);
                String javaMethodName = getJavaMethodName(categoryConfig, commandName);

                String commandOptions = getCommandOptionsVarName(restCategory) + "." + getSubCommandOptionsVarName(categoryConfig, commandName);
                String commonOptionsParams = getCommandOptionsVarName(restCategory) + ".commonCommandOptions.params";

                sb.append("            case \"" + commandName + "\":\n");
                sb.append("                " + executorClassName + " " + executorVarName + " = new " + executorClassName + "(\n");
                sb.append("                        new ObjectMap(" + commonOptionsParams + "), token,\n");
                sb.append("                        clientConfiguration, getSessionManager(), appHome, getLogger());\n");
                sb.append("                queryResponse = " + executorVarName + "." + javaMethodName + "(" + commandOptions + ");\n");
                sb.append("                break;\n");
            }
        }
        sb.append("            default:\n");
        sb.append("                logger.error(\"Subcommand not valid\");\n");
        sb.append("                break;\n");
        sb.append("        }\n\n");
        sb.append("        createOutput(queryResponse);\n\n");
        sb.append("    }\n");
        return sb.toString();
    }

    private String getExtendedClass() {
        return StringUtils.isEmpty(this.config.getApiConfig().getExecutorsParentClass())
                ? "OpencgaCommandExecutor"
                : this.config.getApiConfig().getExecutorsParentClass();
    }

    @Override
    protected String getClassMethods(String key) {
        StringBuilder sb = new StringBuilder();
        RestCategory restCategory = availableCategories.get(key);
        CategoryConfig categoryConfig = availableCategoryConfigs.get(key);
        sb.append(methodExecute(restCategory, categoryConfig));
        for (RestEndpoint restEndpoint : restCategory.getEndpoints()) {
            String commandName = getCommandName(restCategory, restEndpoint);
            if (categoryConfig.isAvailableCommand(commandName)) {
                sb.append("\n");
                sb.append("    private RestResponse<" + getValidResponseNames(restEndpoint.getResponse()) + "> "
                        + getJavaMethodName(categoryConfig, commandName) + "() throws Exception {\n");
                sb.append("        logger.debug(\"Executing " + getJavaMethodName(categoryConfig, commandName) + " in "
                        + restCategory.getName() + " command line\");\n\n");

                String commandOptionsClassName = getCommandOptionsClassName(restCategory, categoryConfig, commandName) + "." + getSubCommandOptionsClassName(categoryConfig, commandName);
                String commandOptionsLocalVarName = "commandOptions";
                String commandOptionsVarName = getCommandOptionsVarName(restCategory) + "." + getSubCommandOptionsVarName(categoryConfig, commandName);

                sb.append("        " + commandOptionsClassName + " " + commandOptionsLocalVarName + " = " + commandOptionsVarName + ";\n");
                if (categoryConfig.isExecutorExtendedCommand(commandName)) {
                    List<RestParameter> body = new ArrayList<>();
                    for (RestParameter restParameter : restEndpoint.getParameters()) {
                        if (restParameter.getData() != null) {
                            body.addAll(restParameter.getData());
                        }
                    }
                    sb.append("        ObjectMap queryParams = new ObjectMap();\n");
                    boolean enc = false;
                    boolean studyPresent = false;
                    restEndpoint.getParameters().addAll(body);
                    for (RestParameter restParameter : restEndpoint.getParameters()) {
                        if (categoryConfig.isAvailableSubCommand(restParameter.getName(), commandName)) {
                            if (restParameter.isAvailableType() && !restParameter.getName().equals("body")) {
                                enc = true;
                                if (normalizeNames(restParameter.getName()).equals("study")) {
                                    studyPresent = true;
                                }
                                if (StringUtils.isNotEmpty(restParameter.getType()) && "string".equalsIgnoreCase(restParameter.getType())) {
                                    sb.append("        queryParams.putIfNotEmpty(\"")
                                            .append(normalizeNames(restParameter.getName()))
                                            .append("\", commandOptions.")
                                            .append(normalizeNames(restParameter.getName()))
                                            .append(");\n");
                                } else {
                                    if (restParameter.isStringList()) {
                                        sb.append("        queryParams.putIfNotNull(\"")
                                                .append(normalizeNames(restParameter.getName()))
                                                .append("\", CommandLineUtils").append(".getListValues(commandOptions.")
                                                .append(normalizeNames(restParameter.getName()))
                                                .append("));\n");
                                    } else {
                                        sb.append("        queryParams.putIfNotNull(\"")
                                                .append(normalizeNames(restParameter.getName()))
                                                .append("\", commandOptions.")
                                                .append(normalizeNames(restParameter.getName()))
                                                .append(");\n");
                                    }
                                }
                            }
                        }
                    }
                    if (enc) {
                        if (studyPresent) {
                            sb.append("        if (queryParams.get(\"study\") == null && OpencgaMain.isShellMode()) {\n");
                            sb.append("            queryParams.putIfNotEmpty(\"study\", sessionManager.getSession().getCurrentStudy());\n");
                            sb.append("        }\n");
                        }
                    }

                    String executorClassName = getExecutorClassName(restCategory, categoryConfig, commandName);
                    String executorVarName = getExecutorVarName(restCategory);

                    sb.append("        " + executorClassName + " " + executorVarName + " = new " + executorClassName + "(queryParams, token, clientConfiguration, getSessionManager(), appHome, getLogger());\n");
                    sb.append("        return " + executorVarName + "." + getJavaMethodName(categoryConfig, commandName) + "(commandOptions);\n");
                } else {
                    sb.append(getQueryParams(restEndpoint, categoryConfig, commandName));
                    sb.append(getBodyParams(restCategory, restEndpoint, categoryConfig, commandName));
                    sb.append(getReturn(restCategory, restEndpoint, categoryConfig, commandName));
                }
                sb.append("    }\n");
            }
        }
        return sb.toString();
    }

    private String getReturn(RestCategory restCategory, RestEndpoint restEndpoint, CategoryConfig config, String commandName) {
        String opencgaClientObjectName = "openCGAClient";
        String className = getAsClassName(config.getKey());
        if (StringUtils.isNotEmpty(this.config.getApiConfig().getExecutorsOpencgaClientPrefix())) {
            opencgaClientObjectName = this.config.getApiConfig().getExecutorsOpencgaClientPrefix() + "OpenCGAClient";
            className = StringUtils.capitalize(this.config.getApiConfig().getExecutorsOpencgaClientPrefix()) + getAsClassName(config.getKey());
        }

        String res = "        return " + opencgaClientObjectName + ".get" + className + "Client()."
                + getJavaMethodName(config, commandName) + "(";
        res += restEndpoint.getPathParams();
        res += restEndpoint.getMandatoryQueryParams(config, commandName);

        String bodyClassName = restEndpoint.getBodyClassName();
        if (StringUtils.isNotEmpty(bodyClassName)) {
            res += getAsVariableName(bodyClassName) + ", ";
        }
        if (restEndpoint.hasQueryParams()) {
            res += "queryParams";
        }
        if (res.trim().endsWith(",")) {
            res = res.substring(0, res.lastIndexOf(","));
        }
        res += ");\n";
        return res;
    }

    private String getBodyParams(RestCategory restCategory, RestEndpoint restEndpoint, CategoryConfig config, String commandName) {
        StringBuilder sb = new StringBuilder();
        String bodyClassName = restEndpoint.getBodyClassName();
        if (StringUtils.isNotEmpty(bodyClassName)) {

            String variableName = getAsVariableName(bodyClassName);

            sb.append("\n        " + bodyClassName + " " + variableName + " = null;");

            sb.append("\n        if (commandOptions.jsonDataModel) {");
            sb.append("\n            RestResponse<" + getValidResponseNames(restEndpoint.getResponse()) + "> res = new RestResponse<>();");
            sb.append("\n            res.setType(QueryType.VOID);");
            sb.append("\n            PrintUtils.println(getObjectAsJSON(categoryName,\""+restEndpoint.getPath()+"\"));");
            sb.append("\n            return res;");
            sb.append("\n        } else if (commandOptions.jsonFile != null) {");
            sb.append("\n            " + getAsVariableName(bodyClassName) + " = JacksonUtils.getDefaultObjectMapper()");
            sb.append("\n                    .readValue(new java.io.File(commandOptions.jsonFile), " + bodyClassName + ".class);");
            sb.append("\n        }");
            if (hasParameters(restEndpoint.getParameters(), commandName, config)) {
                sb.append(" else {\n");
                RestParameter body = restEndpoint.getParameters().stream().filter(r -> r.getName().equals("body")).findFirst().orElse(null);
                if (body != null) {
                    sb.append("            ObjectMap beanParams = new ObjectMap();\n");
                    sb.append(getBodyParams(body));
                    sb.append("\n            " + getAsVariableName(bodyClassName) + " = JacksonUtils.getDefaultObjectMapper().copy()");
                    sb.append("\n                    .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, true)");
                    sb.append("\n                    .readValue(beanParams.toJson(), " + bodyClassName + ".class);");
                    sb.append("\n        }\n");
                }
            } else {
                sb.append("\n");
            }
        }
        return sb.toString();
    }

    private String getBodyParams(RestParameter body) {
        StringBuilder sb = new StringBuilder();
        for (RestParameter restParameter : body.getData()) {
            if (CollectionUtils.isEmpty(restParameter.getData())) {
                if (restParameter.isAvailableType() || restParameter.isEnum() || isValidMap(restParameter)) {
                    String javaCommandOptionsField = "commandOptions." + getJavaFieldName(restParameter);
                    String label = StringUtils.isEmpty(restParameter.getParentName()) ? restParameter.getName() : restParameter.getParentName() + "." + restParameter.getName();
                    if (restParameter.getTypeClass().equals("java.lang.String;")) {
                        sb.append("            putNestedIfNotEmpty(beanParams, \"" + label.replaceAll("body_", "") + "\"," + javaCommandOptionsField + ", true);\n");
                    } else {
                        sb.append("            putNestedIfNotNull(beanParams, \"" + label.replaceAll("body_", "") + "\"," + javaCommandOptionsField + ", true);\n");
                    }
                }
            }
        }
        return sb.toString();
    }


    private String getJavaFieldName(RestParameter restParam) {
        if (restParam.getParentName() != null) {
            return normalizeNames(getAsCamelCase(restParam.getParentName() + " " + restParam.getName()));
        } else {
            return normalizeNames(getAsCamelCase(restParam.getName()));
        }
    }

    private String getJavaClassName(RestParameter restParam) {
        return getJavaClassName(restParam.getTypeClass());
    }

    private String getJavaClassName(String typeClass) {
        typeClass = StringUtils.removeEnd(typeClass, ";"); // trailing `;` for legacy reasons
        typeClass = typeClass.substring(typeClass.lastIndexOf(".") + 1); // Remove package information
        typeClass = StringUtils.replaceChars(typeClass, '$', '.');  // Convert inner classes ref
        return typeClass;
    }

    private boolean hasParameters(List<RestParameter> parameters, String commandName, CategoryConfig config) {
        boolean res = false;
        Set<String> variables = new HashSet<>();
        for (RestParameter restParameter : parameters) {
            if (restParameter.getData() != null && !restParameter.getData().isEmpty()) {
                for (RestParameter bodyParam : restParameter.getData()) {
                    if (config.isAvailableSubCommand(bodyParam.getName(), commandName)) {
                        if (!bodyParam.isComplex() && !bodyParam.isInnerParam()) {
                            // sometimes the name of the parameter has the prefix "body" so as not to coincide with another parameter
                            // with the same name, but the setter does not have this prefix, so it must be removed
                            return true;
                        } else if (bodyParam.isStringList()) {
                            return true;
                        } else if (isValidMap(bodyParam)) {
                            return true;
                        }
                        if (bodyParam.getType().equals("enum")) {
                            if (reverseCommandName(commandName).contains("create")) {
                                return true;
                            }
                        }
                    }

                    // If the parameter is InnerParam (It means it's a field of inner bean) need to add to the variables Set
                    // for no duplicate set action of Bean (Parent)
                    if (bodyParam.isInnerParam() && !bodyParam.isCollection()) {
                        if (!variables.contains(bodyParam.getParentName())) {
                            variables.add(bodyParam.getParentName());
                            return true;
                        }
                    }
                }
            }
        }
        return res;
    }

    private String getQueryParams(RestEndpoint restEndpoint, CategoryConfig config, String commandName) {
        StringBuilder res = new StringBuilder("\n        ObjectMap queryParams = new ObjectMap();\n");
        boolean enc = false;
        boolean studyPresent = false;
        for (RestParameter restParameter : restEndpoint.getParameters()) {
            if (config.isAvailableSubCommand(restParameter.getName(), commandName)) {
                if (restParameter.getParam() == RestParamType.QUERY && !restParameter.isRequired() && restParameter.isAvailableType()) {
                    enc = true;
                    if (normalizeNames(restParameter.getName()).equals("study")) {
                        studyPresent = true;
                    }
                    if (StringUtils.isNotEmpty(restParameter.getType()) && "string".equalsIgnoreCase(restParameter.getType())) {
                        res.append("        queryParams.putIfNotEmpty(\"")
                                .append(normalizeNames(restParameter.getName()))
                                .append("\", commandOptions.")
                                .append(normalizeNames(restParameter.getName()))
                                .append(");\n");
                    } else {
                        if (restParameter.isStringList()) {
                            res.append("        queryParams.putIfNotNull(\"")
                                    .append(normalizeNames(restParameter.getName()))
                                    .append("\", CommandLineUtils").append(".getListValues(commandOptions.")
                                    .append(normalizeNames(restParameter.getName()))
                                    .append("));\n");
                        } else {
                            res.append("        queryParams.putIfNotNull(\"")
                                    .append(normalizeNames(restParameter.getName()))
                                    .append("\", commandOptions.")
                                    .append(normalizeNames(restParameter.getName()))
                                    .append(");\n");
                        }
                    }
                }
            }
        }
        if (enc) {
            if (studyPresent) {
                res.append("        if (queryParams.get(\"study\") == null && OpencgaMain.isShellMode()) {\n");
                res.append("            queryParams.putIfNotEmpty(\"study\", sessionManager.getSession().getCurrentStudy());\n");
                res.append("        }\n");
            }
            return res + "\n";
        }
        return "";
    }

    private String getValidResponseNames(String responseClass) {
        Map<String, String> validResponse = new HashMap<>();
        validResponse.put("map", "ObjectMap");
        validResponse.put("Map", "ObjectMap");
        //  validResponse.put("Object", "ObjectMap");
        validResponse.put("", "ObjectMap");

        responseClass = responseClass.replace('$', '.');
        if (validResponse.containsKey(responseClass)) {
            return validResponse.get(responseClass);
        }
        return responseClass;
    }

    private String normalizeNames(String name) {
        name = getAsCamelCase(name, "\\.");
        if (invalidNames.containsKey(name)) {
            name = invalidNames.get(name);
        }
        return name;
    }

    @Override
    protected String getClassFileName(String key) {
        RestCategory restCategory = availableCategories.get(key);
        return config.getOptions().getExecutorsOutputDir() + "/" + getAsClassName(restCategory.getName()) + "CommandExecutor.java";
    }
}
