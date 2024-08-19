/*
 * Copyright 2015-2017 OpenCB
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

package com.zettagenomics.opencga.enterprise.app.cli.main.executors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zettagenomics.opencga.enterprise.client.rest.EnterpriseOpenCGAClient;
import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.DataModelsUtils;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.io.*;
import org.opencb.opencga.app.cli.main.utils.CommandLineUtils;
import org.opencb.opencga.app.cli.session.Session;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;
import org.opencb.opencga.server.generator.models.RestCategory;
import org.opencb.opencga.server.generator.models.RestEndpoint;
import org.opencb.opencga.server.generator.models.RestParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import org.apache.commons.collections4.MapUtils;

import org.apache.commons.collections4.MapUtils;
/**
 * Created on 27/05/16.
 *
 * @author imedina
 */
public abstract class EnterpriseOpencgaCommandExecutor extends CommandExecutor {

    protected EnterpriseOpenCGAClient enterpriseOpenCGAClient;
    protected EnterpriseConfiguration enterpriseConfiguration;
    protected AbstractOutputWriter writer;

    private Logger privateLogger;

    public EnterpriseOpencgaCommandExecutor(GeneralCliOptions.CommonCommandOptions options)
            throws CatalogAuthenticationException {
        this(options, false);
    }

    @Deprecated
    public EnterpriseOpencgaCommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean skipDuration)
            throws CatalogAuthenticationException {
        super(options, true);

        init(options, skipDuration);
    }

    public void loadEnterpriseConfiguration() throws IOException {
        // We load configuration file either from app home folder or from the JAR
        Path path = Paths.get(this.conf).resolve("enterprise-configuration.yml");
        if (Files.exists(path)) {
            privateLogger.debug("Loading enterprise-configuration from '{}'", path.toAbsolutePath());
            this.enterpriseConfiguration = EnterpriseConfiguration.load(Files.newInputStream(path.toFile().toPath()));
        } else {
            privateLogger.debug("Loading enterprise-configuration from JAR file");
            this.enterpriseConfiguration = EnterpriseConfiguration
                    .load(ClientConfiguration.class.getClassLoader().getResourceAsStream("enterprise-configuration.yml"));
        }
    }

    public static List<String> splitWithTrim(String value) {
        return splitWithTrim(value, ",");
    }

    public static List<String> splitWithTrim(String value, String separator) {
        List<String> result = null;
        if (value != null) {
            String[] splitFields = value.split(separator);
            result = new ArrayList<>(splitFields.length);
            for (String s : splitFields) {
                result.add(s.trim());
            }
        }
        return result;
    }

    private void init(GeneralCliOptions.CommonCommandOptions options, boolean skipDuration) {
        try {
            privateLogger = LoggerFactory.getLogger(EnterpriseOpencgaCommandExecutor.class);
            privateLogger.debug("Executing OpencgaEnterpriseCommandExecutor 'init' method ...");

            this.loadEnterpriseConfiguration();

            // Configure CLI output writer
            WriterConfiguration writerConfiguration = new WriterConfiguration();
            writerConfiguration.setMetadata(options.metadata);
            writerConfiguration.setHeader(!options.noHeader);
            switch (options.outputFormat.toLowerCase()) {
                case "json_pretty":
                    writerConfiguration.setPretty(true);
                case "json":
                    this.writer = new JsonOutputWriter(writerConfiguration);
                    break;
                case "yml":
                case "yaml":
                    this.writer = new YamlOutputWriter(writerConfiguration);
                    break;
                case "table":
                    this.writer = new TextOutputWriter(writerConfiguration, Table.PrinterType.JANSI);
                    break;
                case "text":
                default:
                    this.writer = new TextOutputWriter(writerConfiguration);
                    break;
            }

            privateLogger.debug("Using sessionFile '{}'", this.sessionManager.getSessionPath().toString());
            if (StringUtils.isNotEmpty(options.token)) {
                privateLogger.debug("A new token has been provided, updating session and client");

                // Set internal fields
                ObjectMap objectMap = parseTokenClaims(options.token);
                userId = objectMap.getString("sub", "");
                token = options.token;

                // Update SessionManager and OpencgaClient with the new token
                sessionManager.updateSessionToken(token, host);
                enterpriseOpenCGAClient = new EnterpriseOpenCGAClient(new AuthenticationResponse(options.token), clientConfiguration);
            } else {
                privateLogger.debug("No token has been provided, reading session file");
                if (!StringUtils.isEmpty(sessionManager.getSession().getToken())
                        && !SessionManager.NO_TOKEN.equals(sessionManager.getSession().getToken())) {
                    // FIXME it seems skipDuration is not longer used, this should be either implemented or removed
                    if (skipDuration) {
                        privateLogger.debug("Skip duration set to {}, THIS MUST BE REMOVED", skipDuration);
                    } else {
                        privateLogger.debug("Skip duration set to {}", skipDuration);

                        // Get token claims
                        ObjectMap claimsMap = parseTokenClaims(sessionManager.getSession().getToken());

                        // Check if session has expired
                        Date expirationDate = new Date(claimsMap.getLong("exp") * 1000L);
                        Date currentDate = new Date();
                        if (currentDate.before(expirationDate) || !claimsMap.containsKey("exp")) {
                            privateLogger.debug("Session expiration time is ok, valid until: {}", expirationDate);
                            enterpriseOpenCGAClient = new EnterpriseOpenCGAClient(
                                    new AuthenticationResponse(sessionManager.getSession().getToken(), sessionManager.getSession().getRefreshToken()),
                                    clientConfiguration);
                            enterpriseOpenCGAClient.setUserId(sessionManager.getSession().getUser());

                            // FIXME This looks weird, commenting it
//                            if (options.token == null) {
//                                options.token = sessionManager.getToken();
//                            }
                        } else {
                            privateLogger.debug("Session has expired '{}'.", expirationDate);
                            enterpriseOpenCGAClient = new EnterpriseOpenCGAClient(clientConfiguration);
                            //sessionManager.logoutSessionFile();
                        }
                    }
                } else {
                    privateLogger.debug("No valid session found");
                    enterpriseOpenCGAClient = new EnterpriseOpenCGAClient(clientConfiguration);
                }
            }
            updateClientConfigurationAttributes();

            if (enterpriseOpenCGAClient != null) {
                enterpriseOpenCGAClient.setThrowExceptionOnError(true);
            }
        } catch (IOException e) {
            logger.error("OpencgaEnterpriseCommandExecutorError", e);
            CommandLineUtils.error("OpencgaEnterpriseCommandExecutorError", e);
        }
    }

    private void updateClientConfigurationAttributes() {
        // Update attributes from ClientConfiguration
        Session session = sessionManager.getSession();
        if (session != null && session.getAttributes() != null && !session.getAttributes().isEmpty()) {
            if (clientConfiguration.getAttributes() != null) {
                clientConfiguration.getAttributes().putAll(session.getAttributes());
            } else {
                clientConfiguration.setAttributes(session.getAttributes());
            }
        }
    }

    protected void createOutput(RestResponse queryResponse) {
        if (writer != null && queryResponse != null) {
            writer.print(queryResponse);
        } else {
            privateLogger.error("Null object found: writer set to '{}' and queryResponse set to '{}'", writer, queryResponse);
        }
    }

    @Deprecated
    protected void invokeSetter(Object obj, String propertyName, Object variableValue) {
        if (obj != null && variableValue != null) {
            try {
                String setMethodName = "set" + StringUtils.capitalize(propertyName);
                Method setter = obj.getClass().getMethod(setMethodName, variableValue.getClass());
                setter.invoke(obj, variableValue);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected ObjectMap parseTokenClaims(String token) throws JsonProcessingException {
        String claims = StringUtils.split(token, ".")[1];
        String decodedClaimsString = new String(Base64.getDecoder().decode(claims), StandardCharsets.UTF_8);
        return new ObjectMapper().readValue(decodedClaimsString, ObjectMap.class);
    }

    public EnterpriseOpenCGAClient getOpenCGAClient() {
        return enterpriseOpenCGAClient;
    }

    public EnterpriseOpencgaCommandExecutor setOpenCGAClient(EnterpriseOpenCGAClient enterpriseOpenCGAClient) {
        this.enterpriseOpenCGAClient = enterpriseOpenCGAClient;
        return this;
    }

    public String getObjectAsJSON(String objectCategory, String objectPath) throws Exception {
        return super.getObjectAsJSON(objectCategory,objectPath,enterpriseOpenCGAClient);
    }
/*
    public String getObjectAsJSON(String objectCategory, String objectPath) throws Exception {
        StringBuilder jsonInString = new StringBuilder("\n");
        try {
            ObjectMap queryParams = new ObjectMap();
            queryParams.putIfNotEmpty("category", objectCategory);
            RestResponse<List> response = enterpriseOpenCGAClient.getMetaClient().api(queryParams);
            ObjectMapper jsonObjectMapper = new ObjectMapper();
            for (List list : response.getResponses().get(0).getResults()) {
                List<RestCategory> categories = jsonObjectMapper.convertValue(list, new TypeReference<List<RestCategory>>() {});
                for (RestCategory category : categories) {
                    for (RestEndpoint endpoint : category.getEndpoints()) {
                        if (objectPath.equals(endpoint.getPath())) {
                            boolean enc = false;
                            for (RestParameter parameter : endpoint.getParameters()) {
                                //jsonInString += parameter.getName()+":"+parameter.getAllowedValues()+"\n";
                                if (parameter.getData() != null) {
                                    enc = true;
                                    jsonInString.append(printBody(parameter.getData(), ""));
                                }
                            }
                            if (!enc) {
                                jsonInString.append("No model available");
                            }
                            //
                        }
                    }
                }
            }
        } catch (Exception e) {
            jsonInString = new StringBuilder("Data model not found.");
            CommandLineUtils.error(e);
        }
        return jsonInString.toString();
    }

    private String printBody(List<RestParameter> data, String tabs) {
        String res = "";
        res += "{\n";
        String tab = "    " + tabs;
        for (RestParameter parameter : data) {
            if (parameter.getData() == null) {
                res += printParameter(parameter, tab);
            } else {
                res += tab + parameter.getName() + "\"" + ": [" + printBody(parameter.getData(), tab) + "],\n";
            }
        }
        res += tabs + "}";
        return res;
    }

    private String printParameter(RestParameter parameter, String tab) {

        return tab + "\"" + parameter.getName() + "\"" + ":" + printParameterValue(parameter) + ",\n";
    }

    private String printParameterValue(RestParameter parameter) {

        if(!StringUtils.isEmpty(parameter.getAllowedValues())){
            return parameter.getAllowedValues().replace(" ", "|");
        }
        switch (parameter.getType()) {
            case "Boolean":
            case "java.lang.Boolean":
                return "false";
            case "Long":
            case "Float":
            case "Double":
            case "Integer":
            case "int":
            case "double":
            case "float":
            case "long":
                return "0";
            case "List":
                return "[\"\"]";
            case "Date":
                return "\"dd/mm/yyyy\"";
            case "Map":
                return "{\"key\": \"value\"}";
            case "String":
                return "\"\"";
            default:
                return "\"-\"";
        }
    }

    public RestResponse<AuthenticationResponse> saveSession(String user, AuthenticationResponse response) throws ClientException, IOException {
        RestResponse<AuthenticationResponse> res = new RestResponse<>();
        if (response != null) {
            List<String> studies = new ArrayList<>();
            logger.debug(response.toString());
            RestResponse<Project> projects = enterpriseOpenCGAClient.getProjectClient()
                    .search(new ObjectMap(ProjectDBAdaptor.QueryParams.OWNER.key(), user));

            if (projects.getResponses().get(0).getNumResults() == 0) {
                // We try to fetch shared projects and studies instead when the user does not own any project or study
                projects = enterpriseOpenCGAClient.getProjectClient().search(new ObjectMap());
            }

            for (Project project : projects.getResponses().get(0).getResults()) {
                for (Study study : project.getStudies()) {
                    studies.add(study.getFqn());
                }
            }
            this.sessionManager.saveSession(user, response.getToken(), response.getRefreshToken(), studies, this.host);
            res.setType(QueryType.VOID);
        }
        return res;
    }
*/
    public RestResponse<AuthenticationResponse> refreshToken(AuthenticationResponse response) throws ClientException, IOException {
        RestResponse<AuthenticationResponse> res = new RestResponse<>();
        if (response != null) {
            this.sessionManager.refreshSession(response.getRefreshToken(), this.host);
            res.setType(QueryType.VOID);
        }
        return res;
    }

    public Object putNestedIfNotNull(ObjectMap map, String key, Object value, boolean parents) {
        if (value != null) {
            map.putNested(key, value, parents);
        }
        return null;
    }

    public Object putNestedIfNotEmpty(ObjectMap map, String key, String value, boolean parents) {
        if (StringUtils.isNotEmpty(value)) {
            map.putNested(key, value, parents);
        }
        return null;
    }

    public void putNestedMapIfNotEmpty(ObjectMap map, String key, Map<String, ?> value, boolean parents) {
        if (MapUtils.isNotEmpty(value)) {
            map.putNested(key, value, parents);
        }
    }

    public boolean checkExpiredSession(String[] args) {
        ObjectMap claimsMap;
        try {
            claimsMap = parseTokenClaims(sessionManager.getSession().getToken());
            logger.debug("ClaimsMap: {}", claimsMap.toJson());
        } catch (Exception e) {
            return ArrayUtils.contains(args, "login") || ArrayUtils.contains(args, "logout") || "anonymous".equals(sessionManager.getSession().getUser());
        }
        Date expirationDate;
        Date currentDate = new Date();
        if (claimsMap.containsKey("exp")) {
            expirationDate = new Date(claimsMap.getLong("exp") * 1000L);
        } else {
            logger.debug("Token has no expiration time");
            // No expiration so adding 1 year to result
            Calendar instance = Calendar.getInstance();
            instance.setTime(currentDate);
            instance.add(Calendar.YEAR, 1);
            expirationDate = instance.getTime();
        }
        return currentDate.before(expirationDate) || ArrayUtils.contains(args, "login") || ArrayUtils.contains(args, "logout") || "anonymous".equals(sessionManager.getSession().getUser());
    }


    public void putNestedMapIfNotEmpty(ObjectMap map, String key, Map<String, ?> value, boolean parents) {
        if (MapUtils.isNotEmpty(value)) {
            map.putNested(key, value, parents);
        }
    }

}
