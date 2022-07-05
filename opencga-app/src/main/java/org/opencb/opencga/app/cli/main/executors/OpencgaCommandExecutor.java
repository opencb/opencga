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

package org.opencb.opencga.app.cli.main.executors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.app.cli.GeneralCliOptions;
import org.opencb.commons.app.cli.main.utils.CommandLineUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.DataModelsUtils;
import org.opencb.opencga.app.cli.main.impl.CommandExecutorImpl;
import org.opencb.opencga.app.cli.main.impl.SessionManagerImpl;
import org.opencb.opencga.app.cli.main.io.*;
import org.opencb.opencga.catalog.db.api.ProjectDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.QueryType;
import org.opencb.opencga.core.response.RestResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.List;

/**
 * Created on 27/05/16.
 *
 * @author imedina
 */
public abstract class OpencgaCommandExecutor extends CommandExecutorImpl {

    protected OpenCGAClient openCGAClient;
    protected AbstractOutputWriter writer;


    private Logger privateLogger;

    public OpencgaCommandExecutor(GeneralCliOptions.CommonCommandOptions options) throws CatalogAuthenticationException {
        this(options, false);
    }

    @Deprecated
    public OpencgaCommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean skipDuration)
            throws CatalogAuthenticationException {
        super(options, true);

        init(options, skipDuration);
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
            privateLogger = LoggerFactory.getLogger(OpencgaCommandExecutor.class);
            privateLogger.debug("Executing OpencgaCommandExecutor 'init' method ...");

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
                openCGAClient = new OpenCGAClient(new AuthenticationResponse(options.token), clientConfiguration);
            } else {
                privateLogger.debug("No token has been provided, reading session file");
                if (!StringUtils.isEmpty(sessionManager.getSession().getToken())
                        && !SessionManagerImpl.NO_TOKEN.equals(sessionManager.getSession().getToken())) {
                    // FIXME it seems skipDuration is not longer used,
                    //  this should be either implemented or removed
                    if (skipDuration) {
                        privateLogger.debug("Skip duration set to {}, THIS MUST BE REMOVED", skipDuration);
//                        openCGAClient = new OpenCGAClient(
//                                new AuthenticationResponse(CliSessionManager.getInstance().getToken()
//                                        , CliSessionManager.getInstance().getRefreshToken())
//                                , clientConfiguration);
//                        openCGAClient.setUserId(CliSessionManager.getInstance().getUser());
                    } else {
                        privateLogger.debug("Skip duration set to {}", skipDuration);

                        // Get token claims
                        ObjectMap claimsMap = parseTokenClaims(sessionManager.getSession().getToken());

                        // Check if session has expired
                        Date expirationDate = new Date(claimsMap.getLong("exp") * 1000L);
                        Date currentDate = new Date();
                        if (currentDate.before(expirationDate) || !claimsMap.containsKey("exp")) {
                            privateLogger.debug("Session expiration time is ok, valid until: {}", expirationDate);
                            openCGAClient = new OpenCGAClient(
                                    new AuthenticationResponse(sessionManager.getSession().getToken(), sessionManager.getSession().getRefreshToken()),
                                    clientConfiguration);
                            openCGAClient.setUserId(sessionManager.getSession().getUser());

                            // FIXME This looks weird, commenting it
//                            if (options.token == null) {
//                                options.token = sessionManager.getToken();
//                            }
                        } else {
                            privateLogger.debug("Session has expired '{}'. Logging out session", expirationDate);
                            sessionManager.logoutSessionFile();
                            openCGAClient = new OpenCGAClient(clientConfiguration);
                        }
                    }
                } else {
                    privateLogger.debug("No valid session found");
                    openCGAClient = new OpenCGAClient(clientConfiguration);
                }
            }

            if (openCGAClient != null) {
                openCGAClient.setThrowExceptionOnError(true);
            }

        } catch (IOException e) {
            logger.error("OpencgaCommandExecutorError", e);
            CommandLineUtils.error("OpencgaCommandExecutorError", e);
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

    public OpenCGAClient getOpenCGAClient() {
        return openCGAClient;
    }

    public OpencgaCommandExecutor setOpenCGAClient(OpenCGAClient openCGAClient) {
        this.openCGAClient = openCGAClient;
        return this;
    }

    public String getObjectAsJSON(Object o) throws Exception {
        String jsonInString = "Data model not found.";
        try {
            jsonInString = DataModelsUtils.dataModelToJsonString(o.getClass());
        } catch (Exception e) {
            CommandLineUtils.error(e);
        }
        return jsonInString;
    }

    public RestResponse<AuthenticationResponse> saveSession(String user, AuthenticationResponse response) throws ClientException, IOException {
        RestResponse<AuthenticationResponse> res = new RestResponse<>();
        if (response != null) {
            List<String> studies = new ArrayList<>();
            logger.debug(response.toString());
            RestResponse<Project> projects = openCGAClient.getProjectClient().search(
                    new ObjectMap(ProjectDBAdaptor.QueryParams.OWNER.key(), user));

            if (projects.getResponses().get(0).getNumResults() == 0) {
                // We try to fetch shared projects and studies instead when the user does not own any project or study
                projects = openCGAClient.getProjectClient().search(new ObjectMap());
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

    public RestResponse<AuthenticationResponse> refreshToken(AuthenticationResponse response) throws ClientException, IOException {
        RestResponse<AuthenticationResponse> res = new RestResponse<>();
        if (response != null) {
            this.sessionManager.refreshSession(response.getRefreshToken(), this.host);
            res.setType(QueryType.VOID);
        }
        return res;
    }
}
