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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.utils.PrintUtils;
import org.opencb.opencga.app.cli.CommandExecutor;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.app.cli.main.CommandLineUtils;
import org.opencb.opencga.app.cli.main.io.*;
import org.opencb.opencga.app.cli.session.CliSessionManager;
import org.opencb.opencga.catalog.exceptions.CatalogAuthenticationException;
import org.opencb.opencga.client.exceptions.ClientException;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.opencb.opencga.core.models.user.AuthenticationResponse;
import org.opencb.opencga.core.response.RestResponse;

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
public abstract class OpencgaCommandExecutor extends CommandExecutor {

    protected OpenCGAClient openCGAClient;
    protected AbstractOutputWriter writer;

    public OpencgaCommandExecutor(GeneralCliOptions.CommonCommandOptions options) throws CatalogAuthenticationException {
        this(options, false);
    }

    @Deprecated
    public OpencgaCommandExecutor(GeneralCliOptions.CommonCommandOptions options, boolean skipDuration) throws CatalogAuthenticationException {
        super(options, true);

        init(options, skipDuration);
    }

    public static List<String> splitWithTrim(String value) {
        return splitWithTrim(value, ",");
    }

    public static List<String> splitWithTrim(String value, String separator) {
        String[] splitFields = value.split(separator);
        List<String> result = new ArrayList<>(splitFields.length);
        for (String s : splitFields) {
            result.add(s.trim());
        }
        return result;
    }

    private void init(GeneralCliOptions.CommonCommandOptions options, boolean skipDuration) throws CatalogAuthenticationException {
        try {
            logger.debug("init OpencgaCommandExecutor ");
            CommandLineUtils.printDebug("TOKEN::::: " + CliSessionManager.getInstance().getToken());
            if (options.host != null) {
                CommandLineUtils.printDebug("Switching host to ::::: " + options.host);
                clientConfiguration.setDefaultIndexByName(options.host);
                CommandLineUtils.printDebug("Switched default index to ::::: " + clientConfiguration.getRest().getDefaultHostIndex());
            }
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

            CommandLineUtils.printDebug("CurrentFile::::: " + CliSessionManager.getInstance().getCurrentFile());
            logger.debug("sessionFile = " + CliSessionManager.getInstance().getCurrentFile());
            if (StringUtils.isNotEmpty(options.token)) {
                // Ignore session file. Overwrite with command line information (just sessionId)
                logger.debug("A token has been provided");
                CliSessionManager.getInstance().updateSessionToken(options.token, this);
                token = options.token;
                userId = null;
                openCGAClient = new OpenCGAClient(new AuthenticationResponse(options.token), clientConfiguration);
            } else {
                if (StringUtils.isNotEmpty(CliSessionManager.getInstance().getToken())) {
                    // FIXME it seems skipDuration is not longer used,
                    //  this should be either implemented or removed
                    if (skipDuration) {
                        logger.debug("Skip duration set to {}, THIS MUST BE REMOVED", skipDuration);
                        openCGAClient = new OpenCGAClient(
                                new AuthenticationResponse(CliSessionManager.getInstance().getToken()
                                        , CliSessionManager.getInstance().getRefreshToken())
                                , clientConfiguration);
                        openCGAClient.setUserId(CliSessionManager.getInstance().getUser());
                        if (options.token == null) {
                            options.token = CliSessionManager.getInstance().getToken();
                        }
                    } else {
                        logger.debug("Skip duration set to {}", skipDuration);

                        // Get the expiration of the token stored in the session file
                        String myClaims = StringUtils.split(CliSessionManager.getInstance().getToken(), ".")[1];
                        String decodedClaimsString = new String(Base64.getDecoder().decode(myClaims), StandardCharsets.UTF_8);
                        ObjectMap claimsMap = new ObjectMapper().readValue(decodedClaimsString, ObjectMap.class);

                        Date expirationDate = new Date(claimsMap.getLong("exp") * 1000L);
                        Date currentDate = new Date();
                        // Check if session has expired
                        if (currentDate.before(expirationDate) || !claimsMap.containsKey("exp")) {
                            logger.debug("Session expiration is fine, valid until: {}", expirationDate);
                            openCGAClient = new OpenCGAClient(
                                    new AuthenticationResponse(CliSessionManager.getInstance().getToken(),
                                            CliSessionManager.getInstance().getRefreshToken()),
                                    clientConfiguration);
                            openCGAClient.setUserId(CliSessionManager.getInstance().getUser());

                            // Update token
                            if (clientConfiguration.getRest().isTokenAutoRefresh() && claimsMap.containsKey("exp")) {
                                AuthenticationResponse refreshResponse = openCGAClient.refresh();
                                // FIXME we need to discuss this
//                                CliSessionManager.getInstance().updateTokens(refreshResponse.getToken(), refreshResponse.getRefreshToken());
                            }

                            if (options.token == null) {
                                options.token = CliSessionManager.getInstance().getToken();
                            }
                        } else {
                            logger.debug("Session has expired: {}", expirationDate);
                            if (CliSessionManager.getInstance().isShellMode()) {
                                throw new CatalogAuthenticationException("Your session has expired. Please, either login again.");
                            } else {
                                PrintUtils.printError("Your session has expired. "
                                        + "Please, either login again or logout to work as anonymous.");
                                System.exit(1);
                            }
                        }
                    }
                } else {
                    logger.debug("Session already closed");
                    openCGAClient = new OpenCGAClient(clientConfiguration);
                }
            }
        } catch (ClientException | IOException e) {
            CommandLineUtils.printError("OpencgaCommandExecutorError " + e.getMessage(), e);
        }
    }

    public void createOutput(RestResponse queryResponse) {
        if (writer != null && queryResponse != null) {
            writer.print(queryResponse);
        } else {
            logger.error("Null object found: writer set to '{}' and queryResponse set to '{}'", writer, queryResponse);
        }
    }

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

    public OpenCGAClient getOpenCGAClient() {
        return openCGAClient;
    }

    public OpencgaCommandExecutor setOpenCGAClient(OpenCGAClient openCGAClient) {
        this.openCGAClient = openCGAClient;
        return this;
    }
}
