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
import org.opencb.opencga.app.cli.main.OpencgaMain;
import org.opencb.opencga.app.cli.main.io.*;
import org.opencb.opencga.app.cli.session.CliSession;
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
            CommandLineUtils.printDebug("init OpencgaCommandExecutor ");
            CliSession.getInstance().init();
            CommandLineUtils.printDebug("TOKEN::::: " + CliSessionManager.getInstance().getToken());
            WriterConfiguration writerConfiguration = new WriterConfiguration();
            writerConfiguration.setMetadata(options.metadata);
            writerConfiguration.setHeader(!options.noHeader);

            switch (options.outputFormat.toLowerCase()) {
                case "json_pretty":
                    writerConfiguration.setPretty(true);
                case "json":
                    this.writer = new JsonOutputWriter(writerConfiguration);
                    break;
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
                CommandLineUtils.printDebug("CLI TOKEN ");
                CliSessionManager.getInstance().updateSessionToken(options.token);
                token = options.token;
                userId = null;
                openCGAClient = new OpenCGAClient(new AuthenticationResponse(options.token), clientConfiguration);
            } else {
                // 'logout' field is only null or empty while no logout is executed
                if (StringUtils.isNotEmpty(CliSessionManager.getInstance().getToken())) {
                    // no timeout checks
                    if (skipDuration) {
                        CommandLineUtils.printDebug("skipDuration");
                        openCGAClient = new OpenCGAClient(
                                new AuthenticationResponse(CliSessionManager.getInstance().getToken()
                                        , CliSessionManager.getInstance().getRefreshToken())
                                , clientConfiguration);
                        openCGAClient.setUserId(CliSessionManager.getInstance().getUser());
                        if (options.token == null) {
                            options.token = CliSessionManager.getInstance().getToken();
                        }
                    } else {
                        CommandLineUtils.printDebug("skipDuration FALSE");

                        // Get the expiration of the token stored in the session file
                        String myClaims = StringUtils.split(CliSessionManager.getInstance().getToken(), ".")[1];
                        String decodedClaimsString = new String(Base64.getDecoder().decode(myClaims), StandardCharsets.UTF_8);
                        ObjectMap claimsMap = new ObjectMapper().readValue(decodedClaimsString, ObjectMap.class);

                        Date expirationDate = new Date(claimsMap.getLong("exp") * 1000L);

                        Date currentDate = new Date();

                        if (currentDate.before(expirationDate) || !claimsMap.containsKey("exp")) {
                            CommandLineUtils.printDebug("Session ok!!");
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

                            CommandLineUtils.printDebug("session has expired ");
                            if (OpencgaMain.isShell()) {
                                throw new CatalogAuthenticationException("Your session has expired. Please, either login again.");
                            } else {
                                PrintUtils.printError("Your session has expired. Please, either login again or logout to work as "
                                        + "anonymous.");
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
        CommandLineUtils.printDebug("openCGAClient " + openCGAClient);

    }

    public void createOutput(RestResponse queryResponse) {
        if (queryResponse != null) {
            writer.print(queryResponse);
        }
    }

    public void invokeSetter(Object obj, String propertyName, Object variableValue) {
        if (variableValue != null) {
            try {
                Method setter = obj.getClass().getMethod(getAsSetterName(propertyName), variableValue.getClass());
                setter.invoke(obj, variableValue);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private String getAsSetterName(String propertyName) {
        return "set" + upperCaseFirst(propertyName);
    }

    public String upperCaseFirst(String val) {
        char[] arr = val.toCharArray();
        arr[0] = Character.toUpperCase(arr[0]);
        return new String(arr);
    }

    public OpenCGAClient getOpenCGAClient() {
        return openCGAClient;
    }

    public OpencgaCommandExecutor setOpenCGAClient(OpenCGAClient openCGAClient) {
        this.openCGAClient = openCGAClient;
        return this;
    }
}
