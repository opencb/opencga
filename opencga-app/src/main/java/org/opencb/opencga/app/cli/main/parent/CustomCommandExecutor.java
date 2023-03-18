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

package org.opencb.opencga.app.cli.main.parent;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.session.SessionManager;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.client.rest.OpenCGAClient;
import org.slf4j.Logger;

public class CustomCommandExecutor {

    protected ObjectMap options;
    protected String token;
    protected ClientConfiguration clientConfiguration;
    protected SessionManager session;
    protected Logger logger;
    protected OpenCGAClient openCGAClient;

    public CustomCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                 SessionManager session, Logger logger) {
        this(options, token, clientConfiguration, session, logger, null);
    }

    public CustomCommandExecutor(ObjectMap options, String token, ClientConfiguration clientConfiguration,
                                 SessionManager session, Logger logger, OpenCGAClient openCGAClient) {
        this.options = options;
        this.token = token;
        this.clientConfiguration = clientConfiguration;
        this.session = session;
        this.logger = logger;
        this.openCGAClient = openCGAClient;

        this.init();
    }

    private void init() {
        if (openCGAClient == null) {
            logger.debug("Initialising OpenCGAClient");
            openCGAClient = new OpenCGAClient(this.clientConfiguration);
            openCGAClient.setToken(this.token);
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("CustomExecutor{");
        sb.append("options=").append(options);
        sb.append(", token='").append(token).append('\'');
        sb.append(", clientConfiguration=").append(clientConfiguration);
        sb.append(", session=").append(session);
        sb.append(", openCGAClient=").append(openCGAClient);
        sb.append('}');
        return sb.toString();
    }

    public ObjectMap getOptions() {
        return options;
    }

    public CustomCommandExecutor setOptions(ObjectMap options) {
        this.options = options;
        return this;
    }

    public String getToken() {
        return token;
    }

    public CustomCommandExecutor setToken(String token) {
        this.token = token;
        return this;
    }

    public ClientConfiguration getClientConfiguration() {
        return clientConfiguration;
    }

    public CustomCommandExecutor setClientConfiguration(ClientConfiguration clientConfiguration) {
        this.clientConfiguration = clientConfiguration;
        return this;
    }

    public SessionManager getSession() {
        return session;
    }

    public CustomCommandExecutor setSession(SessionManager session) {
        this.session = session;
        return this;
    }

    public Logger getLogger() {
        return logger;
    }

    public CustomCommandExecutor setLogger(Logger logger) {
        this.logger = logger;
        return this;
    }

    public OpenCGAClient getOpenCGAClient() {
        return openCGAClient;
    }

    public CustomCommandExecutor setOpenCGAClient(OpenCGAClient openCGAClient) {
        this.openCGAClient = openCGAClient;
        return this;
    }
}
