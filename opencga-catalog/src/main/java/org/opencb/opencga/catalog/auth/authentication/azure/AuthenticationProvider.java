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

package org.opencb.opencga.catalog.auth.authentication.azure;

import com.microsoft.azure.credentials.ApplicationTokenCredentials;
import com.microsoft.graph.authentication.IAuthenticationProvider;
import com.microsoft.graph.http.IHttpRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class AuthenticationProvider implements IAuthenticationProvider {

    private static ApplicationTokenCredentials applicationTokenCredentials;

    private static final String BEARER = "Bearer ";
    private static final String AUTHORIZATION_HEADER = "Authorization";

    private static final int MAX_ATTEMPTS = 5;
    private static Logger logger;

    {
        logger = LoggerFactory.getLogger(this.getClass());
    }

    public AuthenticationProvider(String clientId, String clientSecret, String tenantId) {
        applicationTokenCredentials = new ApplicationTokenCredentials(clientId, tenantId, clientSecret, null);
    }

    @Override
    public void authenticateRequest(IHttpRequest request) {
        String token;

        int attempt = 1;
        while (attempt < MAX_ATTEMPTS) {
            try {
                token = applicationTokenCredentials.getToken("https://graph.microsoft.com");
                request.addHeader(AUTHORIZATION_HEADER, BEARER + token);
                return;
            } catch (IOException e) {
                logger.warn("Fail to retrieve token from AD, attempt {}: {}", attempt, e.getMessage());
            }
            attempt++;
        }
        logger.error("Could not retrieve token from AD");
    }
}
