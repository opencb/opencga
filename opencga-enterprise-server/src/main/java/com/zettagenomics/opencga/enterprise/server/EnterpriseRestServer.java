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

package com.zettagenomics.opencga.enterprise.server;

import com.zettagenomics.opencga.enterprise.core.configuration.EnterpriseConfiguration;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.webapp.WebAppContext;
import org.jasig.cas.client.configuration.ConfigurationKeys;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.server.RestServer;

import javax.servlet.DispatcherType;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by imedina on 02/01/16.
 */
public class EnterpriseRestServer extends RestServer {

    private final EnterpriseConfiguration enterpriseConfiguration;

    public EnterpriseRestServer(Path opencgaHome, int port) throws IOException {
        super(opencgaHome, port);
        enterpriseConfiguration = EnterpriseConfiguration.load(opencgaHome);
    }

    @Override
    protected WebAppContext initWebApp(Path warPath) throws Exception {
        WebAppContext webAppContext = super.initWebApp(warPath);
        addSingleSignOnFilters(webAppContext);
        return webAppContext;
    }

    private void addSingleSignOnFilters(WebAppContext webapp) throws Exception {
        if (enterpriseConfiguration.getSso() != null && enterpriseConfiguration.getSso().isActive()) {
            // Check all mandatory fields
            ParamUtils.checkParameter(enterpriseConfiguration.getSso().getCasServerPrefixUrl(), "sso.casServerPrefixUrl");
            ParamUtils.checkParameter(enterpriseConfiguration.getSso().getServerName(), "sso.serverName");
            ParamUtils.checkParameter(enterpriseConfiguration.getSso().getProtocol(), "sso.protocol");
            Map<String, String> initParameters = enterpriseConfiguration.getSso().getInitParameters() != null
                    ? enterpriseConfiguration.getSso().getInitParameters() : Collections.emptyMap();

            switch (enterpriseConfiguration.getSso().getProtocol().toUpperCase()) {
                case "CAS":
                    logger.info("Using CAS protocol");
                    // Start CAS protocol configuration
                    FilterHolder casValidationFilterHolder = new FilterHolder();
                    casValidationFilterHolder.setName("CAS Validation Filter");
                    casValidationFilterHolder.setClassName("org.jasig.cas.client.validation.Cas20ProxyReceivingTicketValidationFilter");
                    Map<String, String> casInitParameters = new HashMap<>();
                    casInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), enterpriseConfiguration.getSso().getCasServerPrefixUrl());
                    casInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), enterpriseConfiguration.getSso().getServerName());
                    casInitParameters.putAll(initParameters);
                    casValidationFilterHolder.setInitParameters(casInitParameters);
                    webapp.addFilter(casValidationFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));

                    FilterHolder casAuthenticationFilterHolder = new FilterHolder();
                    casAuthenticationFilterHolder.setName("CAS Authentication Filter");
//                    casAuthenticationFilterHolder.setClassName("org.jasig.cas.client.authentication.AuthenticationFilter");
                    casAuthenticationFilterHolder.setClassName("com.zettagenomics.opencga.enterprise.server.sso.OpencgaAuthenticationFilter");
                    casInitParameters = new HashMap<>();
                    casInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), enterpriseConfiguration.getSso().getCasServerPrefixUrl());
                    casInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), enterpriseConfiguration.getSso().getServerName());
                    casInitParameters.putAll(initParameters);
                    casAuthenticationFilterHolder.setInitParameters(casInitParameters);
                    webapp.addFilter(casAuthenticationFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));

                    FilterHolder requestWrapperFilterHolder = new FilterHolder();
                    requestWrapperFilterHolder.setName("CAS HttpServletRequest Wrapper Filter");
                    requestWrapperFilterHolder.setClassName("org.jasig.cas.client.util.HttpServletRequestWrapperFilter");
                    casInitParameters = new HashMap<>();
                    casInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), enterpriseConfiguration.getSso().getCasServerPrefixUrl());
                    casInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), enterpriseConfiguration.getSso().getServerName());
                    casInitParameters.putAll(initParameters);
                    requestWrapperFilterHolder.setInitParameters(casInitParameters);
                    webapp.addFilter(requestWrapperFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));
                    // End of CAS configuration
                    break;
                case "SAML1":
                    logger.info("Using SAML1 protocol");
                    // Start SAML1 protocol configuration
                    FilterHolder samlValidationFilterHolder = new FilterHolder();
                    samlValidationFilterHolder.setName("CAS Validation Filter");
                    samlValidationFilterHolder.setClassName("org.jasig.cas.client.validation.Saml11TicketValidationFilter");
                    Map<String, String> samlInitParameters = new HashMap<>();
                    samlInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), enterpriseConfiguration.getSso().getCasServerPrefixUrl());
                    samlInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), enterpriseConfiguration.getSso().getServerName());
                    samlInitParameters.putAll(initParameters);
                    samlValidationFilterHolder.setInitParameters(samlInitParameters);
                    webapp.addFilter(samlValidationFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));

                    FilterHolder samlAuthenticationFilterHolder = new FilterHolder();
                    samlAuthenticationFilterHolder.setName("CAS Authentication Filter");
//                    samlAuthenticationFilterHolder.setClassName("org.jasig.cas.client.authentication.Saml11AuthenticationFilter");
                    samlAuthenticationFilterHolder.setClassName("com.zettagenomics.opencga.enterprise.server.sso.Saml11OpencgaAuthenticationFilter");
                    samlInitParameters = new HashMap<>();
                    samlInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), enterpriseConfiguration.getSso().getCasServerPrefixUrl());
                    samlInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), enterpriseConfiguration.getSso().getServerName());
                    samlInitParameters.putAll(initParameters);
                    samlAuthenticationFilterHolder.setInitParameters(samlInitParameters);
                    webapp.addFilter(samlAuthenticationFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));

                    FilterHolder saml1RequestWrapperFilterHolder = new FilterHolder();
                    saml1RequestWrapperFilterHolder.setName("CAS HttpServletRequest Wrapper Filter");
                    saml1RequestWrapperFilterHolder.setClassName("org.jasig.cas.client.util.HttpServletRequestWrapperFilter");
                    samlInitParameters = new HashMap<>();
                    samlInitParameters.put(ConfigurationKeys.CAS_SERVER_URL_PREFIX.getName(), enterpriseConfiguration.getSso().getCasServerPrefixUrl());
                    samlInitParameters.put(ConfigurationKeys.SERVER_NAME.getName(), enterpriseConfiguration.getSso().getServerName());
                    samlInitParameters.putAll(initParameters);
                    saml1RequestWrapperFilterHolder.setInitParameters(samlInitParameters);
                    webapp.addFilter(saml1RequestWrapperFilterHolder, "/webservices/rest/*", EnumSet.of(DispatcherType.REQUEST));
                    // End of SAML1 configuration
                    break;
                default:
                    throw new Exception("Unsupported protocol '" + enterpriseConfiguration.getSso().getProtocol()
                            + "' found. Supported protocols are 'CAS' and 'SAML1'");
            }
        }
    }

}
