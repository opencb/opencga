/*
 * Copyright 2015 OpenCB
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

package org.opencb.opencga.core.config;

/**
 * Created by imedina on 28/08/15.
 */
public class MonitorConfiguration {

//    OPENCGA.APP.DAEMON.PORT     = 50391
//    OPENCGA.APP.DAEMON.SLEEP    = 4000
//    OPENCGA.APP.DAEMON.USER     = ${OPENCGA.APP.DAEMON.USER}
//    OPENCGA.APP.DAEMON.PASSWORD = ${OPENCGA.APP.DAEMON.PASSWORD}

    /**
     * Host where the monitor is running, if not provided then 'localhost' will be assumed
     */
    private String host;

    /**
     * Port where the monitor is listen
     */
    private int port;

    /**
     * Monitoring frequency in seconds
     */
    private int sleep;

    private String catalogUser;
    private String catalogPassword;

    private String auditHost;

    public MonitorConfiguration() {
    }

    public MonitorConfiguration(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public int getSleep() {
        return sleep;
    }

    public void setSleep(int sleep) {
        this.sleep = sleep;
    }

    public String getCatalogUser() {
        return catalogUser;
    }

    public void setCatalogUser(String catalogUser) {
        this.catalogUser = catalogUser;
    }

    public String getCatalogPassword() {
        return catalogPassword;
    }

    public void setCatalogPassword(String catalogPassword) {
        this.catalogPassword = catalogPassword;
    }

    public String getAuditHost() {
        return auditHost;
    }

    public void setAuditHost(String auditHost) {
        this.auditHost = auditHost;
    }
}
