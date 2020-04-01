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

package org.opencb.opencga.core.config;

/**
 * Created by imedina on 25/04/16.
 */
public abstract class AbstractServerConfiguration {

    protected int port;
    protected String logFile;

    public AbstractServerConfiguration() {
    }

    public AbstractServerConfiguration(int port) {
        this.port = port;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("ServerConfiguration{");
        sb.append("port=").append(port);
        sb.append(", logFile='").append(logFile).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public int getPort() {
        return port;
    }

    public AbstractServerConfiguration setPort(int port) {
        this.port = port;
        return this;
    }

    public String getLogFile() {
        return logFile;
    }

    public AbstractServerConfiguration setLogFile(String logFile) {
        this.logFile = logFile;
        return this;
    }

}
