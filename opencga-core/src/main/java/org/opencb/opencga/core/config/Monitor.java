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
 * Created by imedina on 18/04/16.
 */
public class Monitor {

    private int daysToRemove;
    private int executionDaemonInterval;
    private int fileDaemonInterval;

    private int port;

    public Monitor() {
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Monitor{");
        sb.append("daysToRemove=").append(daysToRemove);
        sb.append(", executionDaemonInterval=").append(executionDaemonInterval);
        sb.append(", fileDaemonInterval=").append(fileDaemonInterval);
        sb.append(", port=").append(port);
        sb.append('}');
        return sb.toString();
    }

    public int getDaysToRemove() {
        return daysToRemove;
    }

    public Monitor setDaysToRemove(int daysToRemove) {
        this.daysToRemove = daysToRemove;
        return this;
    }

    public int getExecutionDaemonInterval() {
        return executionDaemonInterval;
    }

    public Monitor setExecutionDaemonInterval(int executionDaemonInterval) {
        this.executionDaemonInterval = executionDaemonInterval;
        return this;
    }

    public int getFileDaemonInterval() {
        return fileDaemonInterval;
    }

    public Monitor setFileDaemonInterval(int fileDaemonInterval) {
        this.fileDaemonInterval = fileDaemonInterval;
        return this;
    }

    public int getPort() {
        return port;
    }

    public Monitor setPort(int port) {
        this.port = port;
        return this;
    }
}
