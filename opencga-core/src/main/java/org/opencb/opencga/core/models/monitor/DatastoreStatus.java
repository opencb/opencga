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

package org.opencb.opencga.core.models.monitor;

public class DatastoreStatus {

    private String replicaSet;
    private String host;
    private String role;
    private Status status;
    private String responseTime;
    private String exception;

    public enum Status {
        UP,
        DOWN
    }

    public DatastoreStatus() {
    }

    public DatastoreStatus(String replicaSet, String host, String role, Status status, String responseTime) {
        this.replicaSet = replicaSet;
        this.host = host;
        this.role = role;
        this.status = status;
        this.responseTime = responseTime;
    }

    public DatastoreStatus(String replicaSet, String host, String role, Status status, String responseTime, String exception) {
        this.replicaSet = replicaSet;
        this.host = host;
        this.role = role;
        this.status = status;
        this.responseTime = responseTime;
        this.exception = exception;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("DatastoreStatus{");
        sb.append("replicaSet='").append(replicaSet).append('\'');
        sb.append(", host='").append(host).append('\'');
        sb.append(", role='").append(role).append('\'');
        sb.append(", status=").append(status);
        sb.append(", responseTime='").append(responseTime).append('\'');
        sb.append(", exception='").append(exception).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getReplicaSet() {
        return replicaSet;
    }

    public DatastoreStatus setReplicaSet(String replicaSet) {
        this.replicaSet = replicaSet;
        return this;
    }

    public String getHost() {
        return host;
    }

    public DatastoreStatus setHost(String host) {
        this.host = host;
        return this;
    }

    public String getRole() {
        return role;
    }

    public DatastoreStatus setRole(String role) {
        this.role = role;
        return this;
    }

    public Status getStatus() {
        return status;
    }

    public DatastoreStatus setStatus(Status status) {
        this.status = status;
        return this;
    }

    public String getResponseTime() {
        return responseTime;
    }

    public DatastoreStatus setResponseTime(String responseTime) {
        this.responseTime = responseTime;
        return this;
    }

    public String getException() {
        return exception;
    }

    public DatastoreStatus setException(String exception) {
        this.exception = exception;
        return this;
    }
}
