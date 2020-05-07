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

package org.opencb.opencga.core.models.admin;

public class GroupSyncParams {

    private String authenticationOriginId;
    private String from;
    private String to;
    private String study;
    private boolean force;

    public GroupSyncParams() {
    }

    public GroupSyncParams(String authenticationOriginId, String from, String to, String study, boolean force) {
        this.authenticationOriginId = authenticationOriginId;
        this.from = from;
        this.to = to;
        this.study = study;
        this.force = force;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GroupSyncParams{");
        sb.append("authenticationOriginId='").append(authenticationOriginId).append('\'');
        sb.append(", from='").append(from).append('\'');
        sb.append(", to='").append(to).append('\'');
        sb.append(", study='").append(study).append('\'');
        sb.append(", force=").append(force);
        sb.append('}');
        return sb.toString();
    }

    public String getAuthenticationOriginId() {
        return authenticationOriginId;
    }

    public GroupSyncParams setAuthenticationOriginId(String authenticationOriginId) {
        this.authenticationOriginId = authenticationOriginId;
        return this;
    }

    public String getFrom() {
        return from;
    }

    public GroupSyncParams setFrom(String from) {
        this.from = from;
        return this;
    }

    public String getTo() {
        return to;
    }

    public GroupSyncParams setTo(String to) {
        this.to = to;
        return this;
    }

    public String getStudy() {
        return study;
    }

    public GroupSyncParams setStudy(String study) {
        this.study = study;
        return this;
    }

    public boolean isForce() {
        return force;
    }

    public GroupSyncParams setForce(boolean force) {
        this.force = force;
        return this;
    }
}
