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

import org.opencb.opencga.core.models.user.Account;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class GroupSyncParams {

    @DataField(description = ParamConstants.GROUP_SYNC_PARAMS_AUTHENTICATION_ORIGIN_ID_DESCRIPTION)
    private String authenticationOriginId;
    @DataField(description = ParamConstants.GROUP_SYNC_PARAMS_FROM_DESCRIPTION)
    private String from;
    @DataField(description = ParamConstants.GROUP_SYNC_PARAMS_TO_DESCRIPTION)
    private String to;
    @DataField(description = ParamConstants.GROUP_SYNC_PARAMS_STUDY_DESCRIPTION)
    private String study;
    @DataField(description = ParamConstants.GROUP_SYNC_PARAMS_SYNC_ALL_DESCRIPTION)
    private boolean syncAll;
    @DataField(description = ParamConstants.GROUP_SYNC_PARAMS_TYPE_DESCRIPTION)
    private Account.AccountType type;
    @DataField(description = ParamConstants.GROUP_SYNC_PARAMS_FORCE_DESCRIPTION)
    private boolean force;

    public GroupSyncParams() {
    }

    public GroupSyncParams(String authenticationOriginId, String from, String to, String study, boolean syncAll, Account.AccountType type,
                           boolean force) {
        this.authenticationOriginId = authenticationOriginId;
        this.from = from;
        this.to = to;
        this.study = study;
        this.syncAll = syncAll;
        this.type = type;
        this.force = force;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GroupSyncParams{");
        sb.append("authenticationOriginId='").append(authenticationOriginId).append('\'');
        sb.append(", from='").append(from).append('\'');
        sb.append(", to='").append(to).append('\'');
        sb.append(", study='").append(study).append('\'');
        sb.append(", syncAll=").append(syncAll);
        sb.append(", type=").append(type);
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

    public boolean isSyncAll() {
        return syncAll;
    }

    public GroupSyncParams setSyncAll(boolean syncAll) {
        this.syncAll = syncAll;
        return this;
    }

    public Account.AccountType getType() {
        return type;
    }

    public GroupSyncParams setType(Account.AccountType type) {
        this.type = type;
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
