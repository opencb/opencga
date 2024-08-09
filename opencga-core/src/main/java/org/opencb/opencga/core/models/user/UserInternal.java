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

package org.opencb.opencga.core.models.user;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.FieldConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.common.Internal;

public class UserInternal extends Internal {

    @DataField(id = "status", description = FieldConstants.INTERNAL_STATUS_DESCRIPTION)
    private UserStatus status;

    @DataField(id = "account", since = "3.2.1", description = FieldConstants.USER_ACCOUNT)
    private Account account;

    public UserInternal() {
    }

    public UserInternal(UserStatus status) {
        this(TimeUtils.getTime(), TimeUtils.getTime(), status, new Account());
    }

    public UserInternal(UserStatus status, Account account) {
        this(TimeUtils.getTime(), TimeUtils.getTime(), status, account);
    }

    public UserInternal(String registrationDate, String lastModified, UserStatus status1, Account account) {
        super(null, registrationDate, lastModified);
        this.status = status1;
        this.account = account;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserInternal{");
        sb.append("status=").append(status);
        sb.append(", account=").append(account);
        sb.append(", registrationDate='").append(registrationDate).append('\'');
        sb.append(", lastModified='").append(lastModified).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public UserStatus getStatus() {
        return status;
    }

    public UserInternal setStatus(UserStatus status) {
        this.status = status;
        return this;
    }

    public Account getAccount() {
        return account;
    }

    public UserInternal setAccount(Account account) {
        this.account = account;
        return this;
    }

}
