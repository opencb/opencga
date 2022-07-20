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
import org.opencb.opencga.core.models.user.User;

import org.opencb.commons.annotations.DataField;
import org.opencb.opencga.core.api.ParamConstants;

public class UserCreateParams extends org.opencb.opencga.core.models.user.UserCreateParams {

    @DataField(description = ParamConstants.USER_CREATE_PARAMS_TYPE_DESCRIPTION)
    private Account.AccountType type;

    public UserCreateParams() {
    }

    public UserCreateParams(String id, String name, String email, String password, String organization, Account.AccountType type) {
        super(id, name, email, password, organization);
        this.type = type;
    }

    public static UserCreateParams of(User user) {
        return new UserCreateParams(user.getId(), user.getName(), user.getEmail(), "", user.getOrganization(),
                user.getAccount().getType());
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserCreateParams{");
        sb.append("type=").append(type);
        sb.append('}');
        return sb.toString();
    }

    public Account.AccountType getType() {
        return type;
    }

    public UserCreateParams setType(Account.AccountType type) {
        this.type = type;
        return this;
    }
}
