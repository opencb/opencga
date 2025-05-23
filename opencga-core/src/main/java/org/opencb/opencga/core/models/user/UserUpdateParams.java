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
import org.opencb.opencga.core.models.notification.NotificationConfiguration;

public class UserUpdateParams {

    @DataField(id = "name", indexed = true, description = FieldConstants.USER_NAME)
    private String name;

    @DataField(id = "email", indexed = true, description = FieldConstants.USER_EMAIL)
    private String email;

    @DataField(id = "notifications", description = FieldConstants.USER_NOTIFICATIONS)
    private NotificationConfiguration notifications;

    public UserUpdateParams() {
    }

    public UserUpdateParams(String name, String email, NotificationConfiguration notifications) {
        this.name = name;
        this.email = email;
        this.notifications = notifications;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("UserUpdateParams{");
        sb.append("name='").append(name).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", notifications=").append(notifications);
        sb.append('}');
        return sb.toString();
    }

    public String getName() {
        return name;
    }

    public UserUpdateParams setName(String name) {
        this.name = name;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public UserUpdateParams setEmail(String email) {
        this.email = email;
        return this;
    }

    public NotificationConfiguration getNotifications() {
        return notifications;
    }

    public UserUpdateParams setNotifications(NotificationConfiguration notifications) {
        this.notifications = notifications;
        return this;
    }
}
