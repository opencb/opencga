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

package org.opencb.opencga.catalog.config;

import org.opencb.opencga.catalog.models.Session;

import java.util.Collections;
import java.util.List;

/**
 * Created by imedina on 18/04/16.
 */
public class Admin {

    private String password;
    private String email;
    private List<Session> sessions;

    public Admin() {
    }

    public Admin(String password, String email) {
        this.password = password;
        this.email = email;
        this.sessions = Collections.emptyList();
    }

    public String getPassword() {
        return password;
    }

    public Admin setPassword(String password) {
        this.password = password;
        return this;
    }

    public String getEmail() {
        return email;
    }

    public Admin setEmail(String email) {
        this.email = email;
        return this;
    }

    public List<Session> getSessions() {
        return sessions;
    }

    public Admin setSessions(List<Session> sessions) {
        this.sessions = sessions;
        return this;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Admin{");
        sb.append("password='").append(password).append('\'');
        sb.append(", email='").append(email).append('\'');
        sb.append(", sessions=").append(sessions);
        sb.append('}');
        return sb.toString();
    }

}
