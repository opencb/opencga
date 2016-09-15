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

package org.opencb.opencga.catalog.models;

import org.opencb.commons.utils.StringUtils;
import org.opencb.opencga.core.common.TimeUtils;


/**
 * Created by jacobo on 11/09/14.
 */
public class Session {

    private String id;
    private String ip;
    private String login;
    private String logout;  //Empty string if still login

    public Session() {
        this(StringUtils.randomString(20), "", TimeUtils.getTime(), "");
    }

    public Session(String ip, int length) {
        this(StringUtils.randomString(length), ip, TimeUtils.getTime(), "");
    }

    public Session(String id, String ip, String logout) {
        this(StringUtils.randomString(20), ip, TimeUtils.getTime(), logout);
    }

    public Session(String id, String ip, String login, String logout) {
        this.id = id;
        this.ip = ip;
        this.login = login;
        this.logout = logout;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Session{");
        sb.append("id='").append(id).append('\'');
        sb.append(", ip='").append(ip).append('\'');
        sb.append(", login='").append(login).append('\'');
        sb.append(", logout='").append(logout).append('\'');
        sb.append('}');
        return sb.toString();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    public String getLogout() {
        return logout;
    }

    public void setLogout(String logout) {
        this.logout = logout;
    }

    public String generateNewId(int length) {
        this.id = StringUtils.randomString(length);
        return id;
    }
}
