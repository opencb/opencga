/*
 * Copyright 2015-2017 OpenCB
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

/**
 * Created by pfurio on 23/06/17.
 */
public class GroupParams {

    private String users;
    private Action action;

    public enum Action {
        SET,
        ADD,
        REMOVE
    }

    public GroupParams() {
    }

    public GroupParams(String users, Action action) {
        this.users = users;
        this.action = action;
    }

    public String getUsers() {
        return users;
    }

    public GroupParams setUsers(String users) {
        this.users = users;
        return this;
    }

    public Action getAction() {
        return action;
    }

    public GroupParams setAction(Action action) {
        this.action = action;
        return this;
    }
}
