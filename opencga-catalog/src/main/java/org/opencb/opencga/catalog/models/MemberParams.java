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
 * Created by pfurio on 13/07/17.
 */
public class MemberParams {

    private String users;
    private Action action;

    public enum Action {
        ADD,
        REMOVE
    }

    public MemberParams() {
    }

    public MemberParams(String users, Action action) {
        this.users = users;
        this.action = action;
    }

    public String getUsers() {
        return users;
    }

    public MemberParams setUsers(String users) {
        this.users = users;
        return this;
    }

    public Action getAction() {
        return action;
    }

    public MemberParams setAction(Action action) {
        this.action = action;
        return this;
    }

    public GroupParams toGroupParams() {
        return new GroupParams(this.users, GroupParams.Action.valueOf(this.action.name()));
    }

}
