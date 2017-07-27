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

package org.opencb.opencga.core.results;

import java.util.LinkedList;
import java.util.List;

/**
 * Created by pfurio on 18/04/17.
 */
public class LdapImportResult {

    private Input input;
    private Result result;
    private String warningMsg;
    private String errorMsg;

    public LdapImportResult() {
    }

    public LdapImportResult(Input input, Result result) {
        this.input = input;
        this.result = result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("LdapImportResult{");
        sb.append("input=").append(input);
        sb.append(", result=").append(result);
        sb.append('}');
        return sb.toString();
    }

    public Input getInput() {
        return input;
    }

    public LdapImportResult setInput(Input input) {
        this.input = input;
        return this;
    }

    public Result getResult() {
        return result;
    }

    public LdapImportResult setResult(Result result) {
        this.result = result;
        return this;
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public LdapImportResult setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
        return this;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public LdapImportResult setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
        return this;
    }

    public static class Input {

        private List<String> users;
        private String group;
        private String studyGroup;
        private String authOrigin;
        private String type;
        private String study;

        public Input() {
            this(new LinkedList<>(), "", "", "", "", "");
        }

        public Input(List<String> users, String group, String studyGroup, String authOrigin, String type, String study) {
            this.users = users;
            this.group = group;
            this.studyGroup = studyGroup;
            this.authOrigin = authOrigin;
            this.type = type;
            this.study = study;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Input{");
            sb.append("users=").append(users);
            sb.append(", group='").append(group).append('\'');
            sb.append(", studyGroup='").append(studyGroup).append('\'');
            sb.append(", authOrigin='").append(authOrigin).append('\'');
            sb.append(", type='").append(type).append('\'');
            sb.append(", study='").append(study).append('\'');
            sb.append('}');
            return sb.toString();
        }

        public List<String> getUsers() {
            return users;
        }

        public Input setUsers(List<String> users) {
            this.users = users;
            return this;
        }

        public String getGroup() {
            return group;
        }

        public Input setGroup(String group) {
            this.group = group;
            return this;
        }

        public String getStudyGroup() {
            return studyGroup;
        }

        public Input setStudyGroup(String studyGroup) {
            this.studyGroup = studyGroup;
            return this;
        }

        public String getAuthOrigin() {
            return authOrigin;
        }

        public Input setAuthOrigin(String authOrigin) {
            this.authOrigin = authOrigin;
            return this;
        }

        public String getType() {
            return type;
        }

        public Input setType(String type) {
            this.type = type;
            return this;
        }

        public String getStudy() {
            return study;
        }

        public Input setStudy(String study) {
            this.study = study;
            return this;
        }
    }

    public static class SummaryResult {

        private List<String> newUsers;
        private List<String> existingUsers;
        private List<String> nonExistingUsers;
        private int total;

        public SummaryResult() {
            this(new LinkedList<>(), new LinkedList<>(), new LinkedList<>(), 0);
        }

        public SummaryResult(List<String> newUsers, List<String> existingUsers, List<String> nonExistingUsers, int total) {
            this.newUsers = newUsers;
            this.existingUsers = existingUsers;
            this.nonExistingUsers = nonExistingUsers;
            this.total = total;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("UserResult{");
            sb.append("newUsers=").append(newUsers);
            sb.append(", existingUsers=").append(existingUsers);
            sb.append(", nonExistingUsers=").append(nonExistingUsers);
            sb.append(", total=").append(total);
            sb.append('}');
            return sb.toString();
        }

        public List<String> getNewUsers() {
            return newUsers;
        }

        public SummaryResult setNewUsers(List<String> newUsers) {
            this.newUsers = newUsers;
            return this;
        }

        public List<String> getExistingUsers() {
            return existingUsers;
        }

        public SummaryResult setExistingUsers(List<String> existingUsers) {
            this.existingUsers = existingUsers;
            return this;
        }

        public List<String> getNonExistingUsers() {
            return nonExistingUsers;
        }

        public SummaryResult setNonExistingUsers(List<String> nonExistingUsers) {
            this.nonExistingUsers = nonExistingUsers;
            return this;
        }

        public int getTotal() {
            return total;
        }

        public SummaryResult setTotal(int total) {
            this.total = total;
            return this;
        }
    }

    public static class Result {

        private SummaryResult userSummary;
        private List<String> usersInGroup;

        public Result() {
        }

        public Result(SummaryResult userSummary, List<String> usersInGroup) {
            this.userSummary = userSummary;
            this.usersInGroup = usersInGroup;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("Result{");
            sb.append("userSummary=").append(userSummary);
            sb.append(", usersInGroup=").append(usersInGroup);
            sb.append('}');
            return sb.toString();
        }

        public SummaryResult getUserSummary() {
            return userSummary;
        }

        public Result setUserSummary(SummaryResult userSummary) {
            this.userSummary = userSummary;
            return this;
        }

        public List<String> getUsersInGroup() {
            return usersInGroup;
        }

        public Result setUsersInGroup(List<String> usersInGroup) {
            this.usersInGroup = usersInGroup;
            return this;
        }
    }

}
