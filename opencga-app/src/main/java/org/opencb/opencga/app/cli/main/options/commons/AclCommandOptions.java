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

package org.opencb.opencga.app.cli.main.options.commons;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;

/**
 * Created by imedina on 26/07/16.
 */
public class AclCommandOptions {

    private OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonCommandOptions;

    private AclsCommandOptions aclsCommandOptions;
    private AclsCreateCommandOptions aclsCreateCommandOptions;
    private AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    private AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    private AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;

    public AclCommandOptions(OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonCommandOptions) {
        this.commonCommandOptions = commonCommandOptions;
    }

    @Parameters(commandNames = {"acl"}, commandDescription = "Return the acl of the resource")
    public class AclsCommandOptions {

        @ParametersDelegate
        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Id of the resource", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"acl-create"}, commandDescription = "Define a set of permissions for a list of users or groups")
    public class AclsCreateCommandOptions extends AclsCommandOptions {
        @Parameter(names = {"--members"},
                description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true, arity = 1)
        public String members;

        @Parameter(names = {"--permissions"}, description = "Comma separated list of accepted permissions for the resource", arity = 1)
        public String permissions;
    }

    public class AclsCreateCommandOptionsTemplate extends AclsCreateCommandOptions {
        @Parameter(names = {"--template-id"}, description = "Template of permissions to be used (admin, analyst or locked)", arity = 1)
        public String templateId;
    }

    @Parameters(commandNames = {"acl-member-delete"}, commandDescription = "Delete all the permissions granted for the user or group")
    public class AclsMemberDeleteCommandOptions extends AclsCommandOptions {

        @Parameter(names = {"--member-id"}, description = "Member id ('{userId}', '@{groupId}' or '*')", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-info"},
            commandDescription = "Return the set of permissions granted for the user or group")
    public class AclsMemberInfoCommandOptions extends AclsCommandOptions {

        @Parameter(names = {"--member-id"}, description = "Member id  ('{userId}', '@{groupId}' or '*')", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-update"},
            commandDescription = "Update the set of permissions granted for the user or group")
    public class AclsMemberUpdateCommandOptions extends AclsCommandOptions {

        @Parameter(names = {"--member-id"}, description = "Member id  ('{userId}', '@{groupId}' or '*')", required = true, arity = 1)
        public String memberId;

        @Parameter(names = {"--add-permissions"}, description = "Comma separated list of permissions to add", arity = 1)
        public String addPermissions;

        @Parameter(names = {"--remove-permissions"}, description = "Comma separated list of permissions to remove", arity = 1)
        public String removePermissions;

        @Parameter(names = {"--set-permissions"}, description = "Comma separated list of permissions to set", arity = 1)
        public String setPermissions;
    }

    public AclsCommandOptions getAclsCommandOptions() {
        if (this.aclsCommandOptions == null) {
            this.aclsCommandOptions = new AclsCommandOptions();
        }
        return aclsCommandOptions;
    }

    public AclsCreateCommandOptions getAclsCreateCommandOptions() {
        if (this.aclsCreateCommandOptions == null) {
            this.aclsCreateCommandOptions = new AclsCreateCommandOptions();
        }
        return aclsCreateCommandOptions;
    }

    public AclsCreateCommandOptionsTemplate getAclsCreateCommandOptionsTemplate() {
        if (this.aclsCreateCommandOptions == null) {
            this.aclsCreateCommandOptions = new AclsCreateCommandOptionsTemplate();
        }
        return ((AclsCreateCommandOptionsTemplate) aclsCreateCommandOptions);
    }

    public AclsMemberDeleteCommandOptions getAclsMemberDeleteCommandOptions() {
        if (this.aclsMemberDeleteCommandOptions == null) {
            this.aclsMemberDeleteCommandOptions = new AclsMemberDeleteCommandOptions();
        }
        return aclsMemberDeleteCommandOptions;
    }

    public AclsMemberInfoCommandOptions getAclsMemberInfoCommandOptions() {
        if (this.aclsMemberInfoCommandOptions == null) {
            this.aclsMemberInfoCommandOptions = new AclsMemberInfoCommandOptions();
        }
        return aclsMemberInfoCommandOptions;
    }

    public AclsMemberUpdateCommandOptions getAclsMemberUpdateCommandOptions() {
        if (this.aclsMemberUpdateCommandOptions == null) {
            this.aclsMemberUpdateCommandOptions = new AclsMemberUpdateCommandOptions();
        }
        return aclsMemberUpdateCommandOptions;
    }
}
