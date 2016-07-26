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

    public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonCommandOptions;

    private AclsCommandOptions aclsCommandOptions;
    private AclsCreateCommandOptions aclsCreateCommandOptions;
    private AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    private AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    private AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;

    private String xxx;

    public AclCommandOptions(OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonCommandOptions) {
        this.commonCommandOptions = commonCommandOptions;

        this.aclsCommandOptions = new AclsCommandOptions();
        this.aclsCreateCommandOptions = new AclsCreateCommandOptions();
        this.aclsMemberDeleteCommandOptions = new AclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = new AclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = new AclsMemberUpdateCommandOptions();

    }

//    public class BaseAclCommand {
//
//        @ParametersDelegate
//        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = commonCommandOptions;
//
//        @Parameter(names = {"--id"}, description = "Id of ...", required = true, arity = 1)
//        public String id;
//    }

    @Parameters(commandNames = {"acl"}, commandDescription = "Return the acl of the sample [PENDING]")
    public class AclsCommandOptions {

        @ParametersDelegate
        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Id of ...", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"acl-create"}, commandDescription = "Define a set of permissions for a list of users or groups [PENDING]")
    public class AclsCreateCommandOptions extends AclsCommandOptions {

        @Parameter(names = {"--members"},
                description = "Comma separated list of members. Accepts: '{userId}', '@{groupId}' or '*'", required = true, arity = 1)
        public String members;

        @Parameter(names = {"--permissions"}, description = "Comma separated list of cohort permissions", required = true, arity = 1)
        public String permissions;

        @Parameter(names = {"--template-id"}, description = "Template of permissions to be used (admin, analyst or locked)",
                required = false, arity = 1)
        public String templateId;
    }

    @Parameters(commandNames = {"acl-member-delete"},
            commandDescription = "Delete all the permissions granted for the user or group [PENDING]")
    public class AclsMemberDeleteCommandOptions extends AclsCommandOptions {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-info"},
            commandDescription = "Return the set of permissions granted for the user or group [PENDING]")
    public class AclsMemberInfoCommandOptions extends AclsCommandOptions {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-update"},
            commandDescription = "Update the set of permissions granted for the user or group [PENDING]")
    public class AclsMemberUpdateCommandOptions extends AclsCommandOptions {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;

        @Parameter(names = {"--add-permissions"}, description = "Comma separated list of permissions to add", required = false, arity = 1)
        public String addPermissions;

        @Parameter(names = {"--remove-permissions"}, description = "Comma separated list of permissions to remove",
                required = false, arity = 1)
        public String removePermissions;

        @Parameter(names = {"--set-permissions"}, description = "Comma separated list of permissions to set", required = false, arity = 1)
        public String setPermissions;
    }

    public AclsCommandOptions getAclsCommandOptions() {
        return aclsCommandOptions;
    }

    public AclsCreateCommandOptions getAclsCreateCommandOptions() {
        return aclsCreateCommandOptions;
    }

    public AclsMemberDeleteCommandOptions getAclsMemberDeleteCommandOptions() {
        return aclsMemberDeleteCommandOptions;
    }

    public AclsMemberInfoCommandOptions getAclsMemberInfoCommandOptions() {
        return aclsMemberInfoCommandOptions;
    }

    public AclsMemberUpdateCommandOptions getAclsMemberUpdateCommandOptions() {
        return aclsMemberUpdateCommandOptions;
    }
}
