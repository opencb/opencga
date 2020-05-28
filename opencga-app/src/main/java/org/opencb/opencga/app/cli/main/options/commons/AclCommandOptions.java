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

package org.opencb.opencga.app.cli.main.options.commons;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.GeneralCliOptions;
import org.opencb.opencga.catalog.utils.ParamUtils;

/**
 * Created by imedina on 26/07/16.
 */
public class AclCommandOptions {

    private GeneralCliOptions.CommonCommandOptions commonCommandOptions;

    private AclsCommandOptions aclsCommandOptions;
    private AclsUpdateCommandOptions aclsUpdateCommandOptions;

    public AclCommandOptions(GeneralCliOptions.CommonCommandOptions commonCommandOptions) {
        this.commonCommandOptions = commonCommandOptions;
    }

    @Parameters(commandNames = {"acl"}, commandDescription = "Return the acl of the resource")
    public class AclsCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Id of the resource", required = true, arity = 1)
        public String id;

        @Parameter(names = {"-m", "--member"}, description = "Member id  ('userId', '@groupId' or '*'). If provided, only returns acls "
                + "given to the member.", arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-update"}, commandDescription = "Update the permissions set for a member")
    public class AclsUpdateCommandOptions extends GeneralCliOptions.StudyOption {

        @ParametersDelegate
        public GeneralCliOptions.CommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Comma separated list of ids or file containing the list of ids (one per line)", arity = 1)
        public String id;

        @Parameter(names = {"-m", "--member"}, description = "Member id  ('userId', '@groupId' or '*')", required = true, arity = 1)
        public String memberId;

        @Parameter(names = {"-p", "--permissions"}, description = "Comma separated list of accepted permissions for the resource",
                arity = 1)
        public String permissions;

        @Parameter(names = {"-a", "--action"}, description = "Action to be applied with the permissions (SET, ADD, REMOVE or RESET)",
                arity = 1)
        public ParamUtils.AclAction action = ParamUtils.AclAction.SET;

    }

    public AclsCommandOptions getAclsCommandOptions() {
        if (this.aclsCommandOptions == null) {
            this.aclsCommandOptions = new AclsCommandOptions();
        }
        return aclsCommandOptions;
    }

    public AclsUpdateCommandOptions getAclsUpdateCommandOptions() {
        if (this.aclsUpdateCommandOptions == null) {
            this.aclsUpdateCommandOptions = new AclsUpdateCommandOptions();
        }
        return aclsUpdateCommandOptions;
    }

}
