package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

/**
 * Created by sgallego on 6/15/16.
 */
@Parameters(commandNames = {"panels"}, commandDescription = "Panels commands")
public class PanelCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;

    public AclsCommandOptions aclsCommandOptions;
    public AclsCreateCommandOptions aclsCreateCommandOptions;
    public AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    public AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    public AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public PanelCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();


        this.aclsCommandOptions = new AclsCommandOptions();
        this.aclsCreateCommandOptions = new AclsCreateCommandOptions();
        this.aclsMemberDeleteCommandOptions = new AclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = new AclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = new AclsMemberUpdateCommandOptions();

    }

    class BasePanelsCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--panel-id"}, description = "Panel id", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a cohort")
    public class CreateCommandOptions {

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        public String studyId;

        @Parameter(names = {"--name"}, description = "Panel name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"--disease"}, description = "Disease", required = true, arity = 1)
        public String disease;

        @Parameter(names = {"--description"}, description = "Panel description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--genes"}, description = "Genes", required = false, arity = 1)
        public String genes;

        @Parameter(names = {"--regions"}, description = "Regions", required = false, arity = 1)
        public String regions;

        @Parameter(names = {"--variants"}, description = "Variants", required = false, arity = 1)
        public String variants;

    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get cohort information")
    public class InfoCommandOptions extends BasePanelsCommand {
    }
    @Parameters(commandNames = {"acl"}, commandDescription = "Return the acl of the panel [PENDING]")
    public class AclsCommandOptions extends BasePanelsCommand {
    }

    @Parameters(commandNames = {"acl-create"}, commandDescription = "Define a set of permissions for a list of users or groups [PENDING]")
    public class AclsCreateCommandOptions extends BasePanelsCommand {

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
    public class AclsMemberDeleteCommandOptions extends BasePanelsCommand {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-info"},
            commandDescription = "Return the set of permissions granted for the user or group [PENDING]")
    public class AclsMemberInfoCommandOptions extends BasePanelsCommand {

        @Parameter(names = {"--member-id"}, description = "Member id", required = true, arity = 1)
        public String memberId;
    }

    @Parameters(commandNames = {"acl-member-update"},
            commandDescription = "Update the set of permissions granted for the user or group [PENDING]")
    public class AclsMemberUpdateCommandOptions extends BasePanelsCommand {

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
}
