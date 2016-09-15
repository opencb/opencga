package org.opencb.opencga.app.cli.main.options.catalog;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.app.cli.main.options.commons.AclCommandOptions;

/**
 * Created by sgallego on 6/15/16.
 */
@Parameters(commandNames = {"panels"}, commandDescription = "Panels commands")
public class PanelCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;

    public AclCommandOptions.AclsCommandOptions aclsCommandOptions;
    public AclCommandOptions.AclsCreateCommandOptions aclsCreateCommandOptions;
    public AclCommandOptions.AclsMemberDeleteCommandOptions aclsMemberDeleteCommandOptions;
    public AclCommandOptions.AclsMemberInfoCommandOptions aclsMemberInfoCommandOptions;
    public AclCommandOptions.AclsMemberUpdateCommandOptions aclsMemberUpdateCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    private AclCommandOptions aclCommandOptions;

    public PanelCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();

        aclCommandOptions = new AclCommandOptions(commonCommandOptions);
        this.aclsCommandOptions = aclCommandOptions.getAclsCommandOptions();
        this.aclsCreateCommandOptions = aclCommandOptions.getAclsCreateCommandOptions();
        this.aclsMemberDeleteCommandOptions = aclCommandOptions.getAclsMemberDeleteCommandOptions();
        this.aclsMemberInfoCommandOptions = aclCommandOptions.getAclsMemberInfoCommandOptions();
        this.aclsMemberUpdateCommandOptions = aclCommandOptions.getAclsMemberUpdateCommandOptions();
    }

    class BasePanelsCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Panel id", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create a panel")
    public class CreateCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-s","--study-id"}, description = "Study id", required = true, arity = 1)
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

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;
    }

}
