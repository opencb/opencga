package org.opencb.opencga.app.cli.main.options.catalog;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

/**
 * Created by pfurio on 13/06/16.
 */
@Parameters(commandNames = {"projects"}, commandDescription = "Project commands")
public class ProjectCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public StudiesCommandOptions studiesCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public HelpCommandOptions helpCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public ProjectCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.studiesCommandOptions = new StudiesCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.helpCommandOptions = new HelpCommandOptions();
    }

    public class BaseProjectCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Project identifier", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create new project")
    public class CreateCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-n", "--name"}, description = "Project name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-a", "--alias"}, description = "Alias", required = true, arity = 1)
        public String alias;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"-o", "--organization"}, description = "Organization", required = false, arity = 1)
        public String organization;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get project information")
    public class InfoCommandOptions extends BaseProjectCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;
    }

    @Parameters(commandNames = {"studies"}, commandDescription = "Get all studies from a project")
    public class StudiesCommandOptions extends BaseProjectCommand {

        @Parameter(names = {"--include"}, description = "Comma separated list of fields to be included in the response", arity = 1)
        public String include;

        @Parameter(names = {"--exclude"}, description = "Comma separated list of fields to be excluded from the response", arity = 1)
        public String exclude;

        @Parameter(names = {"--skip"}, description = "Number of results to skip", arity = 1)
        public String skip;

        @Parameter(names = {"--limit"}, description = "Maximum number of results to be returned", arity = 1)
        public String limit;
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Update a project")
    public class UpdateCommandOptions extends BaseProjectCommand {

        @Parameter(names = {"-n", "--name"}, description = "Project name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        public String description;

        @Parameter(names = {"-o", "--organization"}, description = "Organization", required = false, arity = 1)
        public String organization;

        //@Parameter(names = {"-s", "--status"}, description = "Status", required = false, arity = 1)
        //public String status;

        @Parameter(names = {"-a", "--attributes"}, description = "Attributes", required = false, arity = 1)
        public String attributes;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete a project")
    public class DeleteCommandOptions extends BaseProjectCommand {

    }

    @Parameters(commandNames = {"help"}, commandDescription = "Help in project")
    public class HelpCommandOptions {
        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;
    }

}
