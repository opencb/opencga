package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
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
    }

    class BaseProjectCommand {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-id", "--project-id"}, description = "Project identifier", required = true, arity = 1)
        String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create new project")
    class CreateCommandOptions {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-n", "--name"}, description = "Project name", required = true, arity = 1)
        String name;

        @Parameter(names = {"-a", "--alias"}, description = "Alias", required = true, arity = 1)
        String alias;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        String description;

        @Parameter(names = {"-o", "--organization"}, description = "Organization", required = false, arity = 1)
        String organization;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get project information")
    class InfoCommandOptions extends BaseProjectCommand{ }

    @Parameters(commandNames = {"studies"}, commandDescription = "Get all studies from a project")
    class StudiesCommandOptions extends BaseProjectCommand{ }

    @Parameters(commandNames = {"update"}, commandDescription = "Update a project")
    class UpdateCommandOptions extends BaseProjectCommand{
        @Parameter(names = {"-n", "--name"}, description = "Project name", required = true, arity = 1)
        String name;

        @Parameter(names = {"-d", "--description"}, description = "Description", required = false, arity = 1)
        String description;

        @Parameter(names = {"-o", "--organization"}, description = "Organization", required = false, arity = 1)
        String organization;

        @Parameter(names = {"-s", "--status"}, description = "Status", required = false, arity = 1)
        String status;

        @Parameter(names = {"-a", "--attributes"}, description = "Attributes", required = false, arity = 1)
        String attributes;
    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete a project")
    class DeleteCommandOptions extends BaseProjectCommand{ }

}
