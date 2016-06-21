package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;
import org.opencb.opencga.catalog.models.Study;

import java.util.List;

/**
 * Created by pfurio on 13/06/16.
 */
@Parameters(commandNames = {"studies"}, commandDescription = "Study commands")
public class StudyCommandOptions {

    public CreateCommandOptions createCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    //public ResyncCommandOptions resyncCommandOptions;
    public FilesCommandOptions filesCommandOptions;
    public ScanFilesCommandOptions scanFilesCommandOptions;
    public StatusCommandOptions statusCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public UpdateCommandOptions updateCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;
    public SummaryCommandOptions summaryCommandOptions;
    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public StudyCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {

        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.filesCommandOptions = new FilesCommandOptions();
        this.scanFilesCommandOptions = new ScanFilesCommandOptions();
        this.statusCommandOptions = new StatusCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.updateCommandOptions = new UpdateCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
        this.summaryCommandOptions = new SummaryCommandOptions();
    }

    public abstract class BaseStudyCommand {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study identifier", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create new study")
    public class CreateCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--project-id"}, description = "Project identifier", required = true, arity = 1)
        public String projectId;

        @Parameter(names = {"-n", "--name"}, description = "Study name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-a", "--alias"}, description = "alias", required = true, arity = 1)
        public String alias;

        @Parameter(names = {"-t", "--type"}, description = "Type", required = false, arity = 1)
        public Study.Type type = Study.Type.COLLECTION;

        @Parameter(names = {"-d", "--description"}, description = "Organization", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--status"}, description = "Status.",
                required = false, arity = 1)
        public String status;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get study information")
    public class InfoCommandOptions extends BaseStudyCommand {
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search studies")
    public class SearchCommandOptions {

        @ParametersDelegate
        public OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--id"}, description = "Id.", required = false, arity = 1)
        public String id;

        @Parameter(names = {"--project-id"}, description = "Project Id.", required = false, arity = 1)
        public String projectId;

        @Parameter(names = {"--name"}, description = "Name.", required = false, arity = 1)
        public String name;

        @Parameter(names = {"--alias"}, description = "Alias.", required = false, arity = 1)
        public String alias;

        @Parameter(names = {"--type"}, description = "Type.", required = false, arity = 1)
        public String type;

        //Actualmente existe pero me dijo pedro que no va a existir
        @Parameter(names = {"--creator-id"}, description = "Creator id.", required = false, arity = 1)
        public String creatorId;

        @Parameter(names = {"--creation-date"}, description = "Creation date.", required = false, arity = 1)
        public String creationDate;

        @Parameter(names = {"--status"}, description = "Status.", required = false, arity = 1)
        public String status;

        @Parameter(names = {"--attributes"}, description = "Attributes.", required = false, arity = 1)
        public String attributes;

        @Parameter(names = {"--nattributes"}, description = "Numerical attributes.", required = false, arity = 1)
        public String nattributes;

        @Parameter(names = {"--battributes"}, description = "Boolean attributes.", required = false, arity = 0)
        public boolean battributes;

        @Parameter(names = {"--groups"}, description = "Groups.", required = false, arity = 1)
        public String groups;

        @Parameter(names = {"--groups-users"}, description = "Groups users.", required = false, arity = 1)
        public String groupsUsers;
    }
   /* @Parameters(commandNames = {"resync"}, commandDescription = "Scans the study folder to find changes")
    class ResyncCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
        public boolean calculateChecksum = false;
    }*/

    @Parameters(commandNames = {"scan-files"},
            commandDescription = "Scans the study folder to find untracked or missing files")
    public class ScanFilesCommandOptions extends BaseStudyCommand {  }

    @Parameters(commandNames = {"files"}, commandDescription = "Study files information")
    public class FilesCommandOptions extends BaseStudyCommand { }

    @Parameters(commandNames = {"status"}, commandDescription = "Scans the study folder to find untracked or missing files")
    public class StatusCommandOptions extends BaseStudyCommand {
    }

    @Parameters(commandNames = {"update"}, commandDescription = "Study modify")
    public class UpdateCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"-n", "--name"}, description = "Study name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-t", "--type"}, description = "Type", required = false, arity = 1)
        public Study.Type type = Study.Type.COLLECTION;

        @Parameter(names = {"-d", "--description"}, description = "Organization", required = false, arity = 1)
        public String description;

        @Parameter(names = {"--status"}, description = "Status.",
                required = false, arity = 1)
        public String status;


    }

    @Parameters(commandNames = {"delete"}, commandDescription = "Delete a study [PENDING]")
    public class DeleteCommandOptions extends BaseStudyCommand {
    }

    @Parameters(commandNames = {"summary"}, commandDescription = "Summary with the general stats of a study")
    public class SummaryCommandOptions extends BaseStudyCommand {
    }

}
