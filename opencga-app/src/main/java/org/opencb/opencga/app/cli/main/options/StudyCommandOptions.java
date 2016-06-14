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
    public ResyncCommandOptions resyncCommandOptions;
    public ListCommandOptions listCommandOptions;
    public CheckCommandOptions checkCommandOptions;
    public StatusCommandOptions statusCommandOptions;
    public AnnotationCommandOptions annotationCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public StudyCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.createCommandOptions = new CreateCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.resyncCommandOptions = new ResyncCommandOptions();
        this.listCommandOptions = new ListCommandOptions();
        this.checkCommandOptions = new CheckCommandOptions();
        this.statusCommandOptions = new StatusCommandOptions();
        this.annotationCommandOptions = new AnnotationCommandOptions();
    }

    abstract class BaseStudyCommand {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-id", "--study-id"}, description = "Study identifier", required = true, arity = 1)
        public String id;
    }

    @Parameters(commandNames = {"create"}, commandDescription = "Create new study")
    public class CreateCommandOptions {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--project-id"}, description = "Project identifier", required = true, arity = 1)
        public String projectId;

        @Parameter(names = {"-n", "--name"}, description = "Study name", required = true, arity = 1)
        public String name;

        @Parameter(names = {"-a", "--alias"}, description = "alias", required = true, arity = 1)
        public String alias;

        @Parameter(names = {"-t", "--type"}, description = "Type", required = false, arity = 1)
        Study.Type type = Study.Type.COLLECTION;

        @Parameter(names = {"-d", "--description"}, description = "Organization", required = false, arity = 1)
        String description;

        @Parameter(names = {"--uri"}, description = "URI for the folder where to place the study files. Must be a correct URI.", required = false, arity = 1)
        String uri;

        @Parameter(names = {"--datastore"}, description = "Configure place to store different files. One datastore per bioformat. <bioformat>:<storageEngineName>:<database_name>")
        List<String> datastores;

        @Parameter(names = {"--aggregation-type"}, description = "Set the study as aggregated of type {NONE, BASIC, EVS, EXAC}")
        VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;
    }

    @Parameters(commandNames = {"info"}, commandDescription = "Get study information")
    public class InfoCommandOptions  extends BaseStudyCommand {}

    @Parameters(commandNames = {"resync"}, commandDescription = "Scans the study folder to find changes")
    class ResyncCommandOptions extends BaseStudyCommand {
        @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
        boolean calculateChecksum = false;

    }

    @Parameters(commandNames = {"check-files"}, commandDescription = "Check if files in study are correctly tracked.")
    class CheckCommandOptions extends BaseStudyCommand {
        @Parameter(names = {"-ch", "--checksum"}, description = "Calculate checksum", required = false, arity = 0)
        boolean calculateChecksum = false;

    }

    @Parameters(commandNames = {"list"}, commandDescription = "List files in folder")
    public class ListCommandOptions extends BaseStudyCommand {
        @Parameter(names = {"--level"}, description = "Descend only level directories deep.", arity = 1)
        public int level = Integer.MAX_VALUE;

        @Parameter(names = {"-R", "--recursive"}, description = "List subdirectories recursively", arity = 0)
        public boolean recursive = false;

        @Parameter(names = {"-U", "--show-uris"}, description = "Show uris from linked files and folders", arity = 0)
        public boolean uries = false;
    }

    @Parameters(commandNames = {"status"}, commandDescription = "Scans the study folder to find untracked or missing files")
    public class StatusCommandOptions extends BaseStudyCommand {}

    @Parameters(commandNames = {"annotate-variants"}, commandDescription = "Annotate variants")
    class AnnotationCommandOptions extends BaseStudyCommand {

        @Parameter(names = {"-o", "--outdir-id"}, description = "Directory ID where to create the file", required = true, arity = 1)
        String outdir = "";

        @Parameter(names = {"--enqueue"}, description = "Enqueue the job to be launched by the execution manager", arity = 0)
        boolean enqueue;

        @Parameter(description = " -- {opencga-storage internal parameter. Use your head}") //Wil contain args after "--"
        public List<String> dashDashParameters;
    }
}
