package org.opencb.opencga.app.cli.main.options;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import com.beust.jcommander.converters.IParameterSplitter;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser.OpencgaCommonCommandOptions;

import java.util.Arrays;
import java.util.List;

/**
 * Created by sgallego on 6/14/16.
 */
@Parameters(commandNames = {"sample"}, commandDescription = "Sample commands")
public class SampleCommandOptions {

    public LoadCommandOptions loadCommandOptions;
    public InfoCommandOptions infoCommandOptions;
    public SearchCommandOptions searchCommandOptions;
    public DeleteCommandOptions deleteCommandOptions;

    public JCommander jCommander;
    public OpencgaCommonCommandOptions commonCommandOptions;

    public SampleCommandOptions(OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;

        this.loadCommandOptions = new LoadCommandOptions();
        this.infoCommandOptions = new InfoCommandOptions();
        this.searchCommandOptions = new SearchCommandOptions();
        this.deleteCommandOptions = new DeleteCommandOptions();
    }

    class BaseSampleCommand {
        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"-id", "--sample-id"}, description = "Sample id", required = true, arity = 1)
        long id;
    }

    @Parameters(commandNames = {"load"}, commandDescription = "Load samples from a pedigree file")
    class LoadCommandOptions {


        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--variable-set-id"}, description = "VariableSetId that represents the pedigree file", required = false, arity = 1)
        long variableSetId;

        @Parameter(names = {"--pedigree-id"}, description = "Pedigree file id already loaded in OpenCGA", required = true, arity = 1)
        String pedigreeFileId;
    }
    @Parameters(commandNames = {"info"}, commandDescription = "Get samples information")
    class InfoCommandOptions extends BaseSampleCommand {
    }

    @Parameters(commandNames = {"search"}, commandDescription = "Search samples")
    class SearchCommandOptions {


        @ParametersDelegate
        OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--study-id"}, description = "Study id", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"--variable-set-id"}, description = "VariableSetId", required = false, arity = 1)
        String variableSetId;

        @Parameter(names = {"--name"}, description = "Sample names (CSV)", required = false, arity = 1)
        String sampleNames;

        @Parameter(names = {"-id", "--sample-id"}, description = "Sample ids (CSV)", required = false, arity = 1)
        String sampleIds;

        @Parameter(names = {"-a", "--annotation"}, description = "SampleAnnotations values. <variableName>:<annotationValue>(,<annotationValue>)*", required = false, arity = 1, splitter = SemiColonParameterSplitter.class)
        List<String> annotation;
    }



    @Parameters(commandNames = {"delete"}, commandDescription = "Deletes the selected sample")
    class DeleteCommandOptions extends BaseSampleCommand {
    }

    public static class SemiColonParameterSplitter implements IParameterSplitter {

        public List<String> split(String value) {
            return Arrays.asList(value.split(";"));
        }

    }

}
