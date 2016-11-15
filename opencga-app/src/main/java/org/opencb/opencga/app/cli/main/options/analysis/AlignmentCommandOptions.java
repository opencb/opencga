package org.opencb.opencga.app.cli.main.options.analysis;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;
import org.opencb.opencga.app.cli.main.OpencgaCliOptionsParser;

/**
 * Created by pfurio on 11/11/16.
 */
@Parameters(commandNames = {"alignments"}, commandDescription = "Alignment commands")
public class AlignmentCommandOptions {

    public JCommander jCommander;
    public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonCommandOptions;
    public IndexCommandOptions indexCommandOptions;

    public AlignmentCommandOptions(OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonCommandOptions, JCommander jCommander) {
        this.commonCommandOptions = commonCommandOptions;
        this.jCommander = jCommander;
        this.indexCommandOptions = new IndexCommandOptions();
    }

    @Parameters(commandNames = {"index"}, commandDescription = "Index BAM files")
    public class IndexCommandOptions {

        @ParametersDelegate
        public OpencgaCliOptionsParser.OpencgaCommonCommandOptions commonOptions = commonCommandOptions;

        @Parameter(names = {"--file-id"}, description = "Comma separated list of file ids (files or directories)", required = true, arity = 1)
        public String fileIds;

        @Parameter(names = {"-s", "--study-id"}, description = "studyId", required = false, arity = 1)
        public String studyId;

        @Parameter(names = {"--outdir"}, description = "Directory where transformed index files will be stored", required = false, arity = 1)
        public String outdirId;

    }

}
