package org.opencb.opencga.app.cli.admin.options;

import com.beust.jcommander.*;
import org.opencb.opencga.app.cli.admin.AdminCliOptionsParser;
import org.opencb.opencga.storage.benchmark.BenchmarkRunner;

import java.util.HashMap;
import java.util.Map;

@Parameters(commandNames = {"benchmark"}, commandDescription = "Execute benchmark operations.")
public class BenchmarkCommandOptions extends org.opencb.opencga.app.cli.GeneralCliOptions {

    public final VariantBenchmarkCommandOptions variantBenchmarkCommandOptions;
    //    public final AlignmentBenchmarkCommandOptions alignmentBenchmarkCommandOptions;
    private final AdminCliOptionsParser.AdminCommonCommandOptions commonCommandOptions;
    private final AdminCliOptionsParser.IgnorePasswordCommonCommandOptions noPasswordCommonCommandOptions;



    public BenchmarkCommandOptions(AdminCliOptionsParser.AdminCommonCommandOptions commonOptions, JCommander jCommander) {
        super(jCommander);
        this.commonCommandOptions = commonOptions;
        noPasswordCommonCommandOptions = new AdminCliOptionsParser.IgnorePasswordCommonCommandOptions(commonOptions.commonOptions);

        variantBenchmarkCommandOptions = new VariantBenchmarkCommandOptions();
//        alignmentBenchmarkCommandOptions = new AlignmentBenchmarkCommandOptions();
    }

    /**
     * Generic benchmark options
     */
    public class GenericBenchmarkOptions {

        @Parameter(names = {"--connector"}, description = "How to connect to the system: REST or DIRECT")
        public BenchmarkRunner.ConnectionType connectionType = BenchmarkRunner.ConnectionType.REST;

        @Parameter(names = {"-m", "--mode"}, description = "Type of queries to execute: FIXED, RANDOM")
        public BenchmarkRunner.ExecutionMode executionMode = BenchmarkRunner.ExecutionMode.RANDOM;

        @Parameter(names = {"-r", "--num-repetition"}, description = "Number of repetition to execute.", arity = 1)
        public Integer repetition;

        @Parameter(names = {"-c", "--concurrency"}, description = "Number of concurrent threads.", arity = 1)
        public Integer concurrency;

        @Parameter(names = {"--project"}, description = "Project name for the benchmark", required = true, arity = 1)
        public String project;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory", required = false, arity = 1)
        public String outdir;

        @Parameter(names = {"--limit"}, description = "Limit the number of returned elements.", required = false, arity = 1)
        public Integer limit;

        @Parameter(names = {"--count"}, description = "Count results. Do not return elements.", required = false, arity = 0)
        public boolean count;

        @Parameter(names = {"--file"}, description = "File path to load queries", required = false, arity = 1)
        public String queryFile;

        @Parameter(names = {"--delay"}, description = "Delay between each sampler thread.", required = false, arity = 1)
        public Integer delay;

    }

    /**
     * Variant command
     */
    @Parameters(commandNames = {"variant"}, commandDescription = "Benchmark read operations over variants")
    public class VariantBenchmarkCommandOptions extends GenericBenchmarkOptions {

        @ParametersDelegate
        public AdminCliOptionsParser.IgnorePasswordCommonCommandOptions commonOptions = noPasswordCommonCommandOptions;

        @Parameter(names = {"-q", "--query"}, description = "Query Ids to execute for FIXED mode (Default All) OR Query pattern to execute for Random mode e.g. gene,ct(30);region(3)", arity = 1)
        public String query;

        @DynamicParameter(names = {"-B", "--baseQuery"}, description = "Overwrite baseQuery options from file, comma separated, ie. -Blimit=1000", hidden = false)
        public Map<String, String> baseQuery = new HashMap<>();
    }

    /**
     * Alignment command
     */
//    @Parameters(commandNames = {"alignment"}, commandDescription = "[PENDING]")
//    public class AlignmentBenchmarkCommandOptions extends GenericBenchmarkOptions {
//
//        @ParametersDelegate
//        public GeneralCliOptions.CommonOptions commonOptions = commonCommandOptions;
//    }

    public AdminCliOptionsParser.AdminCommonCommandOptions getCommonOptions() {
        return commonCommandOptions;
    }
}
