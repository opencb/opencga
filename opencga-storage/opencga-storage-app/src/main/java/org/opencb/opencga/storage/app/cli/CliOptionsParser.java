package org.opencb.opencga.storage.app.cli;

import com.beust.jcommander.*;
import com.beust.jcommander.converters.CommaParameterSplitter;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;

import java.util.*;

/**
 * Created by imedina on 02/03/15.
 */
public class CliOptionsParser {

    private final JCommander jcommander;

    private final GeneralOptions generalOptions;
    private final CommonCommandOptions commonCommandOptions;

    private final CommandCreateAccessions accessions;

    private final IndexVariantsCommandOptions indexVariantsCommandOptions;
    private final IndexAlignmentsCommandOptions indexAlignmentsCommandOptions;
//    private final IndexSequenceCommandOptions indexSequenceCommandOptions;

    private final QueryVariantsCommandOptions queryVariantsCommandOptions;
    private final QueryAlignmentsCommandOptions queryAlignmentsCommandOptions;

    private final AnnotateVariantsCommandOptions annotateVariantsCommandOptions;
    private final StatsVariantsCommandOptions statsVariantsCommandOptions;


    public CliOptionsParser() {

        generalOptions = new GeneralOptions();

        jcommander = new JCommander(generalOptions);
        jcommander.setProgramName("opencga-storage2.sh");

        commonCommandOptions = new CommonCommandOptions();
        jcommander.addCommand(accessions = new CommandCreateAccessions());

        indexVariantsCommandOptions = new IndexVariantsCommandOptions();
        indexAlignmentsCommandOptions = new IndexAlignmentsCommandOptions();
//        indexSequenceCommandOptions = new IndexSequenceCommandOptions();
        queryVariantsCommandOptions = new QueryVariantsCommandOptions();
        queryAlignmentsCommandOptions = new QueryAlignmentsCommandOptions();
        annotateVariantsCommandOptions = new AnnotateVariantsCommandOptions();
        statsVariantsCommandOptions = new StatsVariantsCommandOptions();

        jcommander.addCommand("index-variants", indexVariantsCommandOptions);
        jcommander.addCommand("index-alignments", indexAlignmentsCommandOptions);
//        jcommander.addCommand("index-sequence", indexSequenceCommandOptions);
        jcommander.addCommand("fetch-variants", queryVariantsCommandOptions);
        jcommander.addCommand("fetch-alignments", queryAlignmentsCommandOptions);
        jcommander.addCommand("annotate-variants", annotateVariantsCommandOptions);
        jcommander.addCommand("stats-variants", statsVariantsCommandOptions);
    }

    public void parse(String[] args) throws ParameterException {
        jcommander.parse(args);
//        String parsedCommand = jcommander.getParsedCommand();
//        return parsedCommand != null? parsedCommand: "";
    }

//    Command getCommand() {
//        String parsedCommand = jcommander.getParsedCommand();
//        if (parsedCommand != null) {
//            JCommander jCommander = jcommander.getCommands().get(parsedCommand);
//            List<Object> objects = jCommander.getObjects();
//            if (!objects.isEmpty() && objects.get(0) instanceof Command) {
//                return ((Command) objects.get(0));
//            }
//        }
//        return null;
//    }

    public String getCommand() {
        return (jcommander.getParsedCommand() != null) ? jcommander.getParsedCommand(): "";
    }

    public void printUsage(){
        if(getCommand().isEmpty()) {
            jcommander.usage();
        } else {
            jcommander.usage(getCommand());
        }
    }

    @Deprecated
    String usage() {
        StringBuilder builder = new StringBuilder();
        String parsedCommand = jcommander.getParsedCommand();
        if(parsedCommand != null && !parsedCommand.isEmpty()){
            jcommander.usage(parsedCommand, builder);
        } else {
            jcommander.usage(builder);
        }
        return builder.toString();//.replaceAll("\\^.*Default: false\\$\n", "");
    }

    class GeneralOptions {

        @Parameter(names = {"-h", "--help"}, help = true)
        boolean help;

        @Parameter(names = {"--version"})
        boolean version;

    }

//    class Command {
//
//        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
//        Map<String, String> params = new HashMap<>();
//    }

    class CommonCommandOptions {

        @Parameter(names = { "-h", "--help" }, description = "Print this help", help = true)
        boolean help;

        @Parameter(names = {"-L", "--log-level"}, description = "one of {error, warn, info, debug, trace}")
        String logLevel = "info";

        @Parameter(names = {"-v", "--verbose"}, description = "log-level to debug")
        boolean verbose;

        @Parameter(names = {"-C", "--conf" }, description = "Properties path")
        String configFile;

        @Parameter(names = { "--sm-name" }, description = "StorageManager class name. (Must be in the classpath)")
        String storageManagerName;

        @Parameter(names = {"--storage-engine"}, arity = 1, description = "One of the listed ones in OPENCGA.STORAGE.ENGINES, in storage.properties")
        String storageEngine = null;

        @Parameter(names = {"--storage-engine-config"}, arity = 1, description = "Path of the file with options to overwrite storage-<Plugin>.properties")
        String storageEngineConfigFile = null;

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        Map<String, String> params = new HashMap<>();

        @Override
        public String toString() {
            String paramsString = "{";
            for(Map.Entry<String, String> e :  params.entrySet()){
                paramsString += e.getKey()+" = "+e.getValue() + ", ";
            }
            paramsString += "}";
            return "GeneralParameters{" +
                    "configFile='" + configFile + '\'' +
                    ", storageManagerName='" + storageManagerName + '\'' +
                    ", help=" + help +
                    ", params=" + paramsString +
                    '}';
        }

    }



    @Parameters(commandNames = {"create-accessions"}, commandDescription = "Creates accession IDs for an input file")
    class CommandCreateAccessions extends CommonCommandOptions {

        @Parameter(names = {"-i", "--input"}, description = "File to annotation with accession IDs", required = true, arity = 1)
        String input;

        @Parameter(names = {"-p", "--prefix"}, description = "Accession IDs prefix", arity = 1)
        String prefix;

        @Parameter(names = {"-s", "--study-alias"}, description = "Unique ID for the study where the file is classified (used for prefixes)",
                required = true, arity = 1)//, validateValueWith = StudyIdValidator.class)
        String studyId;

        @Parameter(names = {"-r", "--resume-from-accession"}, description = "Starting point to generate accessions (will not be included)", arity = 1)
        String resumeFromAccession;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where the output file will be saved", arity = 1)
        String outdir;

        class StudyIdValidator implements IValueValidator<String> {
            @Override
            public void validate(String name, String value) throws ParameterException {
                if (value.length() < 6) {
                    throw new ParameterException("The study ID must be at least 6 characters long");
                }
            }
        }
    }


    class IndexCommandOptions extends CommonCommandOptions {

        @Parameter(names = {"-i", "--input"}, description = "File to index in the selected backend", required = true, arity = 1)
        String input;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved (optional)", arity = 1, required = false)
        String outdir = "";

        @Parameter(names = {"--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
        String fileId;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials = "";

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        String dbName;

//        @Parameter(names = {"-T", "--temporal-dir"}, description = "Directory where place temporal files (pending)", required = false, arity = 1)
//        String tmp = "";
//        @Parameter(names = {"--delete-temporal"}, description = "Delete temporal files (pending)", required = false)
//        boolean delete = false;
//        @Parameter(names = {"-b", "--backend"}, description = "StorageManager plugin used to index files into: mongodb (default), hbase (pending)", required = false, arity = 1)
//        String backend = "mongodb";
    }

    @Parameters(commandNames = {"index-variants"}, commandDescription = "Index variants file")
    class IndexVariantsCommandOptions extends IndexCommandOptions {

        @Parameter(names = {"--transform"}, description = "Run only the transform phase")
        boolean transform = false; // stop before load

        @Parameter(names = {"--load"}, description = "Run only the load phase")
        boolean load = false; // skip transform

        @Parameter(names = {"--study-name"}, description = "Full name of the study where the file is classified", required = false, arity = 1)
        String study;

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"-p", "--pedigree"}, description = "File containing pedigree information (in PED format, optional)", arity = 1)
        String pedigree;

        @Deprecated
        @Parameter(names = {"--include-stats"}, description = "Save statistics information available on the input file (optional)")
        boolean includeStats = false;

        @Parameter(names = {"--include-genotypes"}, description = "Index including the genotypes")
        boolean includeGenotype = false;

        @Parameter(names = {"--compress-genotypes"}, description = "Store genotypes as lists of samples")
        boolean compressGenotypes = false;

        @Parameter(names = {"--include-src"}, description = "Store also the source vcf row of each variant: {NO, FIRST_8_COLUMNS, FULL}")
        String includeSrc = "NO";

        @Parameter(names = {"--aggregated"}, description = "Aggregated VCF File: basic, EVS or ExAC (optional)", arity = 1)
        VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;

        @Parameter(names = {"-t", "--study-type"}, description = "Study type (optional) \n{FAMILY, TRIO, CONTROL, CASE, CASE_CONTROL, PAIRED, PAIRED_TUMOR, COLLECTION, TIME_SERIES}", arity = 1)
        VariantStudy.StudyType studyType = VariantStudy.StudyType.CASE_CONTROL;

        @Parameter(names = {"--gvcf"}, description = "[PENDING] The input file is in gvcf format")
        boolean gvcf = false;

        @Parameter(names = {"--bgzip"}, description = "[PENDING] The input file is in bgzip format")
        boolean bgzip = false;

        @Parameter(names = {"--calculate-stats"}, description = "Calculate statistics information over de indexed variants after the load step (optional)")
        boolean calculateStats = false;

        @Parameter(names = {"--annotate"}, description = "Annotate indexed variants after the load step (optional)")
        boolean annotate = false;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource annotator = null;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        boolean overwriteAnnotations = false;

        @Parameter(names = {"--annotator-config"}, description = "Path to the file with the configuration of the annotator")
        String annotatorConfigFile = null;
    }

    @Parameters(commandNames = {"index-alignments"}, commandDescription = "Index alignment file")
    class IndexAlignmentsCommandOptions extends IndexCommandOptions {

        @Parameter(names = "--transform", description = "Run only the transform phase")
        boolean transform = false;

        @Parameter(names = "--load", description = "Run only the load phase")
        boolean load = false;

        @Parameter(names = "--calculate-coverage", description = "Calculate also coverage while indexing")
        boolean calculateCoverage = false;

        //Acceptes values: ^[0-9]+(.[0-9]+)?[kKmMgG]?$  -->   <float>[KMG]
        @Parameter(names = "--mean-coverage", description = "Specify the chunk sizes to calculate average coverage. Only works if flag \"--calculate-coverage\" is also given. Please specify chunksizes as CSV: --mean-coverage 200,400", required = false)
        List<String> meanCoverage = Collections.singletonList("200");
    }



    class QueryCommandOptions extends CommonCommandOptions {
        //File location parameters
        @Parameter(names = {"-b", "--backend"}, description = "StorageManager plugin used to index files into: mongodb (default), hbase (pending)", required = false, arity = 1)
        String backend = "mongodb";

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        String dbName;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials = "";

        @Parameter(names = {"-r","--region"}, description = " [CSV]", required = false)
        List<String> regions = new LinkedList<>();

        @Parameter(names = {"--region-gff-file"}, description = "", required = false)
        String gffFile;

        @Parameter(names = {"-o", "--output"}, description = "Output file. Default: stdout", required = false, arity = 1)
        String output;

        @Parameter(names = {"--output-format"}, description = "Output format: vcf(default), vcf.gz, json, json.gz", required = false, arity = 1)
        String outputFormat = "vcf";

    }

    @Parameters(commandNames = {"fetch-variants"}, commandDescription = "Search over indexed variants")
    class QueryVariantsCommandOptions extends QueryCommandOptions {

        //Filter parameters
        @Parameter(names = {"--study-alias"}, description = " [CSV]", required = false)
        String studyAlias;

        @Parameter(names = {"-a", "--alias"}, description = "File unique ID. [CSV]", required = false, arity = 1)
        String fileId;

        @Parameter(names = {"-e", "--effect"}, description = " [CSV]", required = false, arity = 1)
        String effect;

        @Parameter(names = {"--id"}, description = " [CSV]", required = false)
        String id;

        @Parameter(names = {"-t", "--type"}, description = " [CSV]", required = false)
        String type;

        @Parameter(names = {"-g", "--gene"}, description = " [CSV]", required = false)
        String gene;

        @Parameter(names = {"--reference"}, description = " [CSV]", required = false)
        String reference;

        @Parameter(names = {"-S","--stats-filter"}, description = " [CSV]", required = false)
        List<String> stats = new LinkedList<>();

        @Parameter(names = {"--annot-filter"}, description = " [CSV]", required = false)
        List<String> annot = new LinkedList<>();


    }

    @Parameters(commandNames = {"fetch-alignments"}, commandDescription = "Search over indexed alignments")
    class QueryAlignmentsCommandOptions extends QueryCommandOptions {

        //Filter parameters
        @Parameter(names = {"-a", "--alias"}, description = "File unique ID.", required = false, arity = 1)
        String fileId;

        @Parameter(names = {"--file-path"}, description = "", required = false, arity = 1)
        String filePath;

        @Parameter(names = {"--include-coverage"}, description = " [CSV]", required = false)
        boolean coverage;

        @Parameter(names = {"-H", "--histogram"}, description = " ", required = false, arity = 1)
        int histogram = -1;

        @Parameter(names = {"--view-as-pairs"}, description = " ", required = false)
        boolean asPairs;

        @Parameter(names = {"--process-differences"}, description = " ", required = false)
        boolean processDifferences;

        @Parameter(names = {"-S","--stats-filter"}, description = " [CSV]", required = false)
        List<String> stats = new LinkedList<>();

    }



    @Parameters(commandNames = {"annotate-variants"}, commandDescription = "Create and load annotations into a database.")
    class AnnotateVariantsCommandOptions extends CommonCommandOptions {

        @Parameter(names = {"--create"}, description = "Run only the creation of the annotations to a file (specified by --output-filename)")
        boolean create = false;

        @Parameter(names = {"--load"}, description = "Run only the load of the annotations into the DB from FILE")
        String load = null;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager.AnnotationSource annotator = null;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        boolean overwriteAnnotations = false;

        @Parameter(names = {"--annotator-config"}, description = "Path to the file with the configuration of the annotator")
        String annotatorConfig = null;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        String dbName;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials = "";

        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: dbName", required = false, arity = 1)
        String fileName = "";

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", required = false, arity = 1)
        String outdir = "./";

        @Parameter(names = {"--species"}, description = "Species. Default hsapiens", required = false, arity = 1)
        String species = "hsapiens";

        @Parameter(names = {"--assembly"}, description = "Assembly. Default GRc37", required = false, arity = 1)
        String assembly = "GRc37";


        @Parameter(names = {"--filter-region"}, description = "Comma separated region filters", splitter = CommaParameterSplitter.class)
        List<String> filterRegion = null;

        @Parameter(names = {"--filter-chromosome"}, description = "Comma separated chromosome filters", splitter = CommaParameterSplitter.class)
        List<String> filterChromosome = null;

        @Parameter(names = {"--filter-gene"}, description = "Comma separated gene filters", splitter = CommaParameterSplitter.class)
        String filterGene = null;

        @Parameter(names = {"--filter-annot-consequence-type"}, description = "Comma separated annotation consequence type filters", splitter = CommaParameterSplitter.class)
        List filterAnnotConsequenceType = null; // TODO will receive CSV, only available when create annotations
    }



    @Parameters(commandNames = {"stats-variants"}, commandDescription = "Create and load stats into a database.")
    class StatsVariantsCommandOptions extends CommonCommandOptions {

        @Parameter(names = {"--overwrite-stats"}, description = "[PENDING] Overwrite stats in variants already present")
        boolean overwriteStats = false;

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"-f", "--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
        String fileId;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        String dbName;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials = "";

        @Parameter(names = {"--output-filename"}, description = "Output file name. Default: database name", required = false, arity = 1)
        String fileName = "";

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", required = false, arity = 1)
        String outdir = ".";
        @Parameter(names = {"--create"}, description = "Run only the creation of the stats to a file")
        boolean create = false;
        @Parameter(names = {"--load"}, description = "Load the stats from an already existing FILE directly into the database. FILE is a prefix with structure <INPUT_FILENAME>.<TIME>")
        String load = null;
/* TODO: filters?
        @Parameter(names = {"--filter-region"}, description = "Comma separated region filters", splitter = CommaParameterSplitter.class)
        List<String> filterRegion = null;

        @Parameter(names = {"--filter-chromosome"}, description = "Comma separated chromosome filters", splitter = CommaParameterSplitter.class)
        List<String> filterChromosome = null;

        @Parameter(names = {"--filter-gene"}, description = "Comma separated gene filters", splitter = CommaParameterSplitter.class)
        String filterGene = null;

        @Parameter(names = {"--filter-annot-consequence-type"}, description = "Comma separated annotation consequence type filters", splitter = CommaParameterSplitter.class)
        List filterAnnotConsequenceType = null; // TODO will receive CSV, only available when create annotations
        */
    }


    GeneralOptions getGeneralOptions() {
        return generalOptions;
    }

    IndexVariantsCommandOptions getIndexVariantsCommandOptions() {
        return indexVariantsCommandOptions;
    }

    IndexAlignmentsCommandOptions getIndexAlignmentsCommandOptions() {
        return indexAlignmentsCommandOptions;
    }

//    IndexSequenceCommandOptions getIndexSequenceCommandOptions() {
//        return indexSequenceCommandOptions;
//    }

    CommandCreateAccessions getAccessionsCommand() {
        return accessions;
    }

    QueryVariantsCommandOptions getQueryVariantsCommandOptions() {
        return queryVariantsCommandOptions;
    }

    QueryAlignmentsCommandOptions getQueryAlignmentsCommandOptions() {
        return queryAlignmentsCommandOptions;
    }

    AnnotateVariantsCommandOptions getAnnotateVariantsCommandOptions() {
        return annotateVariantsCommandOptions;
    }
    StatsVariantsCommandOptions getStatsVariantsCommandOptions() {
        return statsVariantsCommandOptions;
    }

    CommonCommandOptions getCommonCommandOptions() {
        return commonCommandOptions;
    }
}
