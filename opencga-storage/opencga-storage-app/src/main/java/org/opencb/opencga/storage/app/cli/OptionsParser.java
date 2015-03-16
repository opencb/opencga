package org.opencb.opencga.storage.app.cli;

import com.beust.jcommander.*;
import com.beust.jcommander.converters.CommaParameterSplitter;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;
import org.opencb.opencga.storage.core.variant.annotation.*;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;

import java.util.*;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 */
@Deprecated
public class OptionsParser {
    private final JCommander jcommander;

    private final GeneralParameters generalParameters;
    private final CommandCreateAccessions accessions;
//    private final CommandTransformVariants transform;
//    private final CommandLoadVariants load;
//    private final CommandTransformAlignments transformAlignments;
//    private final CommandLoadAlignments loadAlignments;
    private final CommandIndexVariants commandIndexVariants;
    private final CommandIndexAlignments commandIndexAlignments;
    private final CommandIndexSequence commandIndexSequence;
    private final CommandFetchVariants commandFetchVariants;
    private final CommandFetchAlignments commandFetchAlignments;
    private final CommandAnnotateVariants commandAnnotateVariants;
    private final CommandStatsVariants commandStatsVariants;
//    private final CommandCreateAnnotations commandCreateAnnotations;
//    private final CommandLoadAnnotations commandLoadAnnotations;
//    private CommandDownloadAlignments downloadAlignments;

    public OptionsParser() {
        jcommander = new JCommander();
        jcommander.addObject(generalParameters = new GeneralParameters());
        jcommander.addCommand(accessions = new CommandCreateAccessions());
//        jcommander.addCommand(transform = new CommandTransformVariants());
//        jcommander.addCommand(load = new CommandLoadVariants());
//        jcommander.addCommand(transformAlignments = new CommandTransformAlignments());
//        jcommander.addCommand(loadAlignments = new CommandLoadAlignments());
        jcommander.addCommand(commandIndexVariants = new CommandIndexVariants());
        jcommander.addCommand(commandIndexAlignments = new CommandIndexAlignments());
        jcommander.addCommand(commandIndexSequence = new CommandIndexSequence());
        jcommander.addCommand(commandFetchVariants = new CommandFetchVariants());
        jcommander.addCommand(commandFetchAlignments = new CommandFetchAlignments());
        jcommander.addCommand(commandAnnotateVariants = new CommandAnnotateVariants());
        jcommander.addCommand(commandStatsVariants = new CommandStatsVariants());
//        jcommander.addCommand(commandCreateAnnotations = new CommandCreateAnnotations());
//        jcommander.addCommand(commandLoadAnnotations = new CommandLoadAnnotations());
//        jcommander.addCommand(downloadAlignments = new CommandDownloadAlignments());
    }

    class Command {

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        Map<String, String> params = new HashMap<>();

    }

    /**
     -h | --help
     -v | --version
     --log-level INT
     --verbose
     --storage-engine TEXT
     --storage-engine-config FILE
     */
    class GeneralParameters extends Command {
        @Parameter(names = { "--properties-path" }, description = "Properties path")
        String propertiesPath = null;

        @Parameter(names = { "--sm-name" }, description = "StorageManager class name. (Must be in the classpath)")
        String storageManagerName;

        @Parameter(names = { "-h", "--help" }, description = "Print this help", help = true)
        boolean help;

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        Map<String, String> params = new HashMap<String, String>();

        @Parameter(names = {"-v", "--version"}, description = "print version and exit")
        boolean version;

        @Parameter(names = {"--verbose"}, description = "log-level to debug")
        boolean verbose;

        @Parameter(names = {"--log-level"}, description = "one of {error, warn, info, debug, trace}")
        String logLevel = null; // TODO add validation?

        @Parameter(names = {"--storage-engine"}, arity = 1, description = "One of the listed ones in OPENCGA.STORAGE.ENGINES, in storage.properties")
        String storageEngine = null;

        @Parameter(names = {"--storage-engine-config"}, arity = 1, description = "Path of the file with options to overwrite storage-<Plugin>.properties")
        String storageEngineConfig = null;

        @Override
        public String toString() {
            String paramsString = "{";
            for(Map.Entry<String, String> e :  params.entrySet()){
                paramsString += e.getKey()+" = "+e.getValue() + ", ";
            }
            paramsString+="}";
            return "GeneralParameters{" +
                    "configFile='" + propertiesPath + '\'' +
                    ", storageManagerName='" + storageManagerName + '\'' +
                    ", help=" + help +
                    ", params=" + paramsString +
                    '}';
        }
    }


    @Parameters(commandNames = {"create-accessions"}, commandDescription = "Creates accession IDs for an input file")
    class CommandCreateAccessions extends Command {

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


    @Deprecated
    @Parameters(commandNames = {"transform-variants"}, commandDescription = "Generates a data model from an input file")
    class CommandTransformVariants extends Command {

        @Parameter(names = {"-i", "--input"}, description = "File to transform into the OpenCGA data model", required = true, arity = 1)
        String file;

        @Parameter(names = {"-a", "--alias"}, description = "Unique ID for the file to be transformed", required = true, arity = 1)
        String fileId;

        @Parameter(names = {"-s", "--study"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        String study;

        @Parameter(names = {"--study-alias"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"-p", "--pedigree"}, description = "File containing pedigree information (in PED format, optional)", arity = 1)
        String pedigree;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", arity = 1)
        String outdir;

        @Parameter(names = {"--include-effect"}, description = "Save variant effect information (optional)")
        boolean includeEffect = false;

        @Parameter(names = {"--include-samples"}, description = "Save samples information (optional)")
        boolean includeSamples = false;

        @Parameter(names = {"--include-stats"}, description = "Save statistics information (optional)")
        boolean includeStats = false;

        @Parameter(names = {"--aggregated"}, description = "Aggregated VCF File: basic or EVS (optional)", arity = 1)
        VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;

        @Parameter(names = {"-t", "--study-type"}, description = "Study type (optional)", arity = 1)
        VariantStudy.StudyType studyType = VariantStudy.StudyType.CASE_CONTROL;
    }

    @Deprecated
    @Parameters(commandNames = {"load-variants"}, commandDescription = "Loads an already generated data model into a backend")
    class CommandLoadVariants extends Command {

        @Parameter(names = {"-i", "--input"}, description = "Prefix of files to save in the selected backend", required = true, arity = 1)
        String input;

        @Parameter(names = {"-b", "--backend"}, description = "Storage to save files into: mongodb (default) or hbase (pending)", required = false, arity = 1)
        String backend = "mongodb";

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials;

        @Parameter(names = {"--include-effect"}, description = "Save variant effect information (optional)")
        boolean includeEffect = false;

        @Parameter(names = {"--include-samples"}, description = "Save samples information (optional)")
        boolean includeSamples = false;

        @Parameter(names = {"--include-stats"}, description = "Save statistics information (optional)")
        boolean includeStats = false;

        @Parameter(names = {"-d", "--dbName"}, description = "DataBase name", required = false, arity = 1)
        String dbName;
    }

    @Deprecated
    @Parameters(commandNames = {"transform-alignments"}, commandDescription = "Generates the Alignment data model from an input file")
    class CommandTransformAlignments extends Command {

        @Parameter(names = {"-i", "--input"}, description = "File to transform into the OpenCGA data model", required = true, arity = 1)
        String file;

        @Parameter(names = {"-a", "--alias"}, description = "Unique ID for the file to be transformed", required = false, arity = 1)
        String fileId;

//        @Parameter(names = {"-s", "--study"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
//        String study;

      //  @Parameter(names = {"--study-alias"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
      //  String studyId;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", arity = 1)
        String outdir = "./";
        
        
        @Parameter(names = {"--plain"}, description = "Do not compress the output (optional)", required = false)
        boolean plain = false;
        
        @Parameter(names = {"--include-coverage"}, description = "Save coverage information (optional)", required = false, arity = 0)
        boolean includeCoverage = false;
        
        //Acceptes values: ^[0-9]+(.[0-9]+)?[kKmMgG]?$  -->   <float>[KMG]
        @Parameter(names = "--mean-coverage", description = "Add mean coverage values (optional)", required = false)
        List<String> meanCoverage = new LinkedList<String>();
        
        
//        @Parameters(commandNames = {"--include-coverage"}, commandDescription = "Save coverage information (optional)")
//        class CommandCoverage extends Command {
//            @Parameter(names = {"--plain"}, description = "Do not compress the output (optional)", required = false)
//            boolean plain = false;
//        }
        
//        @Parameter(names = {"--compact"}, description = "Compact sequence differences with reference genome (optional)", required = false, arity = 0)
//        boolean compact = false;


    }

    @Deprecated
    @Parameters(commandNames = {"load-alignments"}, commandDescription = "Loads an already generated data model into a backend")
    class CommandLoadAlignments extends Command {
        
        @Parameter(names = {"-a", "--alias"}, description = "Unique ID for the file to be loaded", required = true, arity = 1)
        String fileId;

        @Parameter(names = {"-i", "--input"}, description = "Main file to save in the selected backend", required = true, arity = 1)
        String input;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials = "";

        @Parameter(names = {"-d", "--dbName"}, description = "DataBase name", required = false, arity = 1)
        String dbName;

        @Parameter(names = {"-b", "--backend"}, description = "StorageManager plugin used to index files into: mongodb (default), hbase (pending)", required = false, arity = 1)
        String backend = "mongodb";
    }


    @Parameters(commandNames = {"download-alignments"}, commandDescription = "Downloads a data model from the backend")
    class CommandDownloadAlignments extends Command {
        
        @Parameter(names = {"-a", "--alias"}, description = "Unique ID for the file to be extracted", required = true, arity = 1)
        String alias;

        @Parameter(names = {"-s", "--study"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        String study;
        
        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = true, arity = 1)
        String credentials;

//        @Parameter(names = {"--study-alias"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
//        String studyId;
        
        @Parameter(names = {"-b", "--backend"}, description = "Storage to save files into: hbase (default) or hive (pending)", required = false, arity = 1)
        String backend = "hbase";
        
        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved (optional)", arity = 1)
        String outdir;

        @Parameter(names = {"--region"}, description = "Limits the Alignments to a region <chromosome>[:<start>-<end>] (optional)", arity = 1)
        String region;
        
        @Parameter(names = {"-f", "--format"}, description = "Outfile format: sam (default), bam, json, json.gz ", arity = 1)
        String format = "json.gz";

        @Parameter(names = {"--coverage"}, description = "Get the region coverage instead the alignments.", arity = 0)
        boolean coverage = false;

        @Parameter(names = {"--remove"}, description = "Remove file from system (pending)", hidden = true)
        boolean remove;
        
        
    }

    class CommandIndex extends Command {

        @Parameter(names = {"-i", "--input"}, description = "File to index in the selected backend", required = true, arity = 1)
        String input;

//        @Parameter(names = {"-T", "--temporal-dir"}, description = "Directory where place temporal files (pending)", required = false, arity = 1)
//        String tmp = "";

//        @Parameter(names = {"--delete-temporal"}, description = "Delete temporal files (pending)", required = false)
//        boolean delete = false;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved (optional)", arity = 1, required = false)
        String outdir = "";

        @Parameter(names = {"--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
        String fileId;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials = "";

//        @Parameter(names = {"-b", "--backend"}, description = "StorageManager plugin used to index files into: mongodb (default), hbase (pending)", required = false, arity = 1)
//        String backend = "mongodb";

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        String dbName;
    }

    @Parameters(commandNames = {"index-variants"}, commandDescription = "Index variants file")
    class CommandIndexVariants extends CommandIndex {

        @Parameter(names = {"--study-name"}, description = "Full name of the study where the file is classified", required = false, arity = 1)
        String study;

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"-p", "--pedigree"}, description = "File containing pedigree information (in PED format, optional)", arity = 1)
        String pedigree;
//
//        @Parameter(names = {"--include-effect"}, description = "Save variant effect information (optional)")
//        boolean includeEffect = false;

        @Parameter(names = {"--include-stats"}, description = "Save statistics information available on the input file (optional)")
        boolean includeStats = false;

        @Parameter(names = {"--include-genotypes"}, description = "Index including the genotypes")
        boolean includeGenotype = false;

        @Parameter(names = {"--compress-genotypes"}, description = "Store genotypes as lists of samples")
        boolean compressGenotypes = false;

        @Parameter(names = {"--include-src"}, description = "Store also the source vcf row of each variant")
        boolean includeSrc = false;

        @Parameter(names = {"--aggregated"}, description = "Aggregated VCF File: basic or EVS (optional)", arity = 1)
        VariantSource.Aggregation aggregated = VariantSource.Aggregation.NONE;

        @Parameter(names = {"--aggregation-mapping-file"}, description = "Needed to parse population stats in aggregated files.")
        String aggregationMappingFile = null;
        
        @Parameter(names = {"-t", "--study-type"}, description = "Study type (optional) \n{FAMILY, TRIO, CONTROL, CASE, CASE_CONTROL, PAIRED, PAIRED_TUMOR, COLLECTION, TIME_SERIES}", arity = 1)
        VariantStudy.StudyType studyType = VariantStudy.StudyType.CASE_CONTROL;

        @Parameter(names = {"--transform"}, description = "Run only the transform phase")
        boolean transform = false; // stop before load

        @Parameter(names = {"--load"}, description = "Run only the load phase")
        boolean load = false; // skip transform

        @Parameter(names = {"--gvcf"}, description = "[PENDING] The input file is in gvcf format")
        boolean gvcf = false;
        @Parameter(names = {"--bgzip"}, description = "[PENDING] The input file is in bgzip format")
        boolean bgzip = false;

        @Parameter(names = {"--calculate-stats"}, description = "Calculate statistics information over de indexed variants after the load step (optional)")
        boolean calculateStats = false;

        @Parameter(names = {"--annotate"}, description = "Annotate indexed variants after the load step (optional)")
        boolean annotate = false;

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        VariantAnnotationManager.AnnotationSource annotator = null;

        @Parameter(names = {"--overwrite-annotations"}, description = "Overwrite annotations in variants already present")
        boolean overwriteAnnotations = false;

        @Parameter(names = {"--annotator-config"}, description = "Path to the file with the configuration of the annotator")
        String annotatorConfig = null;
    }

    @Parameters(commandNames = {"index-alignments"}, commandDescription = "Index alignment file")
    class CommandIndexAlignments extends CommandIndex {

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

    @Parameters(commandNames = {"index-sequence"}, commandDescription = "Index sequence file")
    class CommandIndexSequence extends CommandIndex {

    }

    class CommandFetch extends Command {
        //File location parameters
        @Parameter(names = {"-b", "--backend"}, description = "StorageManager plugin used to index files into: mongodb (default), hbase (pending)", required = false, arity = 1)
        String backend = "mongodb";

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        String dbName;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials = "";

        //Region parameters
        @Parameter(names = {"-r","--region"}, description = " [CSV]", required = false)
        List<String> regions = new LinkedList<>();

        @Parameter(names = {"--region-gff-file"}, description = "", required = false)
        String gffFile;

        //Output format
        @Parameter(names = {"-o", "--output"}, description = "Output file. Default: stdout", required = false, arity = 1)
        String output;

        @Parameter(names = {"--output-format"}, description = "Output format: vcf(default), vcf.gz, json, json.gz", required = false, arity = 1)
        String outputFormat = "vcf";

    }

    @Parameters(commandNames = {"fetch-variants"}, commandDescription = "Search over indexed variants")
    class CommandFetchVariants extends CommandFetch {

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
    class CommandFetchAlignments extends CommandFetch {


        //Filter parameters
        @Parameter(names = {"-a", "--alias"}, description = "File unique ID.", required = false, arity = 1)
        String fileId;

        @Parameter(names = {"--file-path"}, description = "", required = false, arity = 1)
        String filePath;

        @Parameter(names = {"-C", "--include-coverage"}, description = " [CSV]", required = false)
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
    class CommandAnnotateVariants extends Command {

        @Parameter(names = {"--annotator"}, description = "Annotation source {cellbase_rest, cellbase_db_adaptor}")
        VariantAnnotationManager.AnnotationSource annotator = null;

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

        @Parameter(names = {"--create"}, description = "Run only the creation of the annotations to a file (specified by --output-filename)")
        boolean create = false;
        @Parameter(names = {"--load"}, description = "Run only the load of the annotations into the DB from FILE")
        String load = null;

        @Parameter(names = {"--filter-region"}, description = "Comma separated region filters", splitter = CommaParameterSplitter.class)
        List<String> filterRegion = null;

        @Parameter(names = {"--filter-chromosome"}, description = "Comma separated chromosome filters", splitter = CommaParameterSplitter.class)
        List<String> filterChromosome = null;

        @Parameter(names = {"--filter-gene"}, description = "Comma separated gene filters", splitter = CommaParameterSplitter.class)
        String filterGene = null;

        @Parameter(names = {"--filter-annot-consequence-type"}, description = "Comma separated annotation consequence type filters", splitter = CommaParameterSplitter.class)
        List filterAnnotConsequenceType = null; // TODO will receive CSV, only available when create annotations
    }

    @Deprecated
    @Parameters(commandNames = {"create-annotations"}, commandDescription = "Create an annotation file.")
    class CommandCreateAnnotations extends Command {

        @Parameter(names = {"-b", "--backend"}, description = "Storage to save files into: mongodb (default) or hbase (pending)", required = false, arity = 1)
        String backend = "mongodb";

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials;

        @Parameter(names = {"-d", "--dbName"}, description = "OpenCGA DB name to read variants.", required = true, arity = 1)
        String dbName;

        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", required = false, arity = 1)
        String outdir = "./";

        @Parameter(names = {"-f", "--fileName"}, description = "Output file name. Default: dbName", required = false, arity = 1)
        String fileName = "";

        @Parameter(names = {"-s", "--annotation-source"}, description = "Annotation source", required = false, arity = 1)
        VariantAnnotationManager.AnnotationSource annotationSource = VariantAnnotationManager.AnnotationSource.CELLBASE_REST;

        @Parameter(names = {"--species"}, description = " ", required = true, arity = 1)
        String species;

        //Cellbase specific params

        @Parameter(names = {"--cellbase-assembly"}, description = " ", required = false, arity = 1)
        String cellbaseAssemly = "default";

        @Parameter(names = {"--cellbase-rest"}, description = "Cellbase REST URL.", required = false, arity = 1)
        String cellbaseRest = "";

        @Parameter(names = {"--cellbase-version"}, description = "Cellbase REST version. Default: v3", required = false, arity = 1)
        String cellbaseVersion = "v3";

        @Parameter(names = {"--cellbase-host"}, description = "Cellbase host.", required = false, arity = 1)
        String cellbaseHost;

        @Parameter(names = {"--cellbase-port"}, description = " ", required = false, arity = 1)
        int cellbasePort = 0;

        @Parameter(names = {"--cellbase-database"}, description = " ", required = false, arity = 1)
        String cellbaseDatabase;

        @Parameter(names = {"--cellbase-user"}, description = " ", required = false, arity = 1)
        String cellbaseUser;

        @Parameter(names = {"--cellbase-password"}, description = " ", required = false, arity = 1)
        String cellbasePassword;
    }


    @Deprecated
    @Parameters(commandNames = {"load-annotations"}, commandDescription = "Load an annotation file.")
    class CommandLoadAnnotations extends Command {

        @Parameter(names = {"-b", "--backend"}, description = "Storage to save files into: mongodb (default) or hbase (pending)", required = false, arity = 1)
        String backend = "mongodb";

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials;

        @Parameter(names = {"-d", "--dbName"}, description = "OpenCGA DB name to annotate variants.", required = true, arity = 1)
        String dbName;

        @Parameter(names = {"-i", "--input"}, description = "Input file name. ", required = true, arity = 1)
        String fileName = "";

    }

    @Parameters(commandNames = {"stats-variants"}, commandDescription = "Create and load stats into a database.")
    class CommandStatsVariants extends Command {

        @Parameter(names = {"--overwrite-stats"}, description = "Overwrite stats in variants already present")
        boolean overwriteStats = false;

        @Parameter(names = {"-s", "--study-id"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"-f", "--file-id"}, description = "Unique ID for the file", required = true, arity = 1)
        String fileId;

        @Parameter(names = {"-d", "--database"}, description = "DataBase name", required = false, arity = 1)
        String dbName;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials = "";

        @Parameter(names = {"-o", "--output-filename"}, description = "Output file name. Default: ./<study>_<file>.<timestamp>", required = false, arity = 1)
        String fileName = "";

        // this is confusing, lets try only with output-filename
//        @Parameter(names = {"-o", "--outdir"}, description = "Output directory.", required = false, arity = 1)
//        String outdir = ".";

        @Parameter(names = {"--create"}, description = "Run only the creation of the stats to a file")
        boolean create = false;
        @Parameter(names = {"--load"}, description = "Load the stats from an already existing FILE directly into the database. FILE is a prefix with structure <INPUT_FILENAME>.<TIME>")
        String load = null;

        @Parameter(names = {"--cohort-name"}, description = "Run stats for a subset of all the samples as well, requires \"cohort-samples\"", arity = 1)
        String cohortName = null;

        @Parameter(names = {"--cohort-samples"}, description = "CSV of the samples within the cohort, requires \"cohort-name\"", arity = 1)
        List<String> cohortSamples = null;

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

    String parse(String[] args) throws ParameterException {
        jcommander.parse(args);
        String parsedCommand = jcommander.getParsedCommand();
        return parsedCommand != null? parsedCommand: "";
    }

    Command getCommand() {
        String parsedCommand = jcommander.getParsedCommand();
        if (parsedCommand != null) {
            JCommander jCommander = jcommander.getCommands().get(parsedCommand);
            List<Object> objects = jCommander.getObjects();
            if (!objects.isEmpty() && objects.get(0) instanceof Command) {
                return ((Command) objects.get(0));
            }
        }
        return null;
    }

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

    CommandIndexVariants getCommandIndexVariants() {
        return commandIndexVariants;
    }

    CommandIndexAlignments getCommandIndexAlignments() {
        return commandIndexAlignments;
    }

    CommandIndexSequence getCommandIndexSequence() {
        return commandIndexSequence;
    }

    CommandCreateAccessions getAccessionsCommand() {
        return accessions;
    }

//    CommandLoadVariants getLoadCommand() {
//        return load;
//    }
//
//    CommandTransformVariants getTransformCommand() {
//        return transform;
//    }
//
//    CommandTransformAlignments getTransformAlignments() {
//        return transformAlignments;
//    }
//
//    CommandLoadAlignments getLoadAlignments() {
//        return loadAlignments;
//    }

//    CommandDownloadAlignments getDownloadAlignments() {
//        return downloadAlignments;
//    }

    CommandFetchVariants getCommandFetchVariants() {
        return commandFetchVariants;
    }

    CommandFetchAlignments getCommandFetchAlignments() {
        return commandFetchAlignments;
    }

    CommandAnnotateVariants getCommandAnnotateVariants() {
        return commandAnnotateVariants;
    }
    CommandStatsVariants getCommandStatsVariants() {
        return commandStatsVariants;
    }
//    CommandCreateAnnotations getCommandCreateAnnotations() {
//        return commandCreateAnnotations;
//    }

//    CommandLoadAnnotations getCommandLoadAnnotations() {
//        return commandLoadAnnotations;
//    }

    GeneralParameters getGeneralParameters() {
        return generalParameters;
    }
}
