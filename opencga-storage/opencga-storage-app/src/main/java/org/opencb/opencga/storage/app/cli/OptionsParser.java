package org.opencb.opencga.storage.app.cli;

import com.beust.jcommander.*;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantStudy;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 */
public class OptionsParser {
    private final JCommander jcommander;

    private final GeneralParameters generalParameters;
    private final CommandCreateAccessions accessions;
    private final CommandTransformVariants transform;
    private final CommandLoadVariants load;
    private final CommandTransformAlignments transformAlignments;
    private final CommandLoadAlignments loadAlignments;
    private final CommandIndexVariants commandIndexVariants;
    private final CommandIndexAlignments commandIndexAlignments;
    private final CommandIndexSequence commandIndexSequence;
    private final CommandFetchVariants commandFetchVariants;
    private final CommandFetchAlignments commandFetchAlignments;
    private final CommandAnnotateVariants commandAnnotatevariants;
//    private CommandDownloadAlignments downloadAlignments;

    public OptionsParser() {
        jcommander = new JCommander();
        jcommander.addObject(generalParameters = new GeneralParameters());
        jcommander.addCommand(accessions = new CommandCreateAccessions());
        jcommander.addCommand(transform = new CommandTransformVariants());
        jcommander.addCommand(load = new CommandLoadVariants());
        jcommander.addCommand(transformAlignments = new CommandTransformAlignments());
        jcommander.addCommand(loadAlignments = new CommandLoadAlignments());
        jcommander.addCommand(commandIndexVariants = new CommandIndexVariants());
        jcommander.addCommand(commandIndexAlignments = new CommandIndexAlignments());
        jcommander.addCommand(commandIndexSequence = new CommandIndexSequence());
        jcommander.addCommand(commandFetchVariants = new CommandFetchVariants());
        jcommander.addCommand(commandFetchAlignments = new CommandFetchAlignments());
        jcommander.addCommand(commandAnnotatevariants = new CommandAnnotateVariants());
//        jcommander.addCommand(downloadAlignments = new CommandDownloadAlignments());
    }

    interface Command {
    }

    class GeneralParameters implements Command {
        @Parameter(names = { "--properties-path" }, description = "Properties path")
        String propertiesPath = null;

        @Parameter(names = { "--sm-name" }, description = "StorageManager class name. (Must be in the classpath)")
        String storageManagerName;

        @Parameter(names = { "-h", "--help" }, description = "Print this help", help = true)
        boolean help;

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        private Map<String, String> params = new HashMap<String, String>();

        @Override
        public String toString() {
            String paramsString = "{";
            for(Map.Entry<String, String> e :  params.entrySet()){
                paramsString += e.getKey()+" = "+e.getValue() + ", ";
            }
            paramsString+="}";
            return "GeneralParameters{" +
                    "propertiesPath='" + propertiesPath + '\'' +
                    ", storageManagerName='" + storageManagerName + '\'' +
                    ", help=" + help +
                    ", params=" + paramsString +
                    '}';
        }
    }


    @Parameters(commandNames = {"create-accessions"}, commandDescription = "Creates accession IDs for an input file")
    class CommandCreateAccessions implements Command {

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


    @Parameters(commandNames = {"transform-variants"}, commandDescription = "Generates a data model from an input file")
    class CommandTransformVariants implements Command {

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

    @Parameters(commandNames = {"load-variants"}, commandDescription = "Loads an already generated data model into a backend")
    class CommandLoadVariants implements Command {

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
    
    @Parameters(commandNames = {"transform-alignments"}, commandDescription = "Generates the Alignment data model from an input file")
    class CommandTransformAlignments implements Command {

        @Parameter(names = {"-i", "--input"}, description = "File to transform into the OpenCGA data model", required = true, arity = 1)
        String file;

        @Parameter(names = {"-a", "--alias"}, description = "Unique ID for the file to be transformed", required = false, arity = 1)
        String fileId;

//        @Parameter(names = {"-s", "--study"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
//        String study;

      //  @Parameter(names = {"--study-alias"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
      //  String studyId;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved", arity = 1)
        String outdir = ".";
        
        
        @Parameter(names = {"--plain"}, description = "Do not compress the output (optional)", required = false)
        boolean plain = false;
        
        @Parameter(names = {"--include-coverage"}, description = "Save coverage information (optional)", required = false, arity = 0)
        boolean includeCoverage = false;
        
        //Acceptes values: ^[0-9]+(.[0-9]+)?[kKmMgG]?$  -->   <float>[KMG]
        @Parameter(names = "--mean-coverage", description = "Add mean coverage values (optional)", required = false)
        List<String> meanCoverage = new LinkedList<String>();
        
        
//        @Parameters(commandNames = {"--include-coverage"}, commandDescription = "Save coverage information (optional)")
//        class CommandCoverage implements Command {
//            @Parameter(names = {"--plain"}, description = "Do not compress the output (optional)", required = false)
//            boolean plain = false;
//        }
        
//        @Parameter(names = {"--compact"}, description = "Compact sequence differences with reference genome (optional)", required = false, arity = 0)
//        boolean compact = false;


    }
    
    @Parameters(commandNames = {"load-alignments"}, commandDescription = "Loads an already generated data model into a backend")
    class CommandLoadAlignments implements Command {
        
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
    class CommandDownloadAlignments implements Command {
        
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

    class CommandIndex implements Command {

        @Parameter(names = {"-i", "--input"}, description = "File to index in the selected backend", required = true, arity = 1)
        String input;

//        @Parameter(names = {"-T", "--temporal-dir"}, description = "Directory where place temporal files (pending)", required = false, arity = 1)
//        String tmp = "";

//        @Parameter(names = {"--delete-temporal"}, description = "Delete temporal files (pending)", required = false)
//        boolean delete = false;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved (optional)", arity = 1, required = false)
        String outdir = "";

        @Parameter(names = {"-a", "--alias"}, description = "Unique ID for the file", required = true, arity = 1)
        String fileId;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials = "";

        @Parameter(names = {"-b", "--backend"}, description = "StorageManager plugin used to index files into: mongodb (default), hbase (pending)", required = false, arity = 1)
        String backend = "mongodb";

        @Parameter(names = {"-d", "--dbName"}, description = "DataBase name", required = false, arity = 1)
        String dbName;

        @DynamicParameter(names = "-D", description = "Dynamic parameters go here", hidden = true)
        Map<String, String> params = new HashMap<>();
    }

    @Parameters(commandNames = {"index-variants"}, commandDescription = "Index variants file")
    class CommandIndexVariants extends CommandIndex implements Command {

        @Parameter(names = {"-s", "--study"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        String study;

        @Parameter(names = {"--study-alias"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
        String studyId;

        @Parameter(names = {"-p", "--pedigree"}, description = "File containing pedigree information (in PED format, optional)", arity = 1)
        String pedigree;

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

    @Parameters(commandNames = {"index-alignments"}, commandDescription = "Index alignment file")
    class CommandIndexAlignments extends CommandIndex implements Command {

        //Acceptes values: ^[0-9]+(.[0-9]+)?[kKmMgG]?$  -->   <float>[KMG]
        @Parameter(names = "--mean-coverage", description = "Add mean coverage values (optional)", required = false)
        List<String> meanCoverage = new LinkedList<String>();

    }

    @Parameters(commandNames = {"index-sequence"}, commandDescription = "Index sequence file")
    class CommandIndexSequence extends CommandIndex implements Command {

    }

    class CommandFetch implements Command {
        //File location parameters
        @Parameter(names = {"-b", "--backend"}, description = "StorageManager plugin used to index files into: mongodb (default), hbase (pending)", required = false, arity = 1)
        String backend = "mongodb";

        @Parameter(names = {"-d", "--dbName"}, description = "DataBase name", required = false, arity = 1)
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

    @Parameters(commandNames = {"fetch-variants", "search-variants"}, commandDescription = "Search over indexed variants")
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


    @Parameters(commandNames = {"fetch-alignments", "search-alignments"}, commandDescription = "Search over indexed alignments")
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

    @Parameters(commandNames = {"annotate-variants"}, commandDescription = "Search over indexed alignments")
    class CommandAnnotateVariants implements Command {

        //Filter parameters
        @Parameter(names = {"--cellbase-host"}, description = "File unique ID.", required = true, arity = 1)
        String cellbaseHost;

        @Parameter(names = {"--cellbase-port"}, description = "File unique ID.", required = true, arity = 1)
        String cellbasePort;

        @Parameter(names = {"--cellbase-database"}, description = "File unique ID.", required = true, arity = 1)
        String cellbaseDatabase;

        @Parameter(names = {"--cellbase-user"}, description = "File unique ID.", required = true, arity = 1)
        String cellbaseUser;

        @Parameter(names = {"--cellbase-password"}, description = "File unique ID.", required = true, arity = 1)
        String cellbasePassword;

        @Parameter(names = {"--cellbase-species"}, description = "File unique ID.", required = true, arity = 1)
        String cellbaseSpecies;

        @Parameter(names = {"--cellbase-assembly"}, description = "File unique ID.", required = true, arity = 1)
        String cellbaseAssemly;


        @Parameter(names = {"--opencga-host"}, description = "File unique ID.", required = true, arity = 1)
        String opencgaHost;

        @Parameter(names = {"--opencga-port"}, description = "File unique ID.", required = true, arity = 1)
        String opencgaPort;

        @Parameter(names = {"--opencga-database"}, description = "File unique ID.", required = true, arity = 1)
        String opencgaDatabase;

        @Parameter(names = {"--opencga-user"}, description = "File unique ID.", required = true, arity = 1)
        String opencgaUser;

        @Parameter(names = {"--opencga-password"}, description = "File unique ID.", required = true, arity = 1)
        String opencgaPassword;

    }


    String parse(String[] args) throws ParameterException {
        jcommander.parse(args);
        String parsedCommand = jcommander.getParsedCommand();
        return parsedCommand != null? parsedCommand: "";
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

    CommandLoadVariants getLoadCommand() {
        return load;
    }

    CommandTransformVariants getTransformCommand() {
        return transform;
    }

    CommandTransformAlignments getTransformAlignments() {
        return transformAlignments;
    }

    CommandLoadAlignments getLoadAlignments() {
        return loadAlignments;
    }

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
        return commandAnnotatevariants;
    }

    GeneralParameters getGeneralParameters() {
        return generalParameters;
    }
}
