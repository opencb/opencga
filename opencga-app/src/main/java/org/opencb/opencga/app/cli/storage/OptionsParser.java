package org.opencb.opencga.app.cli.storage;

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
    private final CommandIndex commandIndex;
//    private CommandDownloadAlignments downloadAlignments;

    public OptionsParser() {
        jcommander = new JCommander();
        jcommander.addObject(generalParameters = new GeneralParameters());
        jcommander.addCommand(accessions = new CommandCreateAccessions());
        jcommander.addCommand(transform = new CommandTransformVariants());
        jcommander.addCommand(load = new CommandLoadVariants());
        jcommander.addCommand(transformAlignments = new CommandTransformAlignments());
        jcommander.addCommand(loadAlignments = new CommandLoadAlignments());
        jcommander.addCommand(commandIndex = new CommandIndex());
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

        @Parameter(names = {"-b", "--backend"}, description = "Storage to save files into: mongo (default) or hbase (pending)", required = false, arity = 1)
        String backend;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = true, arity = 1)
        String credentials;

        @Parameter(names = {"--include-effect"}, description = "Save variant effect information (optional)")
        boolean includeEffect = false;

        @Parameter(names = {"--include-samples"}, description = "Save samples information (optional)")
        boolean includeSamples = false;

        @Parameter(names = {"--include-stats"}, description = "Save statistics information (optional)")
        boolean includeStats = false;

        @Parameter(names = {"-t", "--study-type"}, description = "Study type (optional)", arity = 1)
        VariantStudy.StudyType studyType = VariantStudy.StudyType.CASE_CONTROL;
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

    @Parameters(commandNames = {"index"}, commandDescription = "Index file")
    class CommandIndex implements Command {

        @Parameter(names = {"-i", "--input"}, description = "File to index in the selected backend", required = true, arity = 1)
        String input;

        @Parameter(names = {"-t", "--temporal-dir"}, description = "Directory where place temporal files", required = false, arity = 1)
        String tmp = "/tmp";

        @Parameter(names = {"--delete-temporal"}, description = "Delete temporal files (TODO)", required = false)
        boolean delete = false;

        @Parameter(names = {"-o", "--outdir"}, description = "Directory where output files will be saved (optional)", arity = 1)
        String outdir;

        @Parameter(names = {"-a", "--alias"}, description = "Unique ID for the file", required = true, arity = 1)
        String fileId;

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = false, arity = 1)
        String credentials = "";

        @Parameter(names = {"-b", "--backend"}, description = "StorageManager plugin used to index files into: mongo (default), hbase (pending)", required = false, arity = 1)
        String backend = "mongo";

        @Parameter(names = {"-d", "--dbName"}, description = "DataBase name", required = false, arity = 1)
        String dbName;

    }


    String parse(String[] args) throws ParameterException {
        jcommander.parse(args);
        String parsedCommand = jcommander.getParsedCommand();
        return parsedCommand != null? parsedCommand: "";
    }

    String usage() {
        StringBuilder builder = new StringBuilder();
        String parsedCommand = jcommander.getParsedCommand();
        if(parsedCommand != null || !parsedCommand.isEmpty()){
            jcommander.usage(parsedCommand, builder);
        } else {
            jcommander.usage(builder);
        }
        return builder.toString();//.replaceAll("\\^.*Default: false\\$\n", "");
    }

    public CommandIndex getCommandIndex() {
        return commandIndex;
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

    GeneralParameters getGeneralParameters() {
        return generalParameters;
    }
}
