package org.opencb.opencga.app.cli;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class OptionsParser {
    
//    private Options options;
    private JCommander jcommander;
    
    private CommandTransformVariants transformVariants;
    private CommandLoadVariants loadVariants;
    private CommandTransformAlignments transformAlignments;
    private CommandLoadAlignments loadAlignments;
    private CommandDownloadAlignments downloadAlignments;

    public OptionsParser() {
        jcommander = new JCommander();
        jcommander.addCommand(transformVariants = new CommandTransformVariants());
        jcommander.addCommand(loadVariants = new CommandLoadVariants());
        jcommander.addCommand(transformAlignments = new CommandTransformAlignments());
        jcommander.addCommand(loadAlignments = new CommandLoadAlignments());
        jcommander.addCommand(downloadAlignments = new CommandDownloadAlignments());
    }
    
    interface Command {}
    
    @Parameters(commandNames = { "transform-variants" }, commandDescription = "Generates a data model from an input file")
    class CommandTransformVariants implements Command {
        
        @Parameter(names = { "-i", "--input" }, description = "File to transform into the OpenCGA data model", required = true, arity = 1)
        String file;
        
        @Parameter(names = { "-a", "--alias" }, description = "Unique ID for the file to be transformed", required = true, arity = 1)
        String fileId;
        
        @Parameter(names = { "-s", "--study" }, description = "Full name of the study where the file is classified", required = true, arity = 1)
        String study;
        
        @Parameter(names = { "--study-alias" }, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
        String studyId;
        
        @Parameter(names = { "-p", "--pedigree" }, description = "File containing pedigree information (in PED format, optional)", arity = 1)
        String pedigree;
        
        @Parameter(names = { "-o", "--outdir" }, description = "Directory where output files will be saved", arity = 1)
        String outdir;
        
        @Parameter(names = { "--include-effect" }, description = "Save variant effect information (optional)")
        boolean includeEffect = false;
        
        @Parameter(names = { "--include-samples" }, description = "Save samples information (optional)")
        boolean includeSamples = false;
        
        @Parameter(names = { "--include-stats" }, description = "Save statistics information (optional)")
        boolean includeStats = false;
        
    }
    
    @Parameters(commandNames = { "load-variants" }, commandDescription = "Loads an already generated data model into a backend")
    class CommandLoadVariants implements Command {
        
        @Parameter(names = { "-i", "--input" }, description = "Prefix of files to save in the selected backend", required = true, arity = 1)
        String input;
        
        @Parameter(names = { "-b", "--backend" }, description = "Storage to save files into: mongo (default) or hbase (pending)", required = false, arity = 1)
        String backend;
        
        @Parameter(names = { "-c", "--credentials" }, description = "Path to the file where the backend credentials are stored", required = true, arity = 1)
        String credentials;
        
        @Parameter(names = { "--include-effect" }, description = "Save variant effect information (optional)")
        boolean includeEffect = false;
        
        @Parameter(names = { "--include-samples" }, description = "Save samples information (optional)")
        boolean includeSamples = false;
        
        @Parameter(names = { "--include-stats" }, description = "Save statistics information (optional)")
        boolean includeStats = false;
        
    }
    
    @Parameters(commandNames = {"transform-alignments"}, commandDescription = "Generates the Alignment data model from an input file")
    class CommandTransformAlignments implements Command {

        @Parameter(names = {"-i", "--input"}, description = "File to transform into the OpenCGA data model", required = true, arity = 1)
        String file;

        @Parameter(names = {"-a", "--alias"}, description = "Unique ID for the file to be transformed", required = true, arity = 1)
        String fileId;

        @Parameter(names = {"-s", "--study"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
        String study;

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
        
//        @Parameter(names = {"-a", "--alias"}, description = "Unique ID for the file to be loaded", required = true, arity = 1)
//        String fileId;
        
//        @Parameter(names = {"-i", "--input"}, description = "Prefix of files to save in the selected backend", required = true, arity = 1)
//        String input;

        @Parameter(names = {"-i", "--input"}, description = "Main file to save in the selected backend", required = true, arity = 1)
        String input;
        
//        @Parameter(names = {"--indir"}, description = "Directory of the input files", required = false, arity = 1)
//        String dir = "";

//        @Parameter(names = {"-s", "--study"}, description = "Full name of the study where the file is classified", required = true, arity = 1)
//        String study;

//        @Parameter(names = {"--study-alias"}, description = "Unique ID for the study where the file is classified", required = true, arity = 1)
//        String studyId;
        
//        @Parameter(names = {"--plain"}, description = "Do not compress the output (optional)", required = false)
//        boolean plain = false;
        
        @Parameter(names = {"-b", "--backend"}, description = "Storage to save files into: hbase (default) or hive (pending)", required = false, arity = 1)
        String backend = "hbase";

        @Parameter(names = {"-c", "--credentials"}, description = "Path to the file where the backend credentials are stored", required = true, arity = 1)
        String credentials;
        
        @Parameter(names = {"--include-coverage"}, description = "Save coverage information (optional)", required = false, arity = 0)
        boolean includeCoverage = false;
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
    
    
    String parse(String[] args) throws ParameterException {
        jcommander.parse(args);
        return jcommander.getParsedCommand();
    }
    
    String usage() {
        StringBuilder builder = new StringBuilder();
        jcommander.usage(builder);
        return builder.toString();
    }

    CommandTransformVariants getTransformVariants() {
        return transformVariants;
    }

    CommandLoadVariants getLoadVariants() {
        return loadVariants;
    }
    
    CommandTransformAlignments getTransformAlignments() {
        return transformAlignments;
    }

    CommandLoadAlignments getLoadAlignments() {
        return loadAlignments;
    }

    CommandDownloadAlignments getDownloadAlignments() {
        return downloadAlignments;
    }
    

}
