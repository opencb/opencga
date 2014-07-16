package org.opencb.opencga.app.cli;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

/**
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 */
public class OptionsParser {

    private JCommander jcommander;

    private CommandTransformVariants transform;
    private CommandLoadVariants load;

    public OptionsParser() {
        jcommander = new JCommander();
        jcommander.addCommand(transform = new CommandTransformVariants());
        jcommander.addCommand(load = new CommandLoadVariants());
    }

    interface Command {
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
        String aggregated;

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

    CommandTransformVariants getTransform() {
        return transform;
    }

    CommandLoadVariants getLoad() {
        return load;
    }

}
