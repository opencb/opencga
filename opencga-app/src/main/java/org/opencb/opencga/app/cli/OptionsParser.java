package org.opencb.opencga.app.cli;

import com.beust.jcommander.IValueValidator;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

/**
 *
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class OptionsParser {
    
    private final JCommander jcommander;
    
    private final CommandCreateAccessions accessions;
    private final CommandTransformVariants transform;
    private final CommandLoadVariants load;

    public OptionsParser() {
        jcommander = new JCommander();
        jcommander.addCommand(accessions = new CommandCreateAccessions());
        jcommander.addCommand(transform = new CommandTransformVariants());
        jcommander.addCommand(load = new CommandLoadVariants());
    }
    
    interface Command {}
          
    @Parameters(commandNames = { "create-accessions" }, commandDescription = "Creates accession IDs for an input file")
    class CommandCreateAccessions implements Command {
        
        @Parameter(names = { "-i", "--input" }, description = "File to annotation with accession IDs", required = true, arity = 1)
        String input;
        
        @Parameter(names = { "-p", "--prefix" }, description = "Accession IDs prefix", arity = 1)
        String prefix;
        
        @Parameter(names = { "-s", "--study-alias" }, description = "Unique ID for the study where the file is classified (used for prefixes)", 
                required = true, arity = 1)//, validateValueWith = StudyIdValidator.class)
        String studyId;
        
        @Parameter(names = { "-r", "--resume-from-accession" }, description = "Starting point to generate accessions (will not be included)", arity = 1)
        String resumeFromAccession;
        
        @Parameter(names = { "-o", "--outdir" }, description = "Directory where the output file will be saved", arity = 1)
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
    
    String parse(String[] args) throws ParameterException {
        jcommander.parse(args);
        return jcommander.getParsedCommand();
    }
    
    String usage() {
        StringBuilder builder = new StringBuilder();
        jcommander.usage(builder);
        return builder.toString();
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

}
