package org.opencb.opencga.app.cli;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.opencb.commons.bioformats.pedigree.io.readers.PedDataReader;
import org.opencb.commons.bioformats.pedigree.io.readers.PedFileDataReader;
import org.opencb.commons.bioformats.variant.VariantStudy;
import org.opencb.commons.bioformats.variant.vcf4.io.VariantDBWriter;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantDataReader;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantVcfDataReader;
import org.opencb.commons.utils.OptionFactory;
import org.opencb.opencga.lib.auth.MonbaseCredentials;
import org.opencb.opencga.lib.auth.OpenCGACredentials;
import org.opencb.opencga.storage.variant.VariantVcfMonbaseDataWriter;
import org.opencb.opencga.storage.variant.VariantVcfSqliteWriter;
import org.opencb.variant.lib.runners.VariantEffectRunner;
import org.opencb.variant.lib.runners.VariantIndexRunner;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.VariantStatsRunner;

/**
 * @author Cristina Yenyxe Gonzalez Garcia
 */
public class OpenCGAMain {

    private static Options options;

    private Logger logger;

    private static void initOptions() {
        options = new Options();

        options.addOption(OptionFactory.createOption("help", "h", "Print this message", false, false));
        
        options.addOption(OptionFactory.createOption("backend", "b", "Storage to save files into: sqlite (default) or monbase", false, true));
        options.addOption(OptionFactory.createOption("credentials", "c", "Path to the file where the backend credentials are stored", true, true));
        options.addOption(OptionFactory.createOption("datatype", "d", "Datatype to be stored: alignments (BAM) or variants (VCF)", true, true));
        options.addOption(OptionFactory.createOption("file", "f", "File to save in the selected backend", true, true));
        options.addOption(OptionFactory.createOption("study", "s", "File containing study metadata", true, true)); // TODO Only name at the moment
        
        // Alignments optional arguments
        options.addOption(OptionFactory.createOption("include-coverage", "Save coverage information (optional)", false, false));
        
        // Variants optional arguments
        options.addOption(OptionFactory.createOption("include-effect", "Save variant effect information (optional)", false, false));
        options.addOption(OptionFactory.createOption("include-stats", "Save statistics information (optional)", false, false));
        options.addOption(OptionFactory.createOption("pedigree", "File containing pedigree information (in PED format)", false, true));
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        if(args.length > 0 && (args[0].equals("-h") || args[0].equals("--help"))) {
            printHelp();
            return;
        }
        
        initOptions();

        CommandLine commandLine = parse(args, false);
        
        // Get global arguments
        String backend = commandLine.hasOption("backend") ? commandLine.getOptionValue("backend") : "sqlite";
        Path credentialsPath = Paths.get(commandLine.getOptionValue("credentials"));
        Path filePath = Paths.get(commandLine.getOptionValue("file"));
        // TODO check filePath exists
        String studyName = commandLine.hasOption("study") ? commandLine.getOptionValue("study") : "study_" + filePath.getFileName();
        
        // Get arguments for each datatype to store
        switch (commandLine.getOptionValue("datatype").toLowerCase()) {
            case "alignments":
                boolean includeCoverage = commandLine.hasOption("include-coverage");
                // TODO
                indexAlignments(studyName, filePath, backend, credentialsPath, includeCoverage);
                break;
            case "variants":
                boolean includeEffect = commandLine.hasOption("include-effect");
                boolean includeStats = commandLine.hasOption("include-stats");
                Path pedigreePath = commandLine.hasOption("pedigree") ? Paths.get(commandLine.getOptionValue("pedigree")) : null;
                VariantStudy study = new VariantStudy(studyName, studyName.substring(5), studyName, null, null);
                
                indexVariants(study, filePath, pedigreePath, backend, credentialsPath, includeEffect, includeStats);
                break;
            default:
                System.out.println("Datatype " + commandLine.getOptionValue("datatype") + " is not supported");
                System.exit(2);
        }    
    }
    
    
    private static CommandLine parse(String[] args, boolean stopAtNoOption) {
        CommandLineParser parser = new PosixParser();

        try {
            return parser.parse(options, args, stopAtNoOption);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            printHelp();
            System.exit(1);
        }
        
        return null;
    }
    
    private static void printHelp() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("opencga-index", 
                "You must specify at least the datatype to store, the file to read from and the storage credentials. " +
                "Please note that SQLite is the default storage backend.", 
                options, "\nFor more information or reporting a bug, please contact: imedina@cipf.es", true);
    }
    
    private static void indexAlignments(String study, Path filePath, String backend, Path credentialsPath, boolean includeCoverage) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private static void indexVariants(VariantStudy study, Path filePath, Path pedigreePath, String backend, Path credentialsPath, 
            boolean includeEffect, boolean includeStats) throws IOException {
        VariantRunner vr = null;
        VariantDataReader reader = new VariantVcfDataReader(filePath.toString());
        PedDataReader pedReader = pedigreePath != null ? new PedFileDataReader(pedigreePath.toString()) : null;
        
        VariantDBWriter writer = null;
        OpenCGACredentials credentials = null;
        Properties properties = new Properties();
        properties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
        
        if (backend.equalsIgnoreCase("sqlite")) {
            writer = new VariantVcfSqliteWriter(properties.getProperty("db_path")); // TODO Use SQLiteCredentials class
        } else if (backend.equalsIgnoreCase("monbase")) {
            credentials = new MonbaseCredentials(properties);
            writer = new VariantVcfMonbaseDataWriter(study.getName(), "hsapiens", (MonbaseCredentials) credentials);
        }
        
        if (includeEffect) { vr = new VariantEffectRunner(study, reader, pedReader, writer, vr); }
        if (includeStats)  { vr = new VariantStatsRunner(study, reader, pedReader, writer, vr); }
        vr = new VariantIndexRunner(study, reader, pedReader, writer, vr);
        
        System.out.println("Indexing variants...");
        vr.run();
        System.out.println("Variants indexed!");
    }
}
