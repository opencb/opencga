package org.opencb.opencga.app.cli;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.commons.cli.*;
import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.run.Task;
import org.opencb.commons.utils.OptionFactory;
import org.opencb.opencga.lib.auth.*;
import org.opencb.opencga.storage.variant.json.VariantJsonReader;
import org.opencb.opencga.storage.variant.json.VariantJsonWriter;
//import org.opencb.opencga.storage.variant.VariantVcfMonbaseDataWriter;
import org.opencb.opencga.storage.variant.mongodb.VariantMongoWriter;
//import org.opencb.opencga.storage.variant.VariantVcfSqliteWriter;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.tasks.VariantEffectTask;
import org.opencb.variant.lib.runners.tasks.VariantStatsTask;

/**
 * @author Cristina Yenyxe Gonzalez Garcia
 */
public class OpenCGAMain {

    private static Options options;

    private static void initOptions() {
        options = new Options();

        options.addOption(OptionFactory.createOption("help", "h", "Print this message", false, false));

        options.addOption(OptionFactory.createOption("file", "f", "File to save in the selected backend", true, true));
        options.addOption(OptionFactory.createOption("alias", "a", "Unique ID for the file to be uploaded", true, true));
        options.addOption(OptionFactory.createOption("study", "s", "Full name of the study where the file is classified", true, true));
        options.addOption(OptionFactory.createOption("study-alias", "Unique ID for the study where the file is classified", true, true));
        
        options.addOption(OptionFactory.createOption("backend", "b", "Storage to save files into: sqlite (default) or monbase", false, true));
        options.addOption(OptionFactory.createOption("credentials", "c", "Path to the file where the backend credentials are stored", true, true));
        options.addOption(OptionFactory.createOption("datatype", "d", "Datatype to be stored: alignments (BAM) or variants (VCF)", true, true));
        options.addOption(OptionFactory.createOption("outdir", "o", "Directory where output files will be saved (if applies)", false, true));

//        // Alignments optional arguments
//        options.addOption(OptionFactory.createOption("include-coverage", "Save coverage information (optional)", false, false));

        // Variants optional arguments
        options.addOption(OptionFactory.createOption("include-effect", "Save variant effect information (optional)", false, false));
        options.addOption(OptionFactory.createOption("include-stats", "Save statistics information (optional)", false, false));
        options.addOption(OptionFactory.createOption("include-samples", "Save samples information (optional)", false, false));
        options.addOption(OptionFactory.createOption("pedigree", "File containing pedigree information (in PED format)", false, true));
    }


    public static void main(String[] args) throws IOException, InterruptedException, IllegalOpenCGACredentialsException {
        if (args.length > 0 && (args[0].equals("-h") || args[0].equals("--help"))) {
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
        String fileId = commandLine.getOptionValue("alias");
        String study = commandLine.getOptionValue("study");
        String studyId = commandLine.getOptionValue("study-alias");
        Path outdir = commandLine.hasOption("outdir") ? Paths.get(commandLine.getOptionValue("outdir")) : null;

        // Get arguments for each datatype to store
        switch (commandLine.getOptionValue("datatype").toLowerCase()) {
            case "alignments":
                boolean includeCoverage = commandLine.hasOption("include-coverage");
                // TODO
                indexAlignments(fileId, filePath, backend, credentialsPath, includeCoverage);
                break;
            case "variants":
                boolean includeEffect = commandLine.hasOption("include-effect");
                boolean includeStats = commandLine.hasOption("include-stats");
                boolean includeSamples = commandLine.hasOption("include-samples");
                Path pedigreePath = commandLine.hasOption("pedigree") ? Paths.get(commandLine.getOptionValue("pedigree")) : null;
                VariantSource source = new VariantSource(filePath.getFileName().toString(), fileId, study, studyId);

                indexVariants(source, filePath, pedigreePath, outdir, backend, credentialsPath, includeEffect, includeStats, includeSamples);
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
                options, "\nFor more information or reporting a bug, please contact: opencb@googlegroups.com", true);
    }

    private static void indexAlignments(String study, Path filePath, String backend, Path credentialsPath, boolean includeCoverage) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private static void indexVariants(VariantSource source, Path filePath, Path pedigreePath, Path outdir, String backend, 
                                      Path credentialsPath, boolean includeEffect, boolean includeStats, boolean includeSamples) 
            throws IOException, IllegalOpenCGACredentialsException {

        VariantReader reader;
        PedigreeReader pedReader = pedigreePath != null ? new PedigreePedReader(pedigreePath.toString()) : null;

        if (source.getFileName().endsWith(".vcf") || source.getFileName().endsWith(".vcf.gz")) {
            reader = new VariantVcfReader(filePath.toAbsolutePath().toString(), source.getFileId(), source.getStudyId());
        } else if (source.getFileName().endsWith(".json") || source.getFileName().endsWith(".json.gz")) {
            reader = new VariantJsonReader(filePath.toAbsolutePath().toString());
        } else {
            throw new IOException("Variants input file format not supported");
        }

        List<VariantWriter> writers = new ArrayList<>();
        OpenCGACredentials credentials;
        Properties properties = new Properties();
        properties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
        
//        List<VariantAnnotator> annots = new ArrayList<>();
//        annots.add(new VariantControlMongoAnnotator());

        List<Task<Variant>> taskList = new SortedList<>();

        // TODO Restore when SQLite and Monbase are once again ready!!
        if (backend.equalsIgnoreCase("mongo")) {
            credentials = new MongoCredentials(properties);
            writers.add(new VariantMongoWriter(source, (MongoCredentials) credentials));
        } else if (backend.equalsIgnoreCase("json")) {
//            credentials = new MongoCredentials(properties);
            writers.add(new VariantJsonWriter(source, outdir));
        }/* else if (backend.equalsIgnoreCase("sqlite")) {
            credentials = new SqliteCredentials(properties);
            writers.add(new VariantVcfSqliteWriter((SqliteCredentials) credentials));
        } else if (backend.equalsIgnoreCase("monbase")) {
            credentials = new MonbaseCredentials(properties);
            writers.add(new VariantVcfMonbaseDataWriter(source, "opencga-hsapiens", (MonbaseCredentials) credentials));// TODO Restore when SQLite and Monbase are once again ready!!
        } */ 


        // If a JSON file is provided, then stats and effects do not need to be recalculated
        if (!source.getFileName().endsWith(".json") && !source.getFileName().endsWith(".json.gz")) {
            if (includeEffect) {
                taskList.add(new VariantEffectTask());
            }

            if (includeStats) {
                taskList.add(new VariantStatsTask(reader, source));
            }
        }
        
        for (VariantWriter variantWriter : writers) {
            variantWriter.includeSamples(includeSamples);
            variantWriter.includeEffect(includeEffect);
            variantWriter.includeStats(includeStats);
        }

        VariantRunner vr = new VariantRunner(source, reader, pedReader, writers, taskList);

        System.out.println("Indexing variants...");
        vr.run();
        System.out.println("Variants indexed!");
    }
}
