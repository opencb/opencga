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
import org.opencb.commons.bioformats.pedigree.io.readers.PedigreePedReader;
import org.opencb.commons.bioformats.pedigree.io.readers.PedigreeReader;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.VariantSource;
import org.opencb.commons.bioformats.variant.annotators.VariantAnnotator;
import org.opencb.commons.bioformats.variant.annotators.VariantControlMongoAnnotator;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantReader;
import org.opencb.commons.bioformats.variant.vcf4.io.readers.VariantVcfReader;
import org.opencb.commons.bioformats.variant.vcf4.io.writers.VariantJsonDataWriter;
import org.opencb.commons.bioformats.variant.vcf4.io.writers.VariantTabFileDataWriter;
import org.opencb.commons.bioformats.variant.vcf4.io.writers.VariantWriter;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.run.Task;
import org.opencb.commons.utils.OptionFactory;
import org.opencb.opencga.lib.auth.*;
import org.opencb.opencga.storage.variant.VariantVcfMonbaseDataWriter;
import org.opencb.opencga.storage.variant.VariantVcfMongoDataWriter;
import org.opencb.opencga.storage.variant.VariantVcfSqliteWriter;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.tasks.VariantEffectTask;
import org.opencb.variant.lib.runners.tasks.VariantStatsTask;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Cristina Yenyxe Gonzalez Garcia
 */
public class OpenCGAMain {

    private static Options options;

    private static void initOptions() {
        options = new Options();

        options.addOption(OptionFactory.createOption("help", "h", "Print this message", false, false));

        options.addOption(OptionFactory.createOption("alias", "a", "Unique ID for the file to be uploaded", true, true));
        options.addOption(OptionFactory.createOption("backend", "b", "Storage to save files into: sqlite (default), mongo, monbase, json or json-gzip", false, true));
        options.addOption(OptionFactory.createOption("credentials", "c", "Path to the file where the backend credentials are stored", true, true));
        options.addOption(OptionFactory.createOption("datatype", "d", "Datatype to be stored: alignments (BAM) or variants (VCF)", true, true));
        options.addOption(OptionFactory.createOption("file", "f", "File to save in the selected backend", true, true));

        // Alignments optional arguments
        options.addOption(OptionFactory.createOption("include-coverage", "Save coverage information (optional)", false, false));

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
        String alias = commandLine.getOptionValue("alias");

        // Get arguments for each datatype to store
        switch (commandLine.getOptionValue("datatype").toLowerCase()) {
            case "alignments":
                boolean includeCoverage = commandLine.hasOption("include-coverage");
                // TODO
                indexAlignments(alias, filePath, backend, credentialsPath, includeCoverage);
                break;
            case "variants":
                boolean includeEffect = commandLine.hasOption("include-effect");
                boolean includeStats = commandLine.hasOption("include-stats");
                boolean includeSamples = commandLine.hasOption("include-samples");
                Path pedigreePath = commandLine.hasOption("pedigree") ? Paths.get(commandLine.getOptionValue("pedigree")) : null;
                VariantSource source = new VariantSource(filePath.getFileName().toString(), alias, filePath.getFileName().toString(), null, null);

                indexVariants(source, filePath, pedigreePath, backend, credentialsPath, includeEffect, includeStats, includeSamples);
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
                options, "\nFor more information or reporting a bug, please contact: imedina@cipf.es", true
        );
    }

    private static void indexAlignments(String study, Path filePath, String backend, Path credentialsPath, boolean includeCoverage) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private static void indexVariants(VariantSource source, Path filePath, Path pedigreePath, String backend, Path credentialsPath,
                                      boolean includeEffect, boolean includeStats, boolean includeSamples) throws IOException, IllegalOpenCGACredentialsException {

        VariantRunner vr;
        VariantReader reader;
        PedigreeReader pedReader = pedigreePath != null ? new PedigreePedReader(pedigreePath.toString()) : null;

        reader = new VariantVcfReader(filePath.toString());

        List<VariantWriter> writers = new ArrayList<>();
        OpenCGACredentials credentials;
        Properties properties = new Properties();
        properties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));

        List<Task<Variant>> taskList = new SortedList<>();

        if (backend.equalsIgnoreCase("sqlite")) {
            credentials = new SqliteCredentials(properties);
            writers.add(new VariantVcfSqliteWriter((SqliteCredentials) credentials));
        } else if (backend.equalsIgnoreCase("monbase")) {
            credentials = new MonbaseCredentials(properties);
            writers.add(new VariantVcfMonbaseDataWriter(source, "opencga-hsapiens", (MonbaseCredentials) credentials));
        } else if (backend.equalsIgnoreCase("mongo")) {
            credentials = new MongoCredentials(properties);
            writers.add(new VariantVcfMongoDataWriter(source, "opencga-hsapiens", (MongoCredentials) credentials));
        }
//       else if (backend.equalsIgnoreCase("json")) {
//            writers.add(new VariantJsonDataWriter(source, "out.json"));
//        } else if (backend.equalsIgnoreCase("json-gzip")) {
//            writers.add(new VariantJsonDataWriter(source, "out.json.gz", true));
//        } else if (backend.equalsIgnoreCase("tab-file")) {
//            writers.add(new VariantTabFileDataWriter(source, filePath.getFileName().toString() + ".tab"));
//        }

        if (includeEffect) {
            taskList.add(new VariantEffectTask());
        }

        if (includeStats) {
            taskList.add(new VariantStatsTask(reader, source));
        }

        for (VariantWriter variantWriter : writers) {
            variantWriter.includeSamples(includeSamples);
            variantWriter.includeEffect(includeEffect);
            variantWriter.includeStats(includeStats);
        }

        vr = new VariantRunner(source, reader, pedReader, writers, taskList);

        System.out.println("Indexing variants...");
        vr.run();
        System.out.println("Variants indexed!");
    }
}
