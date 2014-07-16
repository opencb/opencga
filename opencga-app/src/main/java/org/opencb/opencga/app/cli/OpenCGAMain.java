package org.opencb.opencga.app.cli;


import com.beust.jcommander.ParameterException;
import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantAggregatedVcfFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantVcfEVSFactory;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.run.Task;
import org.opencb.opencga.app.cli.OptionsParser.Command;
import org.opencb.opencga.app.cli.OptionsParser.CommandLoadVariants;
import org.opencb.opencga.app.cli.OptionsParser.CommandTransformVariants;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.auth.MongoCredentials;
import org.opencb.opencga.lib.auth.OpenCGACredentials;
import org.opencb.opencga.storage.variant.json.VariantJsonReader;
import org.opencb.opencga.storage.variant.json.VariantJsonWriter;
import org.opencb.opencga.storage.variant.mongodb.VariantMongoWriter;
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
import java.util.logging.Level;
import java.util.logging.Logger;

//import org.opencb.opencga.storage.variant.VariantVcfHbaseWriter;

/**
 * @author Cristina Yenyxe Gonzalez Garcia
 */
public class OpenCGAMain {

    public static void main(String[] args) throws IOException, InterruptedException, IllegalOpenCGACredentialsException {
        OptionsParser parser = new OptionsParser();
        if (args.length > 0 && (args[0].equals("-h") || args[0].equals("--help"))) {
            System.out.println(parser.usage());
            return;
        }

        Command command = null;
        try {
            switch (parser.parse(args)) {
                case "load-variants":
                    command = parser.getLoad();
                    break;
                case "transform-variants":
                    command = parser.getTransform();
                    break;
                default:
                    System.out.println("Command not implemented");
                    System.exit(1);
            }
        } catch (ParameterException ex) {
            Logger.getLogger(OpenCGAMain.class.getName()).log(Level.SEVERE, ex.getMessage(), ex);
            System.out.println(parser.usage());
            System.exit(1);
        }

        if (command instanceof CommandLoadVariants) {
            CommandLoadVariants c = (CommandLoadVariants) command;

            Path variantsPath = Paths.get(c.input + ".variants.json.gz");
            Path filePath = Paths.get(c.input + ".file.json.gz");

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, null, null);
            indexVariants("load", source, variantsPath, filePath, null, c.backend, Paths.get(c.credentials), c.includeEffect, c.includeStats, c.includeSamples, null);

        } else if (command instanceof CommandTransformVariants) {
            CommandTransformVariants c = (CommandTransformVariants) command;

            Path variantsPath = Paths.get(c.file);
            Path pedigreePath = c.pedigree != null ? Paths.get(c.pedigree) : null;
            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), c.fileId, c.studyId, c.study);
            indexVariants("transform", source, variantsPath, pedigreePath, outdir, "json", null, c.includeEffect, c.includeStats, c.includeSamples, c.aggregated);
        }
    }

    private static void indexVariants(String step, VariantSource source, Path mainFilePath, Path auxiliaryFilePath, Path outdir, String backend,
                                      Path credentialsPath, boolean includeEffect, boolean includeStats, boolean includeSamples, String aggregated)
            throws IOException, IllegalOpenCGACredentialsException {

        VariantReader reader;
        PedigreeReader pedReader = ("transform".equals(step) && auxiliaryFilePath != null) ?
                new PedigreePedReader(auxiliaryFilePath.toString()) : null;

        if (source.getFileName().endsWith(".vcf") || source.getFileName().endsWith(".vcf.gz")) {

            if (aggregated != null) {
                includeStats = false;
                switch (aggregated.toLowerCase()) {
                    case "basic":
                        reader = new VariantVcfReader(source, mainFilePath.toAbsolutePath().toString(), new VariantAggregatedVcfFactory());
                        break;
                    case "evs":
                        reader = new VariantVcfReader(source, mainFilePath.toAbsolutePath().toString(), new VariantVcfEVSFactory());
                        break;
                    default:
                        reader = new VariantVcfReader(source, mainFilePath.toAbsolutePath().toString());

                }
            } else {
                reader = new VariantVcfReader(source, mainFilePath.toAbsolutePath().toString());
            }
        } else if (source.getFileName().endsWith(".json") || source.getFileName().endsWith(".json.gz")) {
            assert (auxiliaryFilePath != null);
            reader = new VariantJsonReader(source, mainFilePath.toAbsolutePath().toString(), auxiliaryFilePath.toAbsolutePath().toString());
        } else {
            throw new IOException("Variants input file format not supported");
        }

        List<VariantWriter> writers = new ArrayList<>();

        List<Task<Variant>> taskList = new SortedList<>();

        // TODO Restore when SQLite and Monbase are once again ready!!
        if (backend.equalsIgnoreCase("mongo")) {
            Properties properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
            OpenCGACredentials credentials = new MongoCredentials(properties);
            writers.add(new VariantMongoWriter(source, (MongoCredentials) credentials,
                    properties.getProperty("collection_variants", "variants"),
                    properties.getProperty("collection_files", "files")));
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

    private static void indexAlignments(String study, Path filePath, String backend, Path credentialsPath, boolean includeCoverage) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
