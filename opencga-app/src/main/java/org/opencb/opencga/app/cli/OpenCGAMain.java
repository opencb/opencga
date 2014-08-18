package org.opencb.opencga.app.cli;


import com.beust.jcommander.ParameterException;
import com.google.common.io.Files;
import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.VcfRecord;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.formats.variant.vcf4.io.VcfRawReader;
import org.opencb.biodata.formats.variant.vcf4.io.VcfRawWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantAggregatedVcfFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantVcfEVSFactory;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.app.cli.OptionsParser.Command;
import org.opencb.opencga.app.cli.OptionsParser.CommandCreateAccessions;
import org.opencb.opencga.app.cli.OptionsParser.CommandLoadVariants;
import org.opencb.opencga.app.cli.OptionsParser.CommandTransformVariants;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.tools.accession.CreateAccessionTask;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.opencb.variant.lib.runners.VariantRunner;
import org.opencb.variant.lib.runners.tasks.VariantEffectTask;
import org.opencb.variant.lib.runners.tasks.VariantStatsTask;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
//import org.opencb.opencga.storage.variant.VariantVcfHbaseWriter;

/**
 * @author Cristina Yenyxe Gonzalez Garcia
 */
public class OpenCGAMain {

    private static final String APPLICATION_PROPERTIES_FILE = "application.properties";
    private static final String OPENCGA_HOME = System.getenv("OPENCGA_HOME");
    private static final String MONGODB_VARIANT_MANAGER = "org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageManager";

    private static VariantStorageManager variantStorageManager = null;

    public static void main(String[] args) throws IOException, InterruptedException, IllegalOpenCGACredentialsException {
        OptionsParser parser = new OptionsParser();
        // If no arguments are provided, or -h/--help is the first argument, the usage is shown
        if (args.length == 0 || args[0].equals("-h") || args[0].equals("--help")) {
            System.out.println(parser.usage());
            return;
        }

        Command command = null;
        try {
            switch (parser.parse(args)) {
                case "create-accessions":
                    command = parser.getAccessionsCommand();
                    break;
                case "load-variants":
                    command = parser.getLoadCommand();
                    break;
                case "transform-variants":
                    command = parser.getTransformCommand();
                    break;
                default:
                    System.out.println("Command not implemented");
                    System.exit(1);
            }
        } catch (ParameterException ex) {
            System.out.println(ex.getMessage());
            System.out.println(parser.usage());
            System.exit(1);
        }


        try {


            variantStorageManager = (VariantStorageManager) Class.forName(MONGODB_VARIANT_MANAGER).newInstance();
            Path propPath = Paths.get(OPENCGA_HOME, APPLICATION_PROPERTIES_FILE);
            if(propPath != null && propPath.toFile().exists()) {
                variantStorageManager.setProperties(propPath);
            }
            //variantStorageManager.setProperties(...); //Add default properties

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }


        if (command instanceof CommandCreateAccessions) {
            CommandCreateAccessions c = (CommandCreateAccessions) command;

            Path variantsPath = Paths.get(c.input);
            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, c.studyId, null);
            createAccessionIds(variantsPath, source, c.prefix, c.resumeFromAccession, outdir);

        } else if (command instanceof CommandTransformVariants) {
            CommandTransformVariants c = (CommandTransformVariants) command;

            Map<String, Object> params = new LinkedHashMap<>();
            params.put("fileId", c.fileId);
            params.put("studyId", c.studyId);
            params.put("study", c.study);
            params.put("includeEffect", c.includeEffect);
            params.put("includeStats", c.includeStats);
            params.put("includeSamples", c.includeSamples);
            params.put("aggregated", c.aggregated);

            Path variantsPath = Paths.get(c.file);
            Path pedigreePath = c.pedigree != null ? Paths.get(c.pedigree) : null;
            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;
            variantStorageManager.transform(variantsPath, pedigreePath, outdir, params);
//
//            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), c.fileId, c.studyId, c.study);
//            indexVariants("transform", source, variantsPath, pedigreePath, outdir, "json", null, c.includeEffect, c.includeStats, c.includeSamples, c.aggregated);

        } else if (command instanceof CommandLoadVariants) {
            CommandLoadVariants c = (CommandLoadVariants) command;
            Path variantsPath = Paths.get(c.input + ".variants.json.gz");
            Path credentials = Paths.get(c.credentials);
            //variantStorageManager.setProperties(Paths.get(c.credentials));

            Map<String, Object> params = new LinkedHashMap<>();
            params.put("includeEffect", c.includeEffect);
            params.put("includeStats", c.includeStats);
            params.put("includeSamples", c.includeSamples);

            variantStorageManager.load(variantsPath, credentials, params);

//            Path filePath = Paths.get(c.input + ".file.json.gz");
//
//            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, null, null);
//            indexVariants("load", source, variantsPath, filePath, null, c.backend, Paths.get(c.credentials), c.includeEffect, c.includeStats, c.includeSamples, null);
        }
    }

    private static void createAccessionIds(Path variantsPath, VariantSource source, String globalPrefix, String fromAccession, Path outdir) throws IOException {
        String studyId = source.getStudyId();
        String studyPrefix = studyId.substring(studyId.length() - 6);
        VcfRawReader reader = new VcfRawReader(variantsPath.toString());

        List<DataWriter> writers = new ArrayList<>();
        String variantsFilename = Files.getNameWithoutExtension(variantsPath.getFileName().toString());
        writers.add(new VcfRawWriter(reader, outdir.toString() + "/" + variantsFilename + "_accessioned" + ".vcf"));

        List<Task<VcfRecord>> taskList = new ArrayList<>();
        taskList.add(new CreateAccessionTask(source, globalPrefix, studyPrefix, fromAccession));

        Runner vr = new Runner(reader, writers, taskList);

        System.out.println("Accessioning variants with prefix " + studyPrefix + "...");
        vr.run();
        System.out.println("Variants accessioned!");
    }

    //indexVariants("load", source, variantsPath, filePath, null, c.backend, Paths.get(c.credentials), c.includeEffect, c.includeStats, c.includeSamples, null);
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
//            OpenCGACredentials credentials = new MongoCredentials(properties);
//            writers.add(new VariantMongoWriter(source, (MongoCredentials) credentials,
//                    properties.getProperty("collection_variants", "variants"),
//                    properties.getProperty("collection_files", "files")));
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
