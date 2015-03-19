package org.opencb.opencga.storage.app.cli;

import com.beust.jcommander.ParameterException;
import com.google.common.io.Files;
import org.opencb.biodata.formats.feature.gff.Gff;
import org.opencb.biodata.formats.feature.gff.io.GffReader;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.vcf4.VcfRecord;
import org.opencb.biodata.formats.variant.vcf4.io.VcfRawReader;
import org.opencb.biodata.formats.variant.vcf4.io.VcfRawWriter;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.common.TimeUtils;
import org.opencb.opencga.lib.tools.accession.CreateAccessionTask;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.sequence.SqliteSequenceDBAdaptor;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.*;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Cristina Yenyxe Gonzalez Garcia
 */
@Deprecated
public class OpenCGAStorageMain {

    //    private static final String OPENCGA_HOME = System.getenv("OPENCGA_HOME");
    private static String opencgaHome;
    public static final String OPENCGA_STORAGE_ANNOTATOR = "OPENCGA.STORAGE.ANNOTATOR";

    protected static Logger logger = null;// LoggerFactory.getLogger(OpenCGAStorageMain.class);
    private static OptionsParser parser;


    static {

    }

    public static void main(String[] args)
            throws IOException, InterruptedException, IllegalOpenCGACredentialsException, FileFormatException,
            IllegalAccessException, InstantiationException, ClassNotFoundException, URISyntaxException, VariantAnnotatorException {

        parser = new OptionsParser();
        OptionsParser.Command command = null;
        try {
            String parsedCommand = parser.parse(args);
            String logLevel = "info";
            if (parser.getGeneralParameters().verbose) {
                logLevel = "debug";
            }
            if (parser.getGeneralParameters().logLevel != null) {
                logLevel = parser.getGeneralParameters().logLevel;
            }
            setLogLevel(logLevel);
            Config.setOpenCGAHome();

            if(parser.getGeneralParameters().help || args.length == 0) {
                System.out.println(parser.usage());
                return;
            }
            if (parser.getGeneralParameters().version) {
                String version = Config.getStorageProperties().getProperty("OPENCGA.STORAGE.VERSION");
                printVersion(version);
                return;
            }
            command = parser.getCommand();
            if (command == null) {
                System.out.println("Command not implemented");
                System.exit(1);
            }
        } catch (ParameterException ex) {
            System.out.println(ex.getMessage());
            System.out.println(parser.usage());
            System.exit(1);
        }


        if (command instanceof OptionsParser.CommandIndexAlignments) {    //TODO: Create method AlignmentStorageManager.index() ??
            OptionsParser.CommandIndexAlignments c = (OptionsParser.CommandIndexAlignments) command;
            indexAlignments(c);

        } else if (command instanceof OptionsParser.CommandIndexSequence) {
            OptionsParser.CommandIndexSequence c = (OptionsParser.CommandIndexSequence) command;
            indexSequence(c);

        } else if (command instanceof OptionsParser.CommandIndexVariants) {
            OptionsParser.CommandIndexVariants c = (OptionsParser.CommandIndexVariants) command;
            indexVariants(c);

        } else if (command instanceof OptionsParser.CommandCreateAccessions) {
            OptionsParser.CommandCreateAccessions c = (OptionsParser.CommandCreateAccessions) command;

            Path variantsPath = Paths.get(c.input);
            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, c.studyId, null);
            createAccessionIds(variantsPath, source, c.prefix, c.resumeFromAccession, outdir);
 /*
        }else if (command instanceof OptionsParser.CommandTransformVariants) { //TODO: Add "preTransform and postTransform" call
            OptionsParser.CommandTransformVariants c = (OptionsParser.CommandTransformVariants) command;
            VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager();
            URI variantsUri = new URI(null, c.file, null);
            URI pedigreeUri = c.pedigree != null ? new URI(null, c.pedigree, null) : null;
            URI outdirUri = c.outdir != null ? new URI(null, c.outdir + "/", null).resolve(".") : variantsUri.resolve(".");
            Path variantsPath = Paths.get(c.file);
//            Path pedigreePath = c.pedigree != null ? Paths.get(c.pedigree) : null;
//            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;
            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), c.fileId, c.studyId, c.study, c.studyType, c.aggregated);

            ObjectMap params = new ObjectMap();
            params.put(VariantStorageManager.INCLUDE_EFFECT,  c.includeEffect);
            params.put(VariantStorageManager.INCLUDE_STATS,   c.includeStats);
            params.put(VariantStorageManager.INCLUDE_SAMPLES, c.includeSamples);
            params.put(VariantStorageManager.VARIANT_SOURCE, source);

            variantStorageManager.transform(variantsUri, pedigreeUri, outdirUri, params);

//            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), c.fileId, c.studyId, c.study);
//            indexVariants("transform", source, variantsPath, pedigreePath, outdir, "json", null, c.includeEffect, c.includeStats, c.includeSamples, c.aggregated);

        } else if (command instanceof OptionsParser.CommandLoadVariants) {    //TODO: Add "preLoad" call
            OptionsParser.CommandLoadVariants c = (OptionsParser.CommandLoadVariants) command;
            VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager(c.backend);
            if(c.credentials != null && !c.credentials.isEmpty()) {
                variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
            }

            //Path variantsPath = Paths.get(c.input + ".variants.json.gz");
            Path variantsPath = Paths.get(c.input);
            URI variantsUri = new URI(null, c.input, null);
            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, null, null);

            ObjectMap params = new ObjectMap();
            params.put(VariantStorageManager.INCLUDE_EFFECT,  c.includeEffect);
            params.put(VariantStorageManager.INCLUDE_STATS, c.includeStats);
            params.put(VariantStorageManager.INCLUDE_SAMPLES, c.includeSamples);
            params.put(VariantStorageManager.VARIANT_SOURCE, source);
            params.put(VariantStorageManager.DB_NAME, c.dbName);

            // TODO Right now it doesn't matter if the file is aggregated or not to save it to the database
            variantStorageManager.load(variantsUri, params);

     /*       Path variantsPath = Paths.get(c.input + ".variants.json.gz");
            Path filePath = Paths.get(c.input + ".file.json.gz");

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, null, null);
            indexVariants("load", source, variantsPath, filePath, null, c.backend, Paths.get(c.credentials), c.includeEffect, c.includeStats, c.includeSamples, null);
*/

//            indexVariants("load", source, variantsPath, filePath, null, c.backend, Paths.get(c.credentials), c.includeEffect, c.includeStats, c.includeSamples);
/*
        } else if (command instanceof OptionsParser.CommandTransformAlignments) { //TODO: Add "preTransform and postTransform" call
            OptionsParser.CommandTransformAlignments c = (OptionsParser.CommandTransformAlignments) command;
            AlignmentStorageManager alignmentStorageManager = StorageManagerFactory.getAlignmentStorageManager();

            URI inputUri = new URI(null, c.file, null);
            URI outdirUri = c.outdir != null ? new URI(null, c.outdir + "/", null).resolve(".") : inputUri.resolve(".");


            ObjectMap params = new ObjectMap();

            if(c.fileId != null && !c.fileId.isEmpty()) {
                params.put(AlignmentStorageManager.FILE_ID, c.fileId);
            }
            //params.put(AlignmentStorageManager.STUDY,   c.study);
            params.put(AlignmentStorageManager.PLAIN,   c.plain);
            params.put(AlignmentStorageManager.MEAN_COVERAGE_SIZE_LIST, c.meanCoverage);
            params.put(AlignmentStorageManager.INCLUDE_COVERAGE, c.includeCoverage);


            alignmentStorageManager.transform(inputUri, null, outdirUri, params);



//            Path filePath = Paths.get(c.file);
//            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;
//            String backend = c.plain ? "json" : "json.gz";
//
//            if (!filePath.toFile().exists()) {
//                throw new IOException("[Error] File not found : " + c.file);
//            }
//
//            indexAlignments(
//                    c.study, //c.studyId,
//                    filePath,
//                    c.fileId, outdir, null,
//                    backend, null, null,     //Credentials not needed
//                    true,
//                    false,              //Can't be loaded
//                    c.includeCoverage, c.meanCoverage);

        } else if (command instanceof OptionsParser.CommandLoadAlignments){ //TODO: Add "preLoad" call
            OptionsParser.CommandLoadAlignments c = (OptionsParser.CommandLoadAlignments) command;
            AlignmentStorageManager alignmentStorageManager = StorageManagerFactory.getAlignmentStorageManager(c.backend);
            if(c.credentials != null && !c.credentials.isEmpty()) {
                alignmentStorageManager.addConfigUri(new URI(null, c.credentials, null));
            }

            ObjectMap params = new ObjectMap();

//            params.put(AlignmentStorageManager.INCLUDE_COVERAGE, true); //, c.includeCoverage);
            params.put(AlignmentStorageManager.FILE_ID, c.fileId);
            params.put(AlignmentStorageManager.DB_NAME, c.dbName);

            URI inputUri = new URI(null, c.input, null);

            alignmentStorageManager.load(inputUri, params);

*/
        } else if(command instanceof OptionsParser.CommandFetchVariants){
            OptionsParser.CommandFetchVariants c = (OptionsParser.CommandFetchVariants) command;

            /**
             * Open connection
             */
            VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager(c.backend);
            if(c.credentials != null && !c.credentials.isEmpty()) {
                variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
            }

            ObjectMap params = new ObjectMap();
            VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(c.dbName, params);

            /**
             * Parse Regions
             */
            List<Region> regions = null;
            GffReader gffReader = null;
            if(c.regions != null && !c.regions.isEmpty()) {
                regions = new LinkedList<>();
                for (String csvRegion : c.regions) {
                    for (String strRegion : csvRegion.split(",")) {
                        Region region = new Region(strRegion);
                        regions.add(region);
                        logger.info("Parsed region: {}", region);
                    }
                }
            } else if (c.gffFile != null && !c.gffFile.isEmpty()) {
                try {
                    gffReader = new GffReader(c.gffFile);
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }
//                throw new UnsupportedOperationException("Unsuppoted GFF file");
            }

            /**
             * Parse QueryOptions
             */
            QueryOptions options = new QueryOptions();

            if(c.studyAlias != null && !c.studyAlias.isEmpty()) {
                options.add("studies", Arrays.asList(c.studyAlias.split(",")));
            }
            if(c.fileId != null && !c.fileId.isEmpty()) {
                options.add("files", Arrays.asList(c.fileId.split(",")));
            }
            if(c.effect != null && !c.effect.isEmpty()) {
                options.add("effect", Arrays.asList(c.effect.split(",")));
            }

            if(c.stats != null && !c.stats.isEmpty()) {
                for (String csvStat : c.stats) {
                    for (String stat : csvStat.split(",")) {
                        int index = stat.indexOf("<");
                        index = index >= 0 ? index : stat.indexOf("!");
                        index = index >= 0 ? index : stat.indexOf("~");
                        index = index >= 0 ? index : stat.indexOf("<");
                        index = index >= 0 ? index : stat.indexOf(">");
                        index = index >= 0 ? index : stat.indexOf("=");
                        if(index < 0) {
                            throw new UnsupportedOperationException("Unknown stat filter operation: " + stat);
                        }
                        String name = stat.substring(0, index);
                        String cond = stat.substring(index);

//                        if("maf".equals(name) || "mgf".equals(name) || "missingAlleles".equals(name) || "missingGenotypes".equals(name)) {
                        if(name.matches("maf|mgf|missingAlleles|missingGenotypes")) {
                            options.put(name, cond);
                        } else {
                            throw new UnsupportedOperationException("Unknown stat filter name: " + name);
                        }
                        logger.info("Parsed stat filter: {} {}", name, cond);
                    }
                }
            }
            if(c.id != null && !c.id.isEmpty()) {   //csv
                options.add("id", c.id);
            }
            if(c.gene != null && !c.gene.isEmpty()) {   //csv
                options.add("gene", c.gene);
            }
            if(c.type != null && !c.type.isEmpty()) {   //csv
                options.add("type", c.type);
            }
            if(c.reference != null && !c.reference.isEmpty()) {   //csv
                options.add("reference", c.reference);
            }


            /**
             * Run query
             */
            int subListSize = 20;
            logger.info("options = " + options.toJson());
            if (regions != null && !regions.isEmpty()) {
                for(int i = 0; i < (regions.size()+subListSize-1)/subListSize; i++) {
                    List<Region> subRegions = regions.subList(
                            i * subListSize,
                            Math.min((i + 1) * subListSize, regions.size()));

                    logger.info("subRegions = " + subRegions);
                    List<QueryResult> queryResults = dbAdaptor.getAllVariantsByRegionList(subRegions, options);
                    logger.info("{}", queryResults);
                }
            } else if(gffReader != null) {
                List<Gff> gffList;
                List<Region> subRegions;
                while((gffList = gffReader.read(subListSize)) != null) {
                    subRegions = new ArrayList<>(subListSize);
                    for (Gff gff : gffList) {
                        subRegions.add(new Region(gff.getSequenceName(), gff.getStart(), gff.getEnd()));
                    }

                    logger.info("subRegions = " + subRegions);
                    List<QueryResult> queryResults = dbAdaptor.getAllVariantsByRegionList(subRegions, options);
                    logger.info("{}", queryResults);
                }
            } else {
                System.out.println(dbAdaptor.getAllVariants(options));
            }


        } else if(command instanceof OptionsParser.CommandFetchAlignments){
            OptionsParser.CommandFetchAlignments c = (OptionsParser.CommandFetchAlignments) command;

            /**
             * Open connection
             */
            AlignmentStorageManager alignmentStorageManager = StorageManagerFactory.getAlignmentStorageManager(c.backend);
            if(c.credentials != null && !c.credentials.isEmpty()) {
                alignmentStorageManager.addConfigUri(new URI(null, c.credentials, null));
            }

            ObjectMap params = new ObjectMap();
            AlignmentDBAdaptor dbAdaptor = alignmentStorageManager.getDBAdaptor(c.dbName, params);

            /**
             * Parse Regions
             */
            GffReader gffReader = null;
            List<Region> regions = null;
            if(c.regions != null && !c.regions.isEmpty()) {
                regions = new LinkedList<>();
                for (String csvRegion : c.regions) {
                    for (String strRegion : csvRegion.split(",")) {
                        Region region = new Region(strRegion);
                        regions.add(region);
                        logger.info("Parsed region: {}", region);
                    }
                }
            } else if (c.gffFile != null && !c.gffFile.isEmpty()) {
                try {
                    gffReader = new GffReader(c.gffFile);
                } catch (NoSuchMethodException e) {
                    e.printStackTrace();
                }
                //throw new UnsupportedOperationException("Unsuppoted GFF file");
            }

            /**
             * Parse QueryOptions
             */
            QueryOptions options = new QueryOptions();

            if(c.fileId != null && !c.fileId.isEmpty()) {
                options.add(AlignmentDBAdaptor.QO_FILE_ID, c.fileId);
            }
            options.add(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, c.coverage);
            options.add(AlignmentDBAdaptor.QO_VIEW_AS_PAIRS, c.asPairs);
            options.add(AlignmentDBAdaptor.QO_PROCESS_DIFFERENCES, c.processDifferences);
            if(c.histogram > 0) {
                options.add(AlignmentDBAdaptor.QO_INCLUDE_COVERAGE, true);
                options.add(AlignmentDBAdaptor.QO_HISTOGRAM, true);
                options.add(AlignmentDBAdaptor.QO_INTERVAL_SIZE, c.histogram);
            }
            if(c.filePath != null && !c.filePath.isEmpty()) {
                options.add(AlignmentDBAdaptor.QO_BAM_PATH, c.filePath);
            }


            if(c.stats != null && !c.stats.isEmpty()) {
                for (String csvStat : c.stats) {
                    for (String stat : csvStat.split(",")) {
                        int index = stat.indexOf("<");
                        index = index >= 0 ? index : stat.indexOf("!");
                        index = index >= 0 ? index : stat.indexOf("~");
                        index = index >= 0 ? index : stat.indexOf("<");
                        index = index >= 0 ? index : stat.indexOf(">");
                        index = index >= 0 ? index : stat.indexOf("=");
                        if(index < 0) {
                            throw new UnsupportedOperationException("Unknown stat filter operation: " + stat);
                        }
                        String name = stat.substring(0, index);
                        String cond = stat.substring(index);

                        if(name.matches("")) {
                            options.put(name, cond);
                        } else {
                            throw new UnsupportedOperationException("Unknown stat filter name: " + name);
                        }
                        logger.info("Parsed stat filter: {} {}", name, cond);
                    }
                }
            }


            /**
             * Run query
             */
            int subListSize = 20;
            logger.info("options = {}", options.toJson());
            if(c.histogram > 0) {
                for (Region region : regions) {
                    System.out.println(dbAdaptor.getAllIntervalFrequencies(region, options));
                }
            } else if (regions != null && !regions.isEmpty()) {
                for(int i = 0; i < (regions.size()+subListSize-1)/subListSize; i++) {
                    List<Region> subRegions = regions.subList(
                            i * subListSize,
                            Math.min((i + 1) * subListSize, regions.size()));

                    logger.info("subRegions = " + subRegions);
                    QueryResult queryResult = dbAdaptor.getAllAlignmentsByRegion(subRegions, options);
                    logger.info("{}", queryResult);
                    System.out.println(new ObjectMap("queryResult", queryResult).toJson());
                }
            } else if (gffReader != null) {
                List<Gff> gffList;
                List<Region> subRegions;
                while((gffList = gffReader.read(subListSize)) != null) {
                    subRegions = new ArrayList<>(subListSize);
                    for (Gff gff : gffList) {
                        subRegions.add(new Region(gff.getSequenceName(), gff.getStart(), gff.getEnd()));
                    }

                    logger.info("subRegions = " + subRegions);
                    QueryResult queryResult = dbAdaptor.getAllAlignmentsByRegion(subRegions, options);
                    logger.info("{}", queryResult);
                    System.out.println(new ObjectMap("queryResult", queryResult).toJson());
                }
            } else {
                throw new UnsupportedOperationException("Unable to fetch over all the genome");
//                System.out.println(dbAdaptor.getAllAlignments(options));
            }
        } else if(command instanceof OptionsParser.CommandAnnotateVariants) {
            OptionsParser.CommandAnnotateVariants c = (OptionsParser.CommandAnnotateVariants) command;
            annotateVariants(c);
        } else if (command instanceof OptionsParser.CommandStatsVariants) {
            OptionsParser.CommandStatsVariants c = (OptionsParser.CommandStatsVariants) command;
            statsVariants(c);
        }
    }

    private static void indexSequence(OptionsParser.CommandIndexSequence c) throws URISyntaxException, IOException, FileFormatException {
        if (c.input.endsWith(".fasta") || c.input.endsWith(".fasta.gz")) {
            Path input = Paths.get(new URI(c.input).getPath());
            Path outdir = c.outdir.isEmpty() ? input.getParent() : Paths.get(new URI(c.outdir).getPath());

            logger.info("Indexing Fasta : " + input.toString());
            long start = System.currentTimeMillis();
            SqliteSequenceDBAdaptor sqliteSequenceDBAdaptor = new SqliteSequenceDBAdaptor();
            File index = null;
            try {
                index = sqliteSequenceDBAdaptor.index(input.toFile(), outdir);
            } catch (SQLException e) {
                e.printStackTrace();
                System.exit(1);
            }
            long end = System.currentTimeMillis();
            logger.info(
                    "Fasta file '" + input + "' indexed. " +
                            "Result: '" + index + "' . " +
                            "Time = " + (end - start) + "ms");
        } else {
            throw new IOException("Unknown file type");
        }
    }

    private static void indexAlignments(OptionsParser.CommandIndexAlignments c) throws ClassNotFoundException, IllegalAccessException, InstantiationException, URISyntaxException, IOException, FileFormatException {
        AlignmentStorageManager alignmentStorageManager;
        String storageEngine = parser.getGeneralParameters().storageEngine;
        if (storageEngine == null || storageEngine.isEmpty()) {
            alignmentStorageManager = StorageManagerFactory.getAlignmentStorageManager();
        } else {
            alignmentStorageManager = StorageManagerFactory.getAlignmentStorageManager(storageEngine);
        }
        URI input = new URI(null, c.input, null);
        if(c.credentials != null && !c.credentials.isEmpty()) {
            alignmentStorageManager.addConfigUri(new URI(null, c.credentials, null));
        }

        URI outdir;
        if (c.outdir != null && !c.outdir.isEmpty()) {
            outdir = new URI(null, c.outdir + (c.outdir.endsWith("/") ? "" : "/"), null).resolve(".");
        } else {
            outdir = input.resolve(".");
        }

        assertDirectoryExists(outdir);

        ObjectMap params = new ObjectMap();
        params.putAll(parser.getGeneralParameters().params);

        if (c.fileId != 0) {
            params.put(AlignmentStorageManager.FILE_ID, c.fileId);
        }
        params.put(AlignmentStorageManager.PLAIN, false);
        params.put(AlignmentStorageManager.MEAN_COVERAGE_SIZE_LIST, c.meanCoverage);
        params.put(AlignmentStorageManager.INCLUDE_COVERAGE, c.calculateCoverage);
        params.put(AlignmentStorageManager.DB_NAME, c.dbName);
        params.put(AlignmentStorageManager.COPY_FILE, false);
        params.put(AlignmentStorageManager.ENCRYPT, "null");

        params.putAll(c.params);

        boolean extract, transform, load;
        URI nextFileUri = input;

        if (!c.load && !c.transform) {  // if not present --transform nor --load, do both
            extract = true;
            transform = true;
            load = true;
        } else {
            extract = c.transform;
            transform = c.transform;
            load = c.load;
        }

        if (extract) {
            logger.info("-- Extract alignments -- {}", input);
            nextFileUri = alignmentStorageManager.extract(input, outdir, params);
        }

        if (transform) {
            logger.info("-- PreTransform alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.preTransform(nextFileUri, params);
            logger.info("-- Transform alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.transform(nextFileUri, null, outdir, params);
            logger.info("-- PostTransform alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.postTransform(nextFileUri, params);
        }

        if (load) {
            logger.info("-- PreLoad alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.preLoad(nextFileUri, outdir, params);
            logger.info("-- Load alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.load(nextFileUri, params);
            logger.info("-- PostLoad alignments -- {}", nextFileUri);
            nextFileUri = alignmentStorageManager.postLoad(nextFileUri, outdir, params);
        }
    }

    private static void indexVariants(OptionsParser.CommandIndexVariants c)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, URISyntaxException, IOException, FileFormatException {
        VariantStorageManager variantStorageManager;
        String storageEngine = parser.getGeneralParameters().storageEngine;
        variantStorageManager = StorageManagerFactory.getVariantStorageManager(storageEngine);
        if(c.credentials != null && !c.credentials.isEmpty()) {
            variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
        }

        URI variantsUri = new URI(null, c.input, null);
        URI pedigreeUri = c.pedigree != null && !c.pedigree.isEmpty() ? new URI(null, c.pedigree, null) : null;
        URI outdirUri;
        if (c.outdir != null && !c.outdir.isEmpty()) {
            outdirUri = new URI(null, c.outdir + (c.outdir.endsWith("/") ? "" : "/"), null).resolve(".");
        } else {
            outdirUri = variantsUri.resolve(".");
        }
        assertDirectoryExists(outdirUri);

//        VariantSource source = new VariantSource(fileName, c.fileId, c.studyId, c.study, c.studyType, c.aggregated);
        String fileName = variantsUri.resolve(".").relativize(variantsUri).toString();
        StudyConfiguration studyConfiguration;
        Path studyConfigurationPath;
        if (c.studyConfigurationFile == null || c.studyConfigurationFile.isEmpty()) {
            //Create a new StudyConfiguration File
            checkNull(c.studyId, "--study-id");
            checkNull(c.studyName, "--study-name");
            studyConfiguration = new StudyConfiguration(c.studyId, c.studyName);
            studyConfigurationPath = Paths.get(outdirUri.getPath(), studyConfiguration.getStudyName() + ".study.json");
            logger.info("Creating a new StudyConfiguration file: " + studyConfigurationPath);
            studyConfiguration.write(studyConfigurationPath);
        } else {
            studyConfigurationPath = Paths.get(c.studyConfigurationFile);
            studyConfiguration = StudyConfiguration.read(studyConfigurationPath);
        }

        ObjectMap params = new ObjectMap();
//        params.put(VariantStorageManager.INCLUDE_EFFECT,  c.includeEffect);
//        params.put(VariantStorageManager.FILE_ID, studyConfiguration.getFileIds().get(fileName));
        params.put(VariantStorageManager.FILE_ID, c.fileId);
        params.put(VariantStorageManager.CALCULATE_STATS, c.calculateStats);
        params.put(VariantStorageManager.INCLUDE_STATS, c.includeStats);
        params.put(VariantStorageManager.INCLUDE_SAMPLES, c.includeGenotype);   // TODO rename samples to genotypes
        params.put(VariantStorageManager.INCLUDE_SRC, c.includeSrc);
        params.put(VariantStorageManager.COMPRESS_GENOTYPES, c.compressGenotypes);
//        params.put(VariantStorageManager.VARIANT_SOURCE, source);
        params.put(VariantStorageManager.STUDY_CONFIGURATION, studyConfiguration);
        params.put(VariantStorageManager.AGGREGATED_TYPE, c.aggregated);
        params.put(VariantStorageManager.DB_NAME, c.dbName);
        params.put(VariantStorageManager.ANNOTATE, c.annotate);
        params.put(VariantStorageManager.OVERWRITE_ANNOTATIONS, c.overwriteAnnotations);

        if(c.annotate) {
            //Get annotator config
            Properties annotatorProperties = Config.getStorageProperties();
            if(c.annotatorConfig != null && !c.annotatorConfig.isEmpty()) {
                annotatorProperties.load(new FileInputStream(c.annotatorConfig));
            }
            params.put(VariantStorageManager.ANNOTATOR_PROPERTIES, annotatorProperties);

            //Get annotation source
            VariantAnnotationManager.AnnotationSource annotatorSource = c.annotator;
            if(annotatorSource == null) {
                annotatorSource = VariantAnnotationManager.AnnotationSource.valueOf(
                        annotatorProperties.getProperty(
                                OPENCGA_STORAGE_ANNOTATOR,
                                VariantAnnotationManager.AnnotationSource.CELLBASE_REST.name()
                        ).toUpperCase()
                );
            }
            params.put(VariantStorageManager.ANNOTATION_SOURCE, annotatorSource);
        }

        params.putAll(c.params);

        URI nextFileUri = variantsUri;


        boolean extract, transform, load;

        if (!c.load && !c.transform) {
            extract = true;
            transform = true;
            load = true;
        } else {
            extract = c.transform;
            transform = c.transform;
            load = c.load;
        }

        if (extract) {
            logger.info("-- Extract variants -- {}", variantsUri);
            nextFileUri = variantStorageManager.extract(variantsUri, outdirUri, params);
        }

        if (transform) {
            logger.info("-- PreTransform variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.preTransform(nextFileUri, params);
            logger.info("-- Transform variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.transform(nextFileUri, pedigreeUri, outdirUri, params);
            logger.info("-- PostTransform variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.postTransform(nextFileUri, params);
        }

        studyConfiguration.write(studyConfigurationPath);

        if (load) {
            logger.info("-- PreLoad variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.preLoad(nextFileUri, outdirUri, params);
            logger.info("-- Load variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.load(nextFileUri, params);
            logger.info("-- PostLoad variants -- {}", nextFileUri);
            nextFileUri = variantStorageManager.postLoad(nextFileUri, outdirUri, params);
        }

        studyConfiguration.write(studyConfigurationPath);

    }

    private static void createAccessionIds(Path variantsPath, VariantSource source, String globalPrefix, String fromAccession, Path outdir) throws IOException {
        String studyId = source.getStudyId();
        String studyPrefix = studyId.substring(studyId.length() - 6);
        VcfRawReader reader = new VcfRawReader(variantsPath.toString());

        List<DataWriter> writers = new ArrayList<>();
        String variantsFilename = Files.getNameWithoutExtension(variantsPath.getFileName().toString());
        if (variantsPath.toString().endsWith(".gz")) {
            variantsFilename = Files.getNameWithoutExtension(variantsFilename);
        }
        writers.add(new VcfRawWriter(reader, outdir.toString() + "/" + variantsFilename + "_accessioned" + ".vcf"));

        List<Task<VcfRecord>> taskList = new ArrayList<>();
        taskList.add(new CreateAccessionTask(source, globalPrefix, studyPrefix, fromAccession));

        Runner vr = new Runner(reader, writers, taskList);

        System.out.println("Accessioning variants with prefix " + studyPrefix + "...");
        vr.run();
        System.out.println("Variants accessioned!");
    }

    private static void annotateVariants(OptionsParser.CommandAnnotateVariants c)
            throws ClassNotFoundException, IllegalAccessException, InstantiationException, URISyntaxException, IOException, VariantAnnotatorException {
        /**
         * Create DBAdaptor
         */
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager(parser.getGeneralParameters().storageEngine);
        if(c.credentials != null && !c.credentials.isEmpty()) {
            variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
        }
        ObjectMap params = new ObjectMap();
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(c.dbName, params);

        /**
         * Create Annotator
         */
        Properties annotatorProperties = Config.getStorageProperties();
        if(c.annotatorConfig != null && !c.annotatorConfig.isEmpty()) {
            annotatorProperties.load(new FileInputStream(c.annotatorConfig));
        }


        VariantAnnotationManager.AnnotationSource annotatorSource = c.annotator;
        if(annotatorSource == null) {
            annotatorSource = VariantAnnotationManager.AnnotationSource.valueOf(
                    annotatorProperties.getProperty(
                            OPENCGA_STORAGE_ANNOTATOR,
                            VariantAnnotationManager.AnnotationSource.CELLBASE_REST.name()
                    ).toUpperCase()
            );
        }
        logger.info("Annotating with {}", annotatorSource);
        VariantAnnotator annotator = VariantAnnotationManager.buildVariantAnnotator(annotatorSource, annotatorProperties, c.species, c.assembly);
        VariantAnnotationManager variantAnnotationManager =
                new VariantAnnotationManager(annotator, dbAdaptor);

        /**
         * Annotation options
         */
        QueryOptions queryOptions = new QueryOptions();
        if (c.filterRegion != null) {
            queryOptions.add(VariantDBAdaptor.REGION, c.filterRegion);
        }
        if (c.filterChromosome != null) {
            queryOptions.add(VariantDBAdaptor.CHROMOSOME, c.filterChromosome);
        }
        if (c.filterGene != null) {
            queryOptions.add(VariantDBAdaptor.GENE, c.filterGene);
        }
        if (c.filterAnnotConsequenceType != null) {
            queryOptions.add(VariantDBAdaptor.ANNOT_CONSEQUENCE_TYPE, c.filterAnnotConsequenceType);
        }
        if (!c.overwriteAnnotations) {
            queryOptions.add(VariantDBAdaptor.ANNOTATION_EXISTS, false);
        }
        Path outDir = Paths.get(c.outdir);

        /**
         * Create and load annotations
         */
        boolean doCreate = c.create, doLoad = c.load != null;
        if (!c.create && c.load == null) {
            doCreate = true;
            doLoad = true;
        }

        URI annotationFile = null;
        if (doCreate) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation creation ");
            annotationFile = variantAnnotationManager.createAnnotation(outDir, c.fileName.isEmpty() ? c.dbName : c.fileName, queryOptions);
            logger.info("Finished annotation creation {}ms", System.currentTimeMillis() - start);
        }

        if (doLoad) {
            long start = System.currentTimeMillis();
            logger.info("Starting annotation load");
            if (annotationFile == null) {
//                annotationFile = new URI(null, c.load, null);
                annotationFile = Paths.get(c.load).toUri();
            }
            variantAnnotationManager.loadAnnotation(annotationFile, new QueryOptions());
            logger.info("Finished annotation load {}ms", System.currentTimeMillis() - start);
        }

    }

    private static void statsVariants(OptionsParser.CommandStatsVariants c)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException, URISyntaxException, IOException {

        /**
         * query options
         */
        QueryOptions queryOptions = new QueryOptions();
//        VariantSource variantSource = new VariantSource(null, c.fileId, c.studyId, null);
//        queryOptions.put(VariantStorageManager.VARIANT_SOURCE, variantSource);
        queryOptions.put(VariantStorageManager.DB_NAME, c.dbName);
        queryOptions.put(VariantStorageManager.OVERWRITE_STATS, c.overwriteStats);
        queryOptions.put(VariantStorageManager.FILE_ID, c.fileId);

        Map<String, Set<String>> samples = null;
        if (c.cohort != null && !c.cohort.isEmpty()) {
            samples = new LinkedHashMap<>(c.cohort.size());
            for (Map.Entry<String, String> entry : c.cohort.entrySet()) {
                samples.put(entry.getKey(), new HashSet<>(Arrays.asList(entry.getValue().split(","))));
            }
        }

        //TODO: Read cohorts from StudyConfiguration file
//        Path studyConfigurationPath = Paths.get(c.studyConfigurationFile);
//        StudyConfiguration studyConfiguration = StudyConfiguration.read(studyConfigurationPath);
//        samples.put()
//        >>>>>>> CONTINUE HERE! ****

        /**
         * Create DBAdaptor
         */
        VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager(parser.getGeneralParameters().storageEngine);
        if(c.credentials != null && !c.credentials.isEmpty()) {
            variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
        }
        VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(c.dbName, queryOptions);

        /**
         * Create and load stats
         */
        URI outputUri = new URI(c.fileName);
        URI directoryUri = outputUri.resolve(".");
        String filename = outputUri.equals(directoryUri) ? VariantStorageManager.buildFilename(c.studyId, c.fileId)
                : Paths.get(outputUri.getPath()).getFileName().toString();
        assertDirectoryExists(directoryUri);
        VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();

        boolean doCreate = c.create, doLoad = c.load != null;
        if (!c.create && c.load == null) {
            doCreate = doLoad = true;
        } else if (c.load != null) {
            filename = c.load;
        }

        try {
            if (doCreate) {
                filename += "." + TimeUtils.getTime();
                outputUri = outputUri.resolve(filename);
                outputUri = variantStatisticsManager.createStats(dbAdaptor, outputUri, samples, queryOptions);
            }

            if (doLoad) {
                outputUri = outputUri.resolve(filename);
                variantStatisticsManager.loadStats(dbAdaptor, outputUri, queryOptions);
            }
        } catch (IOException | IllegalArgumentException e) {   // file not found? wrong file id or study id?
            logger.error(e.getMessage());
            System.exit(1);
        }
    }


    private static String getDefault(String value, String defaultValue) {
        return value == null || value.isEmpty() ? defaultValue : value;
    }

    private static void checkNull(Integer value, String name) {
        if(value == null || value.equals(0)) {
            missingValue(name);
        }
    }

    private static void checkNull(String value, String name) {
        if(value == null || value.isEmpty()) {
            missingValue(name);
        }
    }

    private static void missingValue(String name) {
        logger.info("The following options are required: " + name + "");
        logger.info(parser.usage());
//            throw new IllegalArgumentException("The following options are required: " + name + "");
        System.exit(1);
    }

    private static void printVersion(String version) {
        System.out.println("OpenCGA Storage CLI. Version " + version);
    }

    private static void setLogLevel(String logLevel) {
// This small hack allow to configure the appropriate Logger level from the command line, this is done
// by setting the DEFAULT_LOG_LEVEL_KEY before the logger object is created.
        System.setProperty(org.slf4j.impl.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, logLevel);
        logger = LoggerFactory.getLogger(OpenCGAStorageMain.class);
    }

    private static void assertDirectoryExists(URI outdir){
        if (!java.nio.file.Files.exists(Paths.get(outdir.getPath()))) {
            logger.error("given output directory {} does not exist, please create it first.", outdir);
            System.exit(1);
        }
    }
}
