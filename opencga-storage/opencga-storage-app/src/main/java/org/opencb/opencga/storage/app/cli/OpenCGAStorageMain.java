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
import org.opencb.cellbase.core.client.CellBaseClient;
import org.opencb.cellbase.core.common.core.CellbaseConfiguration;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.tools.accession.CreateAccessionTask;
import org.opencb.opencga.storage.core.StorageManagerFactory;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.alignment.adaptors.AlignmentDBAdaptor;
import org.opencb.opencga.storage.core.sequence.SqliteSequenceDBAdaptor;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.*;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
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
public class OpenCGAStorageMain {

    //    private static final String OPENCGA_HOME = System.getenv("OPENCGA_HOME");
    private static final String opencgaHome;
    public static final String OPENCGA_STORAGE_ANNOTATOR = "OPENCGA.STORAGE.ANNOTATOR";
    public static final String CELLBASE_VERSION = "CELLBASE.VERSION";
    public static final String CELLBASE_REST_URL = "CELLBASE.REST.URL";
    public static final String CELLBASE_DB_HOST = "CELLBASE.DB.HOST";
    public static final String CELLBASE_DB_NAME = "CELLBASE.DB.NAME";
    public static final String CELLBASE_DB_PORT = "CELLBASE.DB.PORT";
    public static final String CELLBASE_DB_USER = "CELLBASE.DB.USER";
    public static final String CELLBASE_DB_PASSWORD = "CELLBASE.DB.PASSWORD";
    public static final String CELLBASE_DB_MAX_POOL_SIZE = "CELLBASE.DB.MAX_POOL_SIZE";
    public static final String CELLBASE_DB_TIMEOUT = "CELLBASE.DB.TIMEOUT";

    protected static Logger logger = LoggerFactory.getLogger(OpenCGAStorageMain.class);
    private static OptionsParser parser;

    enum AnnotationSource {
        CELLBASE_DB_ADAPTOR,
        CELLBASE_REST
    }

    static {
        // Finds the installation directory (opencgaHome).
        // Searches first in System Property "app.home" set by the shell script.
        // If not found, then in the environment variable "OPENCGA_HOME".
        // If none is found, it supposes "debug-mode" and the opencgaHome is in .../opencga/opencga-app/build/
        String propertyAppHome = System.getProperty("app.home");
        logger.debug("propertyAppHome = {}", propertyAppHome);
        if (propertyAppHome != null) {
            opencgaHome = propertyAppHome;
        } else {
            String envAppHome = System.getenv("OPENCGA_HOME");
            if (envAppHome != null) {
                opencgaHome = envAppHome;
            } else {
                opencgaHome = Paths.get("opencga-app", "build").toString(); //If it has not been run from the shell script (debug)
            }
        }
        Config.setOpenCGAHome(opencgaHome);
    }

    public static void main(String[] args)
            throws IOException, InterruptedException, IllegalOpenCGACredentialsException, FileFormatException,
            IllegalAccessException, InstantiationException, ClassNotFoundException, URISyntaxException {

        parser = new OptionsParser();
        OptionsParser.Command command = null;
        try {
            String parsedCommand = parser.parse(args);

            if(parser.getGeneralParameters().help || args.length == 0){
                System.out.println(parser.usage());
                return;
            }

            switch (parsedCommand) {
                case "index-variants":
                    command = parser.getCommandIndexVariants();
                    break;
                case "index-alignments":
                    command = parser.getCommandIndexAlignments();
                    break;
                case "index-sequences":
                    command = parser.getCommandIndexSequence();
                    break;
                case "create-accessions":
                    command = parser.getAccessionsCommand();
                    break;
                case "load-variants":
                    command = parser.getLoadCommand();
                    break;
                case "transform-variants":
                    command = parser.getTransformCommand();
                    break;
                case "transform-alignments":
                    command = parser.getTransformAlignments();
                    break;
                case "load-alignments":
                    command = parser.getLoadAlignments();
                    break;
                case "search-variants":
                case "fetch-variants":
                    command = parser.getCommandFetchVariants();
                    break;
                case "search-alignments":
                case "fetch-alignments":
                    command = parser.getCommandFetchAlignments();
                    break;
                case "create-annotations":
                    command = parser.getCommandCreateAnnotations();
                    break;
                case "load-annotations":
                    command = parser.getCommandLoadAnnotations();
                    break;
//                case "download-alignments":
//                    command = parser.getDownloadAlignments();
//                    break;
                default:
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
            if (c.input.endsWith(".bam") || c.input.endsWith(".sam")) {
                AlignmentStorageManager alignmentStorageManager = StorageManagerFactory.getAlignmentStorageManager(c.backend);
                ObjectMap params = new ObjectMap();
                params.putAll(c.params);

                if (c.fileId != null) {
                    params.put(AlignmentStorageManager.FILE_ID, c.fileId);
                }
                params.put(AlignmentStorageManager.PLAIN, false);
                params.put(AlignmentStorageManager.MEAN_COVERAGE_SIZE_LIST, Arrays.asList("200"));
                params.put(AlignmentStorageManager.INCLUDE_COVERAGE, true);
                params.put(AlignmentStorageManager.DB_NAME, c.dbName);
                params.put(AlignmentStorageManager.COPY_FILE, false);
                params.put(AlignmentStorageManager.ENCRYPT, "null");

                URI input = new URI(null, c.input, null);
                URI outdir = c.outdir.isEmpty() ? input.resolve(".") : new URI(null, c.outdir + "/", null).resolve(".");
//                Path tmp = c.tmp.isEmpty() ? outdir : Paths.get(URI.create(c.tmp).getPath());
//                Path credentials = Paths.get(c.credentials);

                URI nextFileUri;
                logger.info("-- Extract alignments -- {}", input);
                nextFileUri = alignmentStorageManager.extract(input, outdir, params);

                logger.info("-- PreTransform alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.preTransform(nextFileUri, params);
                logger.info("-- Transform alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.transform(nextFileUri, null, outdir, params);
                logger.info("-- PostTransform alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.postTransform(nextFileUri, params);

                logger.info("-- PreLoad alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.preLoad(nextFileUri, outdir, params);
                logger.info("-- Load alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.load(nextFileUri, params);
                logger.info("-- PostLoad alignments -- {}", nextFileUri);
                nextFileUri = alignmentStorageManager.postLoad(nextFileUri, outdir, params);

            } else {
                throw new IOException("Unknown file type");
            }
        } else if (command instanceof OptionsParser.CommandIndexSequence) {
            OptionsParser.CommandIndexSequence c = (OptionsParser.CommandIndexSequence) command;
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
        } else if (command instanceof OptionsParser.CommandIndexVariants) {
            OptionsParser.CommandIndexVariants c = (OptionsParser.CommandIndexVariants) command;
            if(c.input.endsWith(".vcf") || c.input.endsWith(".vcf.gz")) {
                VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager(c.backend);
                if(c.credentials != null && !c.credentials.isEmpty()) {
                    variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
                }

                URI variantsUri = new URI(null, c.input, null);
                URI pedigreeUri = c.pedigree != null && !c.pedigree.isEmpty() ? new URI(null, c.pedigree, null) : null;
                URI outdirUri = c.outdir != null && !c.outdir.isEmpty() ? new URI(null, c.outdir, null).resolve(".") : variantsUri.resolve(".");

                String fileName = variantsUri.resolve(".").relativize(variantsUri).toString();
                VariantSource source = new VariantSource(fileName, c.fileId, c.studyId, c.study, c.studyType, c.aggregated);

                ObjectMap params = new ObjectMap();
                params.put(VariantStorageManager.INCLUDE_EFFECT,  c.includeEffect);
                params.put(VariantStorageManager.INCLUDE_STATS, c.includeStats);
                params.put(VariantStorageManager.INCLUDE_SAMPLES, c.includeGenotype);   // TODO rename samples to genotypes
                params.put(VariantStorageManager.SOURCE, source);
                params.put(VariantStorageManager.DB_NAME, c.dbName);
                params.put(VariantStorageManager.ANNOTATE, c.annotate);
                params.put(VariantStorageManager.OVERWRITE_ANNOTATIONS, c.overwriteAnnotations);

                if(c.annotate) {
                    Properties annotatorProperties = Config.getStorageProperties();
                    if(c.annotatorConfig != null && !c.annotatorConfig.isEmpty()) {
                        annotatorProperties.load(new FileInputStream(c.annotatorConfig));
                    }
                    String cellbaseVersion = annotatorProperties.getProperty(CELLBASE_VERSION, "v3");
                    String cellbaseRest = annotatorProperties.getProperty(CELLBASE_REST_URL, "");

                    checkNull(cellbaseVersion, CELLBASE_VERSION);
                    checkNull(cellbaseRest, CELLBASE_REST_URL);

                    params.put(VariantStorageManager.CELLBASE_VERSION, cellbaseVersion);
                    params.put(VariantStorageManager.CELLBASE_REST_URL, cellbaseRest);
                }

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

                if (load) {
                    source.setFileName(fileName + ".variants.json.gz");

                    logger.info("-- PreLoad variants -- {}", nextFileUri);
                    nextFileUri = variantStorageManager.preLoad(nextFileUri, outdirUri, params);
                    logger.info("-- Load variants -- {}", nextFileUri);
                    nextFileUri = variantStorageManager.load(nextFileUri, params);
                    logger.info("-- PostLoad variants -- {}", nextFileUri);
                    nextFileUri = variantStorageManager.postLoad(nextFileUri, outdirUri, params);
                }

//                String fileName;
//                fileName = outdir != null
//                        ? outdir.resolve(Paths.get(source.getFileName()).getFileName() + ".variants.json.gz").toString()
//                        : source.getFileName() + ".variants.json.gz";

//                URI newInput = outdirUri.resolve(fileName + ".variants.json.gz");
//                source.setFileName(fileName + ".variants.json.gz");
            }

        } else if (command instanceof OptionsParser.CommandCreateAccessions) {
            OptionsParser.CommandCreateAccessions c = (OptionsParser.CommandCreateAccessions) command;

            Path variantsPath = Paths.get(c.input);
            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, c.studyId, null);
            createAccessionIds(variantsPath, source, c.prefix, c.resumeFromAccession, outdir);

        } else if (command instanceof OptionsParser.CommandTransformVariants) { //TODO: Add "preTransform and postTransform" call
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
            params.put(VariantStorageManager.SOURCE, source);

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
            params.put(VariantStorageManager.SOURCE, source);
            params.put(VariantStorageManager.DB_NAME, c.dbName);

            // TODO Right now it doesn't matter if the file is aggregated or not to save it to the database
            variantStorageManager.load(variantsUri, params);

     /*       Path variantsPath = Paths.get(c.input + ".variants.json.gz");
            Path filePath = Paths.get(c.input + ".file.json.gz");

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, null, null);
            indexVariants("load", source, variantsPath, filePath, null, c.backend, Paths.get(c.credentials), c.includeEffect, c.includeStats, c.includeSamples, null);
*/

//            indexVariants("load", source, variantsPath, filePath, null, c.backend, Paths.get(c.credentials), c.includeEffect, c.includeStats, c.includeSamples);

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

      /*

            Path filePath = Paths.get(c.file);
            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;
            String backend = c.plain ? "json" : "json.gz";

            if (!filePath.toFile().exists()) {
                throw new IOException("[Error] File not found : " + c.file);
            }

            indexAlignments(
                    c.study, //c.studyId,
                    filePath,
                    c.fileId, outdir, null,
                    backend, null, null,     //Credentials not needed
                    true,
                    false,              //Can't be loaded
                    c.includeCoverage, c.meanCoverage);*/

        } else if (command instanceof OptionsParser.CommandLoadAlignments){ //TODO: Add "preLoad" call
            OptionsParser.CommandLoadAlignments c = (OptionsParser.CommandLoadAlignments) command;
            AlignmentStorageManager alignmentStorageManager = StorageManagerFactory.getAlignmentStorageManager(c.backend);
            if(c.credentials != null && !c.credentials.isEmpty()) {
                alignmentStorageManager.addConfigUri(new URI(null, c.credentials, null));
            }

            ObjectMap params = new ObjectMap();

//            params.put(AlignmentStorageManager.INCLUDE_COVERAGE, true/*c.includeCoverage*/);
            params.put(AlignmentStorageManager.FILE_ID, c.fileId);
            params.put(AlignmentStorageManager.DB_NAME, c.dbName);

            URI inputUri = new URI(null, c.input, null);

            alignmentStorageManager.load(inputUri, params);


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
        } else if(command instanceof OptionsParser.CommandAnnotate) {
            OptionsParser.CommandAnnotate c = (OptionsParser.CommandAnnotate) command;

            /**
             * Create DBAdaptor
             */
            VariantStorageManager variantStorageManager = StorageManagerFactory.getVariantStorageManager(c.storageEngine);
            if(c.credentials != null && !c.credentials.isEmpty()) {
                variantStorageManager.addConfigUri(new URI(null, c.credentials, null));
            }
            ObjectMap params = new ObjectMap();
            VariantDBAdaptor dbAdaptor = variantStorageManager.getDBAdaptor(c.dbName, params);

            /**
             * Create Annotator
             */
            VariantAnnotator annotator;
            Properties annotatorProperties = Config.getStorageProperties();
            if(c.annotatorConfig != null && !c.annotatorConfig.isEmpty()) {
                annotatorProperties.load(new FileInputStream(c.annotatorConfig));
            }

            if(c.annotator == null) {
                c.annotator = AnnotationSource.valueOf(
                        annotatorProperties.getProperty(
                                OPENCGA_STORAGE_ANNOTATOR,
                                AnnotationSource.CELLBASE_REST.name()
                        ).toUpperCase()
                );
            }

            switch(c.annotator) {
                default:
                case CELLBASE_REST: {
                    String cellbaseVersion = annotatorProperties.getProperty(CELLBASE_VERSION, "v3");
                    String cellbaseRest = annotatorProperties.getProperty(CELLBASE_REST_URL, "");

                    checkNull(cellbaseVersion, "cellbaseVersion");
                    checkNull(cellbaseRest, "cellbaseRest");

                    URI url = new URI(cellbaseRest);
                    CellBaseClient cellBaseClient = new CellBaseClient(url.getHost(), url.getPort(), url.getPath(), cellbaseVersion, c.species);
                    annotator = new CellBaseVariantAnnotator(cellBaseClient);
                }
                break;
                case CELLBASE_DB_ADAPTOR: {
                    String cellbaseHost = annotatorProperties.getProperty(CELLBASE_DB_HOST, "");
                    String cellbaseDatabase = annotatorProperties.getProperty(CELLBASE_DB_NAME, "");
                    int cellbasePort = Integer.parseInt(annotatorProperties.getProperty(CELLBASE_DB_PORT, "27017"));
                    String cellbaseUser = annotatorProperties.getProperty(CELLBASE_DB_USER, "");
                    String cellbasePassword = annotatorProperties.getProperty(CELLBASE_DB_PASSWORD, "");
                    int maxPoolSize = Integer.parseInt(annotatorProperties.getProperty(CELLBASE_DB_MAX_POOL_SIZE, "10"));
                    int timeout = Integer.parseInt(annotatorProperties.getProperty(CELLBASE_DB_TIMEOUT, "200"));

                    checkNull(cellbaseHost, "cellbaseHost");
                    checkNull(cellbaseDatabase, "cellbaseDatabase");
                    checkNull(cellbasePort, "cellbasePort");
                    checkNull(cellbaseUser, "cellbaseUser");
                    checkNull(cellbasePassword, "cellbasePassword");


                    CellbaseConfiguration cellbaseConfiguration = new CellbaseConfiguration();
                    cellbaseConfiguration.addSpeciesConnection(
                            c.species,
                            c.assembly,
                            cellbaseHost,
                            cellbaseDatabase,
                            cellbasePort,
                            "mongo",    //TODO: Change to "mongodb"
                            cellbaseUser,
                            cellbasePassword,
                            maxPoolSize,
                            timeout);
                    cellbaseConfiguration.addSpeciesAlias(c.species, c.species);

                    annotator = new CellBaseVariantAnnotator(cellbaseConfiguration, c.species, c.assembly);
                }
                break;
            }
            org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager variantAnnotationManager =
                    new VariantAnnotationManager(annotator, dbAdaptor);

            QueryOptions queryOptions = new QueryOptions();
            Path outDir = Paths.get(c.outDir);

            if (c.create) {
                logger.info("staring annotation creation");
                variantAnnotationManager.createAnnotation(outDir, c.fileName.isEmpty() ? c.dbName : c.fileName, queryOptions);
            }

            if (c.load) {
                logger.info("finished annotation creation, starting annotation load");
                variantAnnotationManager.loadAnnotation(new URI(null, c.fileName, null), new QueryOptions());
            }
//            MongoCredentials cellbaseCredentials = new MongoCredentials(
//                    c.cellbaseHost,
//                    c.cellbasePort,
//                    c.cellbaseDatabase,
//                    c.cellbaseUser,
//                    c.cellbasePassword);
//            MongoCredentials opencgaCredentials = new MongoCredentials(
//                    c.opencgaHost,
//                    Integer.parseInt(c.opencgaPort),
//                    c.opencgaDatabase,
//                    c.opencgaUser,
//                    c.opencgaPassword);

//            VariantAnnotationManager variantAnnotationManager = new VariantAnnotationManager(cellbaseCredentials, opencgaCredentials);
//            variantAnnotationManager.annotate(c.cellbaseSpecies, c.cellbaseAssemly, null);
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


/*    @Deprecated
    private static void indexVariants(String step, VariantSource source, Path mainFilePath, Path auxiliaryFilePath, Path outdir, String backend,
                                      Path credentialsPath, boolean includeEffect, boolean includeStats, boolean includeSamples)//, String aggregated)
            throws IOException, IllegalOpenCGACredentialsException {

        VariantReader reader = null;
        PedigreeReader pedReader = ("transform".equals(step) && auxiliaryFilePath != null) ?
                new PedigreePedReader(auxiliaryFilePath.toString()) : null;

        if (source.getFileName().endsWith(".vcf") || source.getFileName().endsWith(".vcf.gz")) {
            switch (source.getAggregation()) {
                case NONE:
                    reader = new VariantVcfReader(source, mainFilePath.toAbsolutePath().toString());
                    break;
                case BASIC:
                    reader = new VariantVcfReader(source, mainFilePath.toAbsolutePath().toString(), new VariantAggregatedVcfFactory());
                    break;
                case EVS:
                    reader = new VariantVcfReader(source, mainFilePath.toAbsolutePath().toString(), new VariantVcfEVSFactory());
                    break;
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
        }
//        else if (backend.equalsIgnoreCase("sqlite")) {
//            credentials = new SqliteCredentials(properties);
//            writers.add(new VariantVcfSqliteWriter((SqliteCredentials) credentials));
//        } else if (backend.equalsIgnoreCase("monbase")) {
//            credentials = new MonbaseCredentials(properties);
//            writers.add(new VariantVcfMonbaseDataWriter(source, "opencga-hsapiens", (MonbaseCredentials) credentials));// TODO Restore when SQLite and Monbase are once again ready!!
//        }


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
*/
/*

    @Deprecated
    private static void indexAlignments(
            String study, //String studyId,
            Path filePath, 
            String fileId, Path outdir, Region region, 
            String backend, Path credentialsPath, Configuration config, 
            boolean compact,
            boolean loadCoverage,
            boolean calculeCoverage, List<String> meanCoverageValues) throws IOException {
        String TABLE_NAME = "alignments";
        AlignmentDataReader reader;
        //List<AlignmentDataWriter<Alignment, AlignmentHeader>> writers = new LinkedList<>();
        List<DataWriter<AlignmentRegion>> writers = new LinkedList<>();
        List<Task<AlignmentRegion>> tasks = new LinkedList<>();
        if(outdir == null) outdir = Paths.get(".");
        Properties properties = null;
        
        if(credentialsPath != null){
            properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(credentialsPath.toString())));
        }
//
//            Configure Readers
//
        
        String filePathString = filePath.toAbsolutePath().toString();
        String lowerCaseFileName = filePath.getFileName().toString().toLowerCase();
        String baseFileName = filePathString.substring(0, filePathString.lastIndexOf("."));
        String extension = filePathString.substring(filePathString.lastIndexOf("."), filePathString.length());
        if (lowerCaseFileName.endsWith(".sam")) {
            reader = new AlignmentSamDataReader(filePathString, study);
        } else if (lowerCaseFileName.endsWith(".bam")) {
            reader = new AlignmentBamDataReader(filePathString, study);
        } else if (lowerCaseFileName.endsWith(".json")) {
//            String headerJsonFilename = filePathString.substring(0, filePathString.lastIndexOf(".", 2)) + ".header.json";
//            reader = new AlignmentJsonDataReader(filePathString, headerJsonFilename);
            baseFileName = baseFileName.substring(0, baseFileName.lastIndexOf("."));
            reader = new AlignmentJsonDataReader(baseFileName, false);
        } else if(lowerCaseFileName.endsWith(".json.gz")){
//            String headerJsonFilename = filePathString.substring(0, filePathString.lastIndexOf(".", 3)) + ".header.json.gz";
//            reader = new AlignmentJsonDataReader(filePathString, headerJsonFilename);
            baseFileName = baseFileName.substring(0, baseFileName.lastIndexOf("."));
            baseFileName = baseFileName.substring(0, baseFileName.lastIndexOf("."));
            extension = ".json.gz";
            reader = new AlignmentJsonDataReader(baseFileName, true);
        } else if(lowerCaseFileName.endsWith(".hbase")){
            AlignmentHBaseDataReader hbdr = new AlignmentHBaseDataReader(properties, TABLE_NAME,  baseFileName);
            reader = hbdr;
            if(region != null) {
                hbdr.setRegion(region);
            }
        } else {
            throw new IOException("Alignment input file format not supported : " + filePath);
        }
        
        
//
//            Configure Backend
//
        if(backend.equalsIgnoreCase("json") || backend.equalsIgnoreCase("json.gz")) {
            boolean gzip = backend.endsWith(".gz");
            String baseOutputFilename = Paths.get(outdir.toString(), fileId).toAbsolutePath().toString();
            writers.add(new AlignmentRegionDataWriter(new AlignmentJsonDataWriter(reader, baseOutputFilename, gzip)));
            if(calculeCoverage || loadCoverage){
                writers.add(new AlignmentCoverageJsonDataWriter(baseOutputFilename, gzip));
            }
        } else if (backend.equalsIgnoreCase("hbase")) {
            if (calculeCoverage || loadCoverage) {
                if (properties != null){
                    writers.add(new AlignmentRegionCoverageHBaseDataWriter(properties, TABLE_NAME , Paths.get(baseFileName).getFileName().toString()));
                }else{
                    writers.add(new AlignmentRegionCoverageHBaseDataWriter(config, TABLE_NAME, Paths.get(baseFileName).getFileName().toString()));
                }
            }
            if (properties != null){
                writers.add(new AlignmentRegionHBaseDataWriter( properties, TABLE_NAME, Paths.get(baseFileName).getFileName().toString(), reader));
            }else{
                writers.add(new AlignmentRegionHBaseDataWriter( config, TABLE_NAME, Paths.get(baseFileName).getFileName().toString(), reader));
            }
        } else if (backend.equalsIgnoreCase("sam")) {
            if(calculeCoverage || loadCoverage){
                throw new UnsupportedOperationException("Can't write coverage in this backend.");
            }
            System.out.println("[CAUTION] Unimplemented Extraction");
            if(fileId == null){
                fileId = "/tmp/unimplemented.sam";
            }
            writers.add(new AlignmentRegionDataWriter(new AlignmentSamDataWriter(fileId, reader)));
        } else {
            throw new IOException("Alignment backend format not supported : " + backend);
        }
        
//
//         Configure Tasks
//
        if(compact){
            QueryOptions queryOptions = new QueryOptions();
            AlignmentRegionCompactorTask alignmentRegionCompactorTask = new AlignmentRegionCompactorTask(queryOptions);
            tasks.add(alignmentRegionCompactorTask);
        }
        if(calculeCoverage){
            AlignmentRegionCoverageCalculatorTask coverageTask = new AlignmentRegionCoverageCalculatorTask();
            for(String name : meanCoverageValues){
                coverageTask.addMeanCoverageCalculator(name);
            }
            tasks.add(coverageTask);
        }
        if(loadCoverage) {
            String coverageJsonFilename;
            if (lowerCaseFileName.endsWith(".json")) {
                //coverageJsonFilename = filePathString.substring(0, filePathString.lastIndexOf(".", 2)) + ".coverage.json";
                coverageJsonFilename = baseFileName += ".coverage.json";
            } else if (lowerCaseFileName.endsWith(".json.gz")) {
                //coverageJsonFilename = filePathString.substring(0, filePathString.lastIndexOf(".", 2)) + ".coverage.json.gz";
                coverageJsonFilename = baseFileName += ".coverage.json.gz";
            } else {
                throw new UnsupportedOperationException("Coverage can be loaded only from Json");
            }
            AlignmentRegionCoverageFromJsonTask alignmentRegionCoverageFromJsonTask = new AlignmentRegionCoverageFromJsonTask(coverageJsonFilename);
            tasks.add(alignmentRegionCoverageFromJsonTask);
        }
        
        
        AlignmentRegionDataReader regionReader = new AlignmentRegionDataReader(reader);
        Runner<AlignmentRegion> runner = new Runner<>(regionReader, writers, tasks, 1);
        
        runner.run();
    }


    //TODO
    private static void transformAlignments(CommandTransformAlignments c) throws IOException{
        AlignmentDataReader reader;
        List<DataWriter<AlignmentRegion>> writers = new LinkedList<>();
        List<Task<AlignmentRegion>> tasks = new LinkedList<>();

        if(c.file.endsWith(".sam")){
            reader = new AlignmentSamDataReader(c.file, c.study);
        } else if (c.file.endsWith(".bam")) {
            reader = new AlignmentBamDataReader(c.file, c.study);
        } else {
            throw new UnsupportedOperationException("[ERROR] Unsuported file input format : " + c.file);
        }
        
        writers.add(new AlignmentRegionDataWriter(new AlignmentJsonDataWriter(reader, Paths.get(c.outdir,c.fileId).toString(), c.plain)));
        
        AlignmentRegionDataReader regionReader = new AlignmentRegionDataReader(reader);
        Runner<AlignmentRegion> runner = new Runner<>(regionReader, writers, tasks, 1);

        runner.run();
    }
    
    //TODO
    private static void loadAlignments(CommandLoadAlignments c) throws IOException  {
        AlignmentDataReader reader;
        Properties properties = null;
        if (c.credentials != null) {
            properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(c.credentials)));
        }
        
        switch(c.backend){
            case "hbase" :
                
                break;
            default:
                throw new UnsupportedOperationException("[ERROR] Unsupported backend : " + c.backend);
        }
        
    }
    private static void downloadAlignments(CommandDownloadAlignments c) throws IOException {
        AlignmentDataReader reader;
        Properties properties = null;

        if (c.credentials != null) {
            properties = new Properties();
            properties.load(new InputStreamReader(new FileInputStream(c.credentials)));
        }
        switch(c.backend){
            case "hbase":
                AlignmentHBaseDataReader hb = new AlignmentHBaseDataReader(properties, "alignments", c.alias);
                if(c.region != null){
                    hb.setRegion(new Region(c.region));
                }
                reader = hb;
                break;
            default:
                throw new UnsupportedOperationException("[ERROR] Unsupported backend : " + c.backend);
        }
        List<DataWriter<AlignmentRegion>> writers = new LinkedList<>();
        boolean gzip = false;
        switch(c.format){
            case "json.gz":
                gzip = true;
            case "json":
                writers.add(new AlignmentRegionDataWriter(new AlignmentJsonDataWriter(reader, Paths.get(c.outdir, c.alias).toString(), gzip)));
                break;
            case "sam":
                writers.add(new AlignmentRegionDataWriter(new AlignmentSamDataWriter(Paths.get(c.outdir, c.alias).toString(), reader)));
                break;
            case "bam":
                writers.add(new AlignmentRegionDataWriter(new AlignmentBamDataWriter(Paths.get(c.outdir, c.alias).toString(), reader)));
                break;
            default:
                throw new UnsupportedOperationException("[ERROR] Unsupported format : " + c.format);
        }
        List<Task<AlignmentRegion>> tasks = new LinkedList<>();

        AlignmentRegionDataReader regionReader = new AlignmentRegionDataReader(reader);
        Runner<AlignmentRegion> runner = new Runner<>(regionReader, writers, tasks, 1);

        runner.run();
    }*/
    
}
