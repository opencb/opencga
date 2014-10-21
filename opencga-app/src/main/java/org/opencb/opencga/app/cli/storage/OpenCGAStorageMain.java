package org.opencb.opencga.app.cli.storage;


import com.beust.jcommander.ParameterException;
import com.google.common.io.Files;
import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.vcf4.VcfRecord;
import org.opencb.biodata.formats.variant.vcf4.io.VcfRawReader;
import org.opencb.biodata.formats.variant.vcf4.io.VcfRawWriter;
import org.opencb.biodata.models.variant.*;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.Runner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.opencga.app.cli.storage.OptionsParser.Command;
import org.opencb.opencga.app.cli.storage.OptionsParser.CommandCreateAccessions;
import org.opencb.opencga.app.cli.storage.OptionsParser.CommandLoadVariants;
import org.opencb.opencga.app.cli.storage.OptionsParser.CommandTransformVariants;
import org.opencb.opencga.app.cli.storage.OptionsParser.CommandLoadAlignments;
import org.opencb.opencga.app.cli.storage.OptionsParser.CommandTransformAlignments;
import org.opencb.opencga.app.cli.storage.OptionsParser.CommandDownloadAlignments;
import org.opencb.opencga.app.cli.storage.OptionsParser.CommandIndexAlignments;
import org.opencb.opencga.app.cli.storage.OptionsParser.CommandIndexVariants;
import org.opencb.opencga.app.cli.storage.OptionsParser.CommandIndexSequence;
import org.opencb.opencga.lib.auth.IllegalOpenCGACredentialsException;
import org.opencb.opencga.lib.common.Config;
import org.opencb.opencga.lib.tools.accession.CreateAccessionTask;
import org.opencb.opencga.storage.core.alignment.AlignmentStorageManager;
import org.opencb.opencga.storage.core.sequence.SqliteSequenceDBAdaptor;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Cristina Yenyxe Gonzalez Garcia
 */
public class OpenCGAStorageMain {

//    private static final String OPENCGA_HOME = System.getenv("OPENCGA_HOME");
    private static final String appHome;

    protected static Logger logger = LoggerFactory.getLogger(OpenCGAStorageMain.class);

    static {
        appHome = System.getProperty("app.home");
        Config.setGcsaHome(appHome);
    }

    public static void main(String[] args) throws IOException, InterruptedException, IllegalOpenCGACredentialsException, FileFormatException {
        AlignmentStorageManager alignmentStorageManager = null;
        VariantStorageManager variantStorageManager = null;
        //StorageManager storageManager = null; //TODO: Use only one generic StorageManager instead of one for variant and other for alignment

        OptionsParser parser = new OptionsParser();
        boolean variantCommand = false;
        boolean alignmentCommand = false;
        Command command = null;
        try {
            String parsedCommand = parser.parse(args);

            if(parser.getGeneralParameters().help || args.length == 0){
                System.out.println(parser.usage());
                return;
            }

            switch (parsedCommand) {
                case "index-variants":
                    command = parser.getCommandIndexVariants();
                    variantCommand = true;
                    break;
                case "index-alignments":
                    command = parser.getCommandIndexAlignments();
                    alignmentCommand = true;
                    break;
                case "index-sequences":
                    command = parser.getCommandIndexSequence();
                    break;
                case "create-accessions":
                    command = parser.getAccessionsCommand();
                    break;
                case "load-variants":
                    variantCommand = true;
                    command = parser.getLoadCommand();
                    break;
                case "transform-variants":
                    variantCommand = true;
                    command = parser.getTransformCommand();
                    break;
                case "transform-alignments":
                    alignmentCommand = true;
                    command = parser.getTransformAlignments();
                    break;
                case "load-alignments":
                    alignmentCommand = true;
                    command = parser.getLoadAlignments();
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

        /*
            Get properties conf
         */
        Path confPath = Paths.get("opencga-app", "build", "conf");
        Path storagePropertiesPath = Paths.get(confPath.toString() , "storage.properties");
        Properties storageProperties = new Properties();
        if(storagePropertiesPath.toFile().exists()) {
            storageProperties.load(new FileInputStream(storagePropertiesPath.toFile()));
        }


        //Get the StorageManager
        try {
            if(variantCommand) {
                //Get Storage Manager name
                String defaultVariantStorageManager = storageProperties.getProperty("OPENCGA.STORAGE.DEFAULT.VARIANT");
                String variantManagerName = parser.getGeneralParameters().storageManagerName !=null ?
                        parser.getGeneralParameters().storageManagerName :
                        storageProperties.getProperty(defaultVariantStorageManager);
                //New instance
                variantStorageManager = (VariantStorageManager) Class.forName(variantManagerName).newInstance();
                //Add configuration files
                String pluginPropertiesName = storageProperties.getProperty(defaultVariantStorageManager + ".PROPERTIES");
                Path pluginPropertiesPath = confPath.resolve(pluginPropertiesName);
                variantStorageManager.addPropertiesPath(storagePropertiesPath);
                variantStorageManager.addPropertiesPath(pluginPropertiesPath);
            }
            if(alignmentCommand) {
                //Get Storage Manager name
                String defaultAlignmentStorageManager = storageProperties.getProperty("OPENCGA.STORAGE.DEFAULT.ALIGNMENT");
                String alignmentManagerName = parser.getGeneralParameters().storageManagerName != null ?
                        parser.getGeneralParameters().storageManagerName :
                        storageProperties.getProperty(defaultAlignmentStorageManager);
                //New instance
                alignmentStorageManager = (AlignmentStorageManager) Class.forName(alignmentManagerName).newInstance();
                //Add configuration files
                String pluginPropertiesName = storageProperties.getProperty(defaultAlignmentStorageManager + ".PROPERTIES");
                Path pluginPropertiesPath = confPath.resolve(pluginPropertiesName);
                alignmentStorageManager.addPropertiesPath(storagePropertiesPath);
                alignmentStorageManager.addPropertiesPath(pluginPropertiesPath);
            }
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            logger.error("Error during the reflexion",e);
            e.printStackTrace();
        }

        if (command instanceof CommandIndexAlignments) {
            CommandIndexAlignments c = (CommandIndexAlignments) command;
            if (c.input.endsWith(".bam") || c.input.endsWith(".sam")) {
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

                Path input = Paths.get(URI.create(c.input).getPath());
                Path outdir = c.outdir.isEmpty() ? input.getParent() : Paths.get(URI.create(c.outdir).getPath());
                Path tmp = c.tmp.isEmpty() ? outdir : Paths.get(URI.create(c.tmp).getPath());
                Path credentials = Paths.get(c.credentials);

                assert alignmentStorageManager != null;
                logger.info("1 -- Transform alignments");
                alignmentStorageManager.transform(input, null, outdir, params);

                logger.info("2 -- PreLoad alignments");
                alignmentStorageManager.preLoad(input, outdir, params);

                logger.info("3 -- Load alignments");
                alignmentStorageManager.load(input, credentials, params);

            } else {
                throw new IOException("Unknown file type");
            }
        } else if (command instanceof CommandIndexSequence) {
            CommandIndexSequence c = (CommandIndexSequence) command;
            if (c.input.endsWith(".fasta") || c.input.endsWith(".fasta.gz")) {
                Path input = Paths.get(URI.create(c.input).getPath());
                Path outdir = c.outdir.isEmpty() ? input.getParent() : Paths.get(URI.create(c.outdir).getPath());

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
        } else if (command instanceof CommandIndexVariants) {
            CommandIndexVariants c = (CommandIndexVariants) command;
            if(c.input.endsWith(".vcf") || c.input.endsWith(".vcf.gz")) {
                Path variantsPath = Paths.get(c.input);
                Path pedigreePath = c.pedigree != null ? Paths.get(c.pedigree) : null;
                Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;
                Path credentials = Paths.get(c.credentials);
                VariantSource source = new VariantSource(variantsPath.getFileName().toString(), c.fileId, c.studyId, c.study, c.studyType, c.aggregated);

                Map<String, Object> params = new LinkedHashMap<>();
                params.put(VariantStorageManager.INCLUDE_EFFECT,  c.includeEffect);
                params.put(VariantStorageManager.INCLUDE_STATS, c.includeStats);
                params.put(VariantStorageManager.INCLUDE_SAMPLES, c.includeSamples);
                params.put(VariantStorageManager.SOURCE, source);

                assert variantStorageManager != null;
                logger.info("1 -- Transform variants");
                variantStorageManager.transform(variantsPath, pedigreePath, outdir, params);

                String fileName;
                fileName = outdir != null
                        ? outdir.resolve(Paths.get(source.getFileName()).getFileName() + ".variant.json.gz").toString()
                        : source.getFileName() + ".variant.json.gz";
                source.setFileName(fileName);
                logger.info("2 -- Preload variants");

                logger.info("3 -- Transform variants");
                variantStorageManager.load(variantsPath, credentials, params);
            }

        } else if (command instanceof CommandCreateAccessions) {
            CommandCreateAccessions c = (CommandCreateAccessions) command;

            Path variantsPath = Paths.get(c.input);
            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, c.studyId, null);
            createAccessionIds(variantsPath, source, c.prefix, c.resumeFromAccession, outdir);

        } else if (command instanceof CommandTransformVariants) { //TODO: Add "preTransform and postTransform" call
            CommandTransformVariants c = (CommandTransformVariants) command;
            Path variantsPath = Paths.get(c.file);
            Path pedigreePath = c.pedigree != null ? Paths.get(c.pedigree) : null;
            Path outdir = c.outdir != null ? Paths.get(c.outdir) : null;
            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), c.fileId, c.studyId, c.study, c.studyType, c.aggregated);

            Map<String, Object> params = new LinkedHashMap<>();
            params.put(VariantStorageManager.INCLUDE_EFFECT,  c.includeEffect);
            params.put(VariantStorageManager.INCLUDE_STATS,   c.includeStats);
            params.put(VariantStorageManager.INCLUDE_SAMPLES, c.includeSamples);
            params.put(VariantStorageManager.SOURCE, source);

            variantStorageManager.transform(variantsPath, pedigreePath, outdir, params);

//            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), c.fileId, c.studyId, c.study);
//            indexVariants("transform", source, variantsPath, pedigreePath, outdir, "json", null, c.includeEffect, c.includeStats, c.includeSamples, c.aggregated);

        } else if (command instanceof CommandLoadVariants) {    //TODO: Add "preLoad" call
            CommandLoadVariants c = (CommandLoadVariants) command;

            //Path variantsPath = Paths.get(c.input + ".variants.json.gz");
            Path variantsPath = Paths.get(c.input);
            Path credentials = c.credentials == null? null : Paths.get(c.credentials);
            VariantStudy.StudyType st = c.studyType;
            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, null, null, st, VariantSource.Aggregation.NONE);

            Map<String, Object> params = new LinkedHashMap<>();
            params.put(VariantStorageManager.INCLUDE_EFFECT,  c.includeEffect);
            params.put(VariantStorageManager.INCLUDE_STATS, c.includeStats);
            params.put(VariantStorageManager.INCLUDE_SAMPLES, c.includeSamples);
            params.put(VariantStorageManager.SOURCE, source);

            // TODO Right now it doesn't matter if the file is aggregated or not to save it to the database
            variantStorageManager.load(variantsPath, credentials, params);

     /*       Path variantsPath = Paths.get(c.input + ".variants.json.gz");
            Path filePath = Paths.get(c.input + ".file.json.gz");

            VariantSource source = new VariantSource(variantsPath.getFileName().toString(), null, null, null);
            indexVariants("load", source, variantsPath, filePath, null, c.backend, Paths.get(c.credentials), c.includeEffect, c.includeStats, c.includeSamples, null);
*/

//            indexVariants("load", source, variantsPath, filePath, null, c.backend, Paths.get(c.credentials), c.includeEffect, c.includeStats, c.includeSamples);

        } else if (command instanceof CommandTransformAlignments) { //TODO: Add "preTransform and postTransform" call

            CommandTransformAlignments c = (CommandTransformAlignments) command;

            ObjectMap params = new ObjectMap();

            if(c.fileId != null) {
                params.put(AlignmentStorageManager.FILE_ID, c.fileId);
            }
            //params.put(AlignmentStorageManager.STUDY,   c.study);
            params.put(AlignmentStorageManager.PLAIN,   c.plain);
            params.put(AlignmentStorageManager.MEAN_COVERAGE_SIZE_LIST, c.meanCoverage);
            params.put(AlignmentStorageManager.INCLUDE_COVERAGE, c.includeCoverage);

            Path input = Paths.get(c.file);
            Path output = Paths.get(c.outdir);

            alignmentStorageManager.transform(input, null, output, params);

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

        } else if (command instanceof CommandLoadAlignments){ //TODO: Add "preLoad" call
            CommandLoadAlignments c = (CommandLoadAlignments) command;

            ObjectMap params = new ObjectMap();

//            params.put(AlignmentStorageManager.INCLUDE_COVERAGE, true/*c.includeCoverage*/);
            params.put(AlignmentStorageManager.FILE_ID, c.fileId);
            params.put(AlignmentStorageManager.DB_NAME, c.dbName);

            Path input = Paths.get(c.input);
            Path credentials = Paths.get(c.credentials);

            alignmentStorageManager.load(input, credentials, params);

            
        } else if(command instanceof CommandDownloadAlignments){
            CommandDownloadAlignments c = (CommandDownloadAlignments) command;
           /* downloadAlignments(c);*/

        }
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
