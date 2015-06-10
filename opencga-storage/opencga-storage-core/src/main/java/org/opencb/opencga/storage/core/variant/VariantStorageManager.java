/*
 * Copyright 2015 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.opencb.opencga.storage.core.variant;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.pedigree.io.PedigreePedReader;
import org.opencb.biodata.formats.pedigree.io.PedigreeReader;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.*;
import org.opencb.biodata.tools.variant.tasks.VariantRunner;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.runner.SimpleThreadRunner;
import org.opencb.opencga.storage.core.runner.StringDataReader;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonReader;
import org.opencb.opencga.storage.core.variant.io.json.VariantJsonWriter;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageManager extends StorageManager<VariantWriter, VariantDBAdaptor> {

    public static final String INCLUDE_STATS = "include.stats";              //Include existing stats on the original file.
    public static final String INCLUDE_GENOTYPES = "include.genotypes";      //Include sample information (genotypes)
    public static final String INCLUDE_SRC = "include.src";                  //Include original source file on the transformed file and the final db
    public static final String COMPRESS_GENOTYPES = "compressGenotypes";    //Stores sample information as compressed genotypes
//    @Deprecated public static final String VARIANT_SOURCE = "variantSource";            //VariantSource object
    public static final String STUDY_CONFIGURATION = "studyConfiguration";      //
    public static final String STUDY_CONFIGURATION_MANAGER_CLASS_NAME         = "studyConfigurationManagerClassName";
    public static final String AGGREGATED_TYPE = "aggregatedType";
    public static final String STUDY_NAME = "studyName";
    public static final String STUDY_ID = "studyId";
    public static final String FILE_ID = "fileId";
    public static final String STUDY_TYPE = "studyType";
    public static final String SAMPLE_IDS = "sampleIds";
    public static final String COMPRESS_METHOD = "compressMethod";

    public static final String CALCULATE_STATS = "calculateStats";          //Calculate stats on the postLoad step
    public static final String OVERWRITE_STATS = "overwriteStats";          //Overwrite stats already present
    public static final String AGGREGATION_MAPPING_PROPERTIES = "aggregationMappingFile";

    public static final String DB_NAME = "database.name";
    public static final String SPECIES = "species";

    public static final String ASSEMBLY = "assembly";
    public static final String ANNOTATE = "annotate";
    public static final String ANNOTATION_SOURCE = "annotationSource";
    public static final String ANNOTATOR_PROPERTIES = "annotatorProperties";
    public static final String OVERWRITE_ANNOTATIONS = "overwriteAnnotations";

    public static final String BATCH_SIZE = "batch.size";
    public static final String TRANSFORM_THREADS = "transform.threads";
    public static final String LOAD_THREADS = "load.threads";

//    public static final String OPENCGA_STORAGE_VARIANT_TRANSFORM_THREADS      = "OPENCGA.STORAGE.VARIANT.TRANSFORM.THREADS";
//    public static final String OPENCGA_STORAGE_VARIANT_LOAD_THREADS           = "OPENCGA.STORAGE.VARIANT.LOAD.THREADS";
//    public static final String OPENCGA_STORAGE_VARIANT_TRANSFORM_BATCH_SIZE   = "OPENCGA.STORAGE.VARIANT.TRANSFORM.BATCH_SIZE";
//    public static final String OPENCGA_STORAGE_VARIANT_INCLUDE_SRC            = "OPENCGA.STORAGE.VARIANT.INCLUDE_SRC";
//    public static final String OPENCGA_STORAGE_VARIANT_INCLUDE_SAMPLES        = "OPENCGA.STORAGE.VARIANT.INCLUDE_SAMPLES";
//    public static final String OPENCGA_STORAGE_VARIANT_INCLUDE_STATS          = "OPENCGA.STORAGE.VARIANT.INCLUDE_STATS";

//    @Deprecated protected Properties properties;
    protected static Logger logger;

    public VariantStorageManager() {
//        this.properties = new Properties();
        logger = LoggerFactory.getLogger(VariantStorageManager.class);
    }

//    public VariantStorageManager() {
//        this(null);
//    }

    public VariantStorageManager(StorageConfiguration configuration) {
        super(configuration);
        logger = LoggerFactory.getLogger(VariantStorageManager.class);
    }

    public VariantStorageManager(String storageEngineId, StorageConfiguration configuration) {
        super(storageEngineId, configuration);
        logger = LoggerFactory.getLogger(VariantStorageManager.class);
    }

    @Override
    @Deprecated
    public void addConfigUri(URI configUri){
        logger.warn("Ignoring Config URI {} ", configUri);
//        if(configUri != null
//                && Paths.get(configUri.getPath()).toFile().exists()
//                && (configUri.getScheme() == null || configUri.getScheme().equals("file"))) {
//            try {
//                properties.load(new InputStreamReader(new FileInputStream(configUri.getPath())));
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
    }

    @Override
    public URI extract(URI input, URI ouput) {
        return input;
    }

    @Override
    public URI preTransform(URI input) throws StorageManagerException, IOException, FileFormatException {
        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        if (studyConfiguration == null) {
            logger.info("Creating a new StudyConfiguration");
            studyConfiguration = new StudyConfiguration(options.getInt(STUDY_ID), options.getString(STUDY_NAME));
            options.put(STUDY_CONFIGURATION, studyConfiguration);
        }
        String fileName = Paths.get(input.getPath()).getFileName().toString();
        Integer fileId = options.getInt(FILE_ID);    //TODO: Transform into an optional field

        checkNewFile(studyConfiguration, fileId, fileName);

        return input;
    }

    /**
     * Transform raw variant files into biodata model.
     *
     * @param inputUri         Input file. Accepted formats: *.vcf, *.vcf.gz
     * @param pedigreeUri      Pedigree input file. Accepted formats: *.ped
     * @param outputUri
     * @throws IOException
     */
    @Override
    final public URI transform(URI inputUri, URI pedigreeUri, URI outputUri) throws StorageManagerException {
        // input: VcfReader
        // output: JsonWriter

        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        Path input = Paths.get(inputUri.getPath());
        Path pedigree = pedigreeUri == null? null : Paths.get(pedigreeUri.getPath());
        Path output = Paths.get(outputUri.getPath());


//        boolean includeSamples = params.getBoolean(INCLUDE_GENOTYPES, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_SAMPLES, "false")));
//        boolean includeStats = params.getBoolean(INCLUDE_STATS, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_STATS, "false")));
//        boolean includeSrc = params.getBoolean(INCLUDE_SRC, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_SRC, "false")));

        boolean includeSamples = options.getBoolean(INCLUDE_GENOTYPES, false);
        boolean includeStats = options.getBoolean(INCLUDE_STATS, false);
        boolean includeSrc = options.getBoolean(INCLUDE_SRC, false);

        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        Integer fileId = options.getInt(FILE_ID);    //TODO: Transform into an optional field
        VariantSource.Aggregation aggregation = options.get(AGGREGATED_TYPE, VariantSource.Aggregation.class, VariantSource.Aggregation.NONE);
        String fileName = input.getFileName().toString();
        VariantStudy.StudyType type = options.get(STUDY_TYPE, VariantStudy.StudyType.class, VariantStudy.StudyType.CASE_CONTROL);
        VariantSource source = new VariantSource(
                fileName,
                fileId.toString(),
                Integer.toString(studyConfiguration.getStudyId()),
                studyConfiguration.getStudyName(), type, aggregation);

        int batchSize = options.getInt(BATCH_SIZE, 100);
        String compression = options.getString(COMPRESS_METHOD, "snappy");
        String extension = "";
        int numThreads = options.getInt(VariantStorageManager.TRANSFORM_THREADS, 8);
        int capacity = options.getInt("blockingQueueCapacity", numThreads*2);

        if (compression.equalsIgnoreCase("gzip") || compression.equalsIgnoreCase("gz")) {
            extension = ".gz";
        } else if (compression.equalsIgnoreCase("snappy") || compression.equalsIgnoreCase("snz")) {
            extension = ".snappy";
        } else if (!compression.isEmpty()) {
            throw new IllegalArgumentException("Unknown compression method " + compression);
        }

        // TODO Create a utility to determine which extensions are variants files
        final VariantVcfFactory factory;
        if (fileName.endsWith(".vcf") || fileName.endsWith(".vcf.gz") || fileName.endsWith(".vcf.snappy")) {
            switch (aggregation) {
                default:
                case NONE:
                    factory = new VariantVcfFactory();
                    break;
                case BASIC:
                    factory = new VariantAggregatedVcfFactory();
                    break;
                case EVS:
                    factory = new VariantVcfEVSFactory(options.get(AGGREGATION_MAPPING_PROPERTIES, Properties.class, null));
                    break;
                case EXAC:
                    factory = new VariantVcfExacFactory(options.get(AGGREGATION_MAPPING_PROPERTIES, Properties.class, null));
                    break;
            }
        } else {
            throw new StorageManagerException("Variants input file format not supported");
        }


        Path outputVariantJsonFile = output.resolve(fileName + ".variants.json" + extension);
        Path outputFileJsonFile = output.resolve(fileName + ".file.json" + extension);

        logger.info("Transforming variants...");
        long start, end;
        if (numThreads == 1) { //Run transformation with a SingleThread runner. The legacy way
            if (!extension.equals(".gz")) { //FIXME: Add compatibility with snappy compression
                logger.warn("Force using gzip compression");
                extension = ".gz";
                outputVariantJsonFile = output.resolve(fileName + ".variants.json" + extension);
                outputFileJsonFile = output.resolve(fileName + ".file.json" + extension);
            }

            //Ped Reader
            PedigreeReader pedReader = null;
            if(pedigree != null && pedigree.toFile().exists()) {    //FIXME Add "endsWith(".ped") ??
                pedReader = new PedigreePedReader(pedigree.toString());
            }

            //Reader
            VariantReader reader = new VariantVcfReader(source, input.toAbsolutePath().toString());

            //Writers
            VariantJsonWriter jsonWriter = new VariantJsonWriter(source, output);
            jsonWriter.includeSrc(includeSrc);
            jsonWriter.includeSamples(includeSamples);
            jsonWriter.includeStats(includeStats);

            List<VariantWriter> writers = Collections.<VariantWriter>singletonList(jsonWriter);

            //Runner
            VariantRunner vr = new VariantRunner(source, reader, pedReader, writers, Collections.<Task<Variant>>emptyList(), batchSize);

            logger.info("Single thread transform...");
            start = System.currentTimeMillis();
            try {
                vr.run();
            } catch (IOException e) {
                throw new StorageManagerException("Fail runner execution", e);
            }
            end = System.currentTimeMillis();

        } else {
            //Read VariantSource
            source = readVariantSource(input, source);

            //Reader
            StringDataReader dataReader = new StringDataReader(input);

            //Writers
            StringDataWriter dataWriter = new StringDataWriter(outputVariantJsonFile);

            SimpleThreadRunner runner = new SimpleThreadRunner(
                    dataReader,
                    Collections.<Task>singletonList(new VariantJsonTransformTask(factory, source, outputFileJsonFile)),
                    dataWriter,
                    batchSize,
                    capacity,
                    numThreads);

            logger.info("Multi thread transform...");
            start = System.currentTimeMillis();
            runner.run();
            end = System.currentTimeMillis();
        }
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants transformed!");

        return outputUri.resolve(outputVariantJsonFile.getFileName().toString());
    }

    @Override
    public URI postTransform(URI input) throws IOException, FileFormatException {
        return input;
    }

    @Override
    public URI preLoad(URI input, URI output) throws StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        if (studyConfiguration == null) {
            logger.info("Creating a new StudyConfiguration");
            studyConfiguration = new StudyConfiguration(options.getInt(STUDY_ID), options.getString(STUDY_NAME));
            options.put(STUDY_CONFIGURATION, studyConfiguration);
        }

        //TODO: Expect JSON file
        VariantSource source = readVariantSource(Paths.get(input.getPath()), null);

        /*
         * Before load file, check and add fileName to the StudyConfiguration.
         * FileID and FileName is read from the VariantSource
         * Will fail if:
         *     fileId is not an integer
         *     fileId was already in the studyConfiguration
         *     fileName was already in the studyConfiguration
         */

        int fileId;
        String fileName = source.getFileName();
        try {
            fileId = Integer.parseInt(source.getFileId());
        } catch (NumberFormatException e) {
            throw new StorageManagerException("FileId " + source.getFileId() + " is not an integer", e);
        }

        checkNewFile(studyConfiguration, fileId, fileName);
        studyConfiguration.getFileIds().put(source.getFileName(), fileId);


        /*
         * Before load file, the StudyConfiguration has to be updated with the new sample names.
         *  Will read param SAMPLE_IDS like [<sampleName>:<sampleId>,]*
         *  If SAMPLE_IDS is missing, will auto-generate sampleIds
         *  Will fail if:
         *      param SAMPLE_IDS is malformed
         *      any given sampleId is not an integer
         *      any given sampleName is not in the input file
         *      any given sampleName was already in the StudyConfiguration (so, was already loaded)
         *      some sample was missing in the given SAMPLE_IDS param
         */

        if (options.containsKey(SAMPLE_IDS) && !options.getAsStringList(SAMPLE_IDS).isEmpty()) {
            for (String sampleEntry : options.getAsStringList(SAMPLE_IDS)) {
                String[] split = sampleEntry.split(":");
                if (split.length != 2) {
                    throw new StorageManagerException("Param " + sampleEntry + " is malformed");
                }
                String sampleName = split[0];
                int sampleId;
                try {
                    sampleId = Integer.parseInt(split[1]);
                } catch (NumberFormatException e) {
                    throw new StorageManagerException("SampleId " + split[1] + " is not an integer", e);
                }

                if (!source.getSamplesPosition().containsKey(sampleName)) {
                    //ERROR
                    throw new StorageManagerException("Given sampleName is not in the input file");
                } else {
                    if (!studyConfiguration.getSampleIds().containsKey(sampleName)) {
                        //Add sample to StudyConfiguration
                        studyConfiguration.getSampleIds().put(sampleName, sampleId);
                    } else {
                        if (studyConfiguration.getSampleIds().get(sampleName) == sampleId) {
                            //throw new StorageManagerException("Sample " + sampleName + ":" + sampleId + " was already loaded. It was in the StudyConfiguration");
                            logger.warn("Sample " + sampleName + ":" + sampleId + " was already loaded. It was in the StudyConfiguration");
                        } else {
                            throw new StorageManagerException("Sample " + sampleName + ":" + sampleId + " was already loaded. It was in the StudyConfiguration with a different sampleId: " + studyConfiguration.getSampleIds().get(sampleName));
                        }
                    }
                }
            }

            //Check that all samples has a sampleId
            List<String> missingSamples = new LinkedList<>();
            for (String sampleName : source.getSamples()) {
                if (!studyConfiguration.getSampleIds().containsKey(sampleName)) {
                    missingSamples.add(sampleName);
                }
            }
            if (!missingSamples.isEmpty()) {
                throw new StorageManagerException("Samples " + missingSamples.toString() + " has not assigned sampleId");
            }

        } else {
            //Find the grader sample Id in the studyConfiguration, in order to add more sampleIds if necessary.
            int maxId = 0;
            for (Integer i : studyConfiguration.getSampleIds().values()) {
                if (i > maxId) {
                    maxId = i;
                }
            }
            //Assign new sampleIds
            for (String sample : source.getSamples()) {
                if (!studyConfiguration.getSampleIds().containsKey(sample)) {
                    //If the sample was not in the original studyId, a new SampleId is assigned.

                    int sampleId;
                    int samplesSize = studyConfiguration.getSampleIds().size();
                    Integer samplePosition = source.getSamplesPosition().get(sample);
                    if (!studyConfiguration.getSampleIds().containsValue(samplePosition)) {
                        //1- Use with the SamplePosition
                        sampleId = samplePosition;
                    } else if (!studyConfiguration.getSampleIds().containsValue(samplesSize)) {
                        //2- Use the number of samples in the StudyConfiguration.
                        sampleId = samplesSize;
                    } else {
                        //3- Use the maxId
                        sampleId = maxId + 1;
                    }
                    studyConfiguration.getSampleIds().put(sample, sampleId);
                    if (sampleId > maxId) {
                        maxId = sampleId;
                    }
                }
            }
        }
        options.put(STUDY_CONFIGURATION, studyConfiguration);

        return input;
    }

    @Override
    public URI postLoad(URI input, URI output) throws IOException, StorageManagerException {
        ObjectMap options = configuration.getStorageEngine(storageEngineId).getVariant().getOptions();

        boolean annotate = options.getBoolean(ANNOTATE);
        String annotationSourceStr = options.getString(ANNOTATION_SOURCE, VariantAnnotationManager.AnnotationSource.CELLBASE_REST.name());
        VariantAnnotationManager.AnnotationSource annotationSource = VariantAnnotationManager.AnnotationSource.valueOf(annotationSourceStr);
        Properties annotatorProperties = options.get(ANNOTATOR_PROPERTIES, Properties.class, new Properties());

        //Update StudyConfiguration
        StudyConfiguration studyConfiguration = getStudyConfiguration(options);
        getStudyConfigurationManager(options).updateStudyConfiguration(studyConfiguration, new QueryOptions());

        String dbName = options.getString(DB_NAME, null);
        String species = options.getString(SPECIES, "hsapiens");
        String assembly = options.getString(ASSEMBLY, "");
        int fileId = options.getInt(FILE_ID);

//        VariantSource variantSource = params.get(VARIANT_SOURCE, VariantSource.class);

        if (annotate) {

            VariantAnnotator annotator;
            try {
                annotator = VariantAnnotationManager.buildVariantAnnotator(annotationSource, annotatorProperties, species, assembly);
            } catch (VariantAnnotatorException e) {
                e.printStackTrace();
                logger.error("Can't annotate variants." , e);
                return input;
            }

            VariantAnnotationManager variantAnnotationManager = new VariantAnnotationManager(annotator, getDBAdaptor(dbName));

            QueryOptions annotationOptions = new QueryOptions();
            if (!options.getBoolean(OVERWRITE_ANNOTATIONS, false)) {
                annotationOptions.put(VariantDBAdaptor.ANNOTATION_EXISTS, false);
            }
            annotationOptions.put(VariantDBAdaptor.FILES, Collections.singletonList(fileId));    // annotate just the indexed variants

            annotationOptions.add(VariantAnnotationManager.OUT_DIR, output.getPath());
            annotationOptions.add(VariantAnnotationManager.FILE_NAME, dbName + "." + TimeUtils.getTime());
            variantAnnotationManager.annotate(annotationOptions);
//            URI annotationFile = variantAnnotationManager.createAnnotation(Paths.get(output.getPath()), dbName + "." + TimeUtils.getTime(), annotationOptions);
//            variantAnnotationManager.loadAnnotation(annotationFile, annotationOptions);
        }

        if (options.getBoolean(CALCULATE_STATS)) {
            // TODO add filters
            try {
                logger.debug("about to calculate stats");
                VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
                VariantDBAdaptor dbAdaptor = getDBAdaptor(dbName);
                URI outputUri = output.resolve(buildFilename(studyConfiguration.getStudyId(), fileId) + "." + TimeUtils.getTime());
                URI statsUri = variantStatisticsManager.createStats(dbAdaptor, outputUri, null, studyConfiguration, new QueryOptions(options));
                variantStatisticsManager.loadStats(dbAdaptor, statsUri, studyConfiguration, new QueryOptions(options));
            } catch (Exception e) {
                logger.error("Can't calculate stats." , e);
                e.printStackTrace();
            }
        }

        return input;
    }

    public static String buildFilename(int studyId, int fileId) {
        return studyId + "_" + fileId;
    }

    public static VariantSource readVariantSource(Path input, VariantSource source) throws StorageManagerException {
        if (source == null) {
            source = new VariantSource("", "", "", "");
        }
        if (input.toFile().getName().contains("json")) {
            try {
                VariantJsonReader reader = getVariantJsonReader(input, source);
                reader.open();
                reader.pre();
                reader.post();
                reader.close();
            } catch (IOException e) {
                throw new StorageManagerException("Can not read VariantSource from " + input, e);
            }
        } else {
            VariantReader reader = new VariantVcfReader(source, input.toAbsolutePath().toString());
            reader.open();
            reader.pre();
            source.addMetadata("variantFileHeader", reader.getHeader());
            reader.post();
            reader.close();
        }
        return source;
    }

    protected static VariantJsonReader getVariantJsonReader(Path input, VariantSource source) throws IOException {
        VariantJsonReader variantJsonReader;
        if (    input.toString().endsWith(".json") ||
                input.toString().endsWith(".json.gz") ||
                input.toString().endsWith(".json.snappy") ||
                input.toString().endsWith(".json.snz")) {
            String sourceFile = input.toAbsolutePath().toString().replace("variants.json", "file.json");
            variantJsonReader = new VariantJsonReader(source, input.toAbsolutePath().toString(), sourceFile);
        } else {
            throw new IOException("Variants input file format not supported for file: " + input);
        }
        return variantJsonReader;
    }

    /* --------------------------------------- */
    /*  StudyConfiguration utils methods        */
    /* --------------------------------------- */

    final public StudyConfiguration getStudyConfiguration(ObjectMap params) {
        if (params.containsKey(STUDY_CONFIGURATION)) {
            return params.get(STUDY_CONFIGURATION, StudyConfiguration.class);
        } else {
            StudyConfigurationManager studyConfigurationManager = getStudyConfigurationManager(params);
            return studyConfigurationManager.getStudyConfiguration(params.getInt(STUDY_ID), new QueryOptions(params)).first();
        }
    }

    /**
     * Get the StudyConfigurationManager.
     *
     *  If there is no StudyConfigurationManager, try to build by dependency injection.
     *  If can't build, call to the method "buildStudyConfigurationManager", witch could be override.
     *
     * @param options
     * @return
     */
    final public StudyConfigurationManager getStudyConfigurationManager(ObjectMap options) {
        StudyConfigurationManager studyConfigurationManager = null;
        if (studyConfigurationManager == null) {
            if (options.containsKey(STUDY_CONFIGURATION_MANAGER_CLASS_NAME)) {
                String studyConfigurationManagerClassName = options.getString(STUDY_CONFIGURATION_MANAGER_CLASS_NAME);
                try {
                    studyConfigurationManager = StudyConfigurationManager.build(studyConfigurationManagerClassName, options);
                } catch (ReflectiveOperationException e) {
                    e.printStackTrace();
                    logger.error("Error creating a StudyConfigurationManager. Creating default StudyConfigurationManager", e);
                    throw new RuntimeException(e);
                }
            }
            if (studyConfigurationManager == null) {
                studyConfigurationManager = buildStudyConfigurationManager(options);
            }
        }
        return studyConfigurationManager;
    }

    /**
     * Build the default StudyConfigurationManager. This method could be override by children classes if they want to use other class.
     *
     * @param options
     * @return
     */
    protected StudyConfigurationManager buildStudyConfigurationManager(ObjectMap options) {
        return new FileStudyConfigurationManager(options);
    }

    /**
     * Check if the file(name,id) can be added to the StudyConfiguration.
     * Will fail if:
     *     fileId was already in the studyConfiguration
     *     fileName was already in the studyConfiguration
     */
    protected void checkNewFile(StudyConfiguration studyConfiguration, int fileId, String fileName) throws StorageManagerException {
        if (studyConfiguration.getFileIds().containsKey(fileName)) {
            throw new StorageManagerException("FileName " + fileName + " was already loaded. It was in the StudyConfiguration " +
                    "(" + fileName + ":" + studyConfiguration.getFileIds().get(fileName) + ")");
        }
        if (studyConfiguration.getFileIds().containsKey(fileId)) {
            throw new StorageManagerException("FileId " + fileId + " was already already loaded. It was in the StudyConfiguration" +
                    "(" + StudyConfiguration.inverseMap(studyConfiguration.getFileIds()).get(fileId) + ":" + fileId + ")");
        }
    }

    /**
     * Check if the StudyConfiguration is correct
     * @param studyConfiguration    StudyConfiguration to check
     * @param dbAdaptor             VariantDBAdaptor to the DB containing the indexed study
     */
    public void checkStudyConfiguration(StudyConfiguration studyConfiguration, VariantDBAdaptor dbAdaptor) throws StorageManagerException {
        if (studyConfiguration == null) {
            throw new StorageManagerException("StudyConfiguration is null");
        }
        if (studyConfiguration.getFileIds().size() != StudyConfiguration.inverseMap(studyConfiguration.getFileIds()).size() ) {
            throw new StorageManagerException("StudyConfiguration has duplicated fileIds");
        }
        if (studyConfiguration.getCohortIds().size() != StudyConfiguration.inverseMap(studyConfiguration.getCohortIds()).size() ) {
            throw new StorageManagerException("StudyConfiguration has duplicated cohortIds");
        }
    }

}
