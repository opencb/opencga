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
import org.opencb.opencga.lib.common.TimeUtils;
import org.opencb.opencga.storage.core.StorageManager;
import org.opencb.opencga.storage.core.StorageManagerException;
import org.opencb.opencga.storage.core.StudyConfiguration;
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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by imedina on 13/08/14.
 */
public abstract class VariantStorageManager implements StorageManager<VariantWriter, VariantDBAdaptor> {


    public static final String INCLUDE_STATS = "includeStats";              //Include existing stats on the original file.
    public static final String INCLUDE_SAMPLES = "includeSamples";          //Include sample information (genotypes)
    public static final String INCLUDE_SRC = "includeSrc";                  //Include original source file on the transformed file and the final db
    public static final String COMPRESS_GENOTYPES = "compressGenotypes";    //Stores sample information as compressed genotypes
//    @Deprecated public static final String VARIANT_SOURCE = "variantSource";            //VariantSource object
    public static final String STUDY_CONFIGURATION = "studyConfiguration";      //
    public static final String AGGREGATED_TYPE = "aggregatedType";
    public static final String FILE_ID = "fileId";
    public static final String SAMPLE_IDS = "sampleIds";
    public static final String COMPRESS_METHOD = "compressMethod";

    public static final String CALCULATE_STATS = "calculateStats";          //Calculate stats on the postLoad step
    public static final String OVERWRITE_STATS = "overwriteStats";          //Overwrite stats already present
    public static final String AGGREGATION_MAPPING_PROPERTIES = "aggregationMappingFile";

    public static final String DB_NAME = "dbName";
    public static final String SPECIES = "species";

    public static final String ASSEMBLY = "assembly";
    public static final String ANNOTATE = "annotate";
    public static final String ANNOTATION_SOURCE = "annotationSource";
    public static final String ANNOTATOR_PROPERTIES = "annotatorProperties";
    public static final String OVERWRITE_ANNOTATIONS = "overwriteAnnotations";

    public static final String BATCH_SIZE = "batchSize";
    public static final String TRANSFORM_THREADS = "transformThreads";
    public static final String LOAD_THREADS = "loadThreads";
    public static final String OPENCGA_STORAGE_VARIANT_TRANSFORM_THREADS      = "OPENCGA.STORAGE.VARIANT.TRANSFORM.THREADS";
    public static final String OPENCGA_STORAGE_VARIANT_LOAD_THREADS           = "OPENCGA.STORAGE.VARIANT.LOAD.THREADS";
    public static final String OPENCGA_STORAGE_VARIANT_TRANSFORM_BATCH_SIZE   = "OPENCGA.STORAGE.VARIANT.TRANSFORM.BATCH_SIZE";
    public static final String OPENCGA_STORAGE_VARIANT_INCLUDE_SRC            = "OPENCGA.STORAGE.VARIANT.INCLUDE_SRC";
    public static final String OPENCGA_STORAGE_VARIANT_INCLUDE_SAMPLES        = "OPENCGA.STORAGE.VARIANT.INCLUDE_SAMPLES";
    public static final String OPENCGA_STORAGE_VARIANT_INCLUDE_STATS          = "OPENCGA.STORAGE.VARIANT.INCLUDE_STATS";

    protected Properties properties;
    protected static Logger logger = LoggerFactory.getLogger(VariantStorageManager.class);

    public VariantStorageManager() {
        this.properties = new Properties();
    }

    @Override
    public void addConfigUri(URI configUri){
        if(configUri != null
                && Paths.get(configUri.getPath()).toFile().exists()
                && (configUri.getScheme() == null || configUri.getScheme().equals("file"))) {
            try {
                properties.load(new InputStreamReader(new FileInputStream(configUri.getPath())));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public URI extract(URI from, URI to, ObjectMap params) {
        return from;
    }

    @Override
    public URI preTransform(URI input, ObjectMap params) throws IOException, FileFormatException {
        return input;
    }

    /**
     * Transform raw variant files into biodata model.
     *
     * @param inputUri         Input file. Accepted formats: *.vcf, *.vcf.gz
     * @param pedigreeUri      Pedigree input file. Accepted formats: *.ped
     * @param outputUri
     * @param params
     * @throws IOException
     */
    @Override
    final public URI transform(URI inputUri, URI pedigreeUri, URI outputUri, ObjectMap params) throws IOException {
        // input: VcfReader
        // output: JsonWriter

        Path input = Paths.get(inputUri.getPath());
        Path pedigree = pedigreeUri == null? null : Paths.get(pedigreeUri.getPath());
        Path output = Paths.get(outputUri.getPath());


        boolean includeSamples = params.getBoolean(INCLUDE_SAMPLES, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_SAMPLES, "false")));
        boolean includeStats = params.getBoolean(INCLUDE_STATS, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_STATS, "false")));
        boolean includeSrc = params.getBoolean(INCLUDE_SRC, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_SRC, "false")));

        StudyConfiguration studyConfiguration = params.get(STUDY_CONFIGURATION, StudyConfiguration.class);
        Integer fileId = params.getInt(FILE_ID);    //TODO: Transform into an optional field
        VariantSource.Aggregation aggregation = params.get(AGGREGATED_TYPE, VariantSource.Aggregation.class, VariantSource.Aggregation.NONE);
        String fileName = input.getFileName().toString();
        VariantSource source = new VariantSource(
                fileName,
                fileId.toString(),
                Integer.toString(studyConfiguration.getStudyId()),
                studyConfiguration.getStudyName(), null, aggregation);

        int batchSize = params.getInt(BATCH_SIZE, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_VARIANT_TRANSFORM_BATCH_SIZE, "100")));
        String compression = params.getString(COMPRESS_METHOD, "snappy");
        String extension = "";
        int numThreads = params.getInt(VariantStorageManager.TRANSFORM_THREADS, 8);
        int capacity = params.getInt("blockingQueueCapacity", numThreads*2);

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
                    factory = new VariantVcfEVSFactory(params.get(AGGREGATION_MAPPING_PROPERTIES, Properties.class, null));
                    break;
            }
        } else {
            throw new IOException("Variants input file format not supported");
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
            vr.run();
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
    public URI postTransform(URI input, ObjectMap params) throws IOException, FileFormatException {
        return input;
    }

    @Override
    public URI preLoad(URI input, URI output, ObjectMap params) throws IOException {
        StudyConfiguration studyConfiguration = params.get(STUDY_CONFIGURATION, StudyConfiguration.class);
        VariantSource source = readVariantSource(Paths.get(input.getPath()), null);

        /*
         * Before load file, add to the StudyConfiguration.
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
            throw new IOException("FileId " + source.getFileId() + " is not an integer", e);
        }

        if (studyConfiguration.getFileIds().containsKey(fileName)) {
            throw new IOException("FileName " + fileName + " was already in the StudyConfiguration " +
                    "(" + fileName + ":" + studyConfiguration.getFileIds().get(fileName) + ")");
        }
        if (studyConfiguration.getFileIds().containsKey(fileId)) {
            throw new IOException("FileId " + fileId + " was already in the StudyConfiguration" +
                    "(" + StudyConfiguration.inverseMap(studyConfiguration.getFileIds()).get(fileId) + ":" + fileId + ")");
        }
        studyConfiguration.getFileIds().put(fileName, fileId);


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

        if (params.containsKey(SAMPLE_IDS) && !params.getAsStringList(SAMPLE_IDS).isEmpty()) {
            for (String sampleEntry : params.getAsStringList(SAMPLE_IDS)) {
                String[] split = sampleEntry.split(":");
                if (split.length != 2) {
                    throw new IOException("Param " + sampleEntry + " is malformed");
                }
                String sampleName = split[0];
                int sampleId;
                try {
                    sampleId = Integer.getInteger(split[1]);
                } catch (NumberFormatException e) {
                    throw new IOException("SampleId " + split[1] + " is not an integer", e);
                }

                if (!source.getSamplesPosition().containsKey(sampleName)) {
                    //ERROR
                    throw new IOException("Given sampleName is not in the input file");
                } else {
                    if (!studyConfiguration.getSampleIds().containsKey(sampleName)) {
                        //Add sample to StudyConfiguration
                        studyConfiguration.getSampleIds().put(sampleName, sampleId);
                    } else {
                        if (studyConfiguration.getSampleIds().get(sampleName) == sampleId) {
                            throw new IOException("Sample " + sampleName + ":" + sampleId + " was already in the StudyConfiguration");
                        } else {
                            throw new IOException("Sample " + sampleName + ":" + sampleId + " was already in the StudyConfiguration with a different sampleId: " + studyConfiguration.getSampleIds().get(sampleName));
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
                throw new IOException("Samples " + missingSamples.toString() + " has not assigned sampleId");
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

        return input;
    }

    @Override
    public URI postLoad(URI input, URI output, ObjectMap params) throws IOException, StorageManagerException {
        boolean annotate = params.getBoolean(ANNOTATE);
        VariantAnnotationManager.AnnotationSource annotationSource = params.get(ANNOTATION_SOURCE, VariantAnnotationManager.AnnotationSource.class);
        Properties annotatorProperties = params.get(ANNOTATOR_PROPERTIES, Properties.class);

        String dbName = params.getString(DB_NAME, null);
        String species = params.getString(SPECIES, "hsapiens");
        String assembly = params.getString(ASSEMBLY, "");
        int fileId = params.getInt(FILE_ID);
        StudyConfiguration studyConfiguration = params.get(STUDY_CONFIGURATION, StudyConfiguration.class);
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

            VariantAnnotationManager variantAnnotationManager = new VariantAnnotationManager(annotator, getDBAdaptor(dbName, params));

            QueryOptions annotationOptions = new QueryOptions();
            if (!params.getBoolean(OVERWRITE_ANNOTATIONS, false)) {
                annotationOptions.put(VariantDBAdaptor.ANNOTATION_EXISTS, false);
            }
            annotationOptions.put(VariantDBAdaptor.FILES, Collections.singletonList(fileId));    // annotate just the indexed variants

            annotationOptions.add(VariantAnnotationManager.OUT_DIR, output.getPath());
            annotationOptions.add(VariantAnnotationManager.FILE_NAME, dbName + "." + TimeUtils.getTime());
            variantAnnotationManager.annotate(annotationOptions);
//            URI annotationFile = variantAnnotationManager.createAnnotation(Paths.get(output.getPath()), dbName + "." + TimeUtils.getTime(), annotationOptions);
//            variantAnnotationManager.loadAnnotation(annotationFile, annotationOptions);
        }

        if (params.getBoolean(CALCULATE_STATS)) {
            // TODO add filters
            logger.debug("about to calculate stats");
            VariantStatisticsManager variantStatisticsManager = new VariantStatisticsManager();
            URI statsUri = variantStatisticsManager.createStats(getDBAdaptor(dbName, params), output.resolve(buildFilename(studyConfiguration.getStudyId(), fileId) + "." + TimeUtils.getTime()), null, new QueryOptions(params));
            variantStatisticsManager.loadStats(getDBAdaptor(dbName, params), statsUri, new QueryOptions(params));
        }

        return input;
    }

    public static String buildFilename(int studyId, int fileId) {
        return studyId + "_" + fileId;
    }

    public static VariantSource readVariantSource(Path input, VariantSource source) throws IOException {
        if (source == null) {
            source = new VariantSource("", "", "", "");
        }
        if (input.toFile().getName().contains("json")) {
            VariantJsonReader reader = getVariantJsonReader(input, source);
            reader.open();
            reader.pre();
            reader.post();
            reader.close();
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


}
