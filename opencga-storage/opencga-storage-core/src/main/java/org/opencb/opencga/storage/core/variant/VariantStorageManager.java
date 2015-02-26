package org.opencb.opencga.storage.core.variant;

import org.opencb.biodata.formats.io.FileFormatException;
import org.opencb.biodata.formats.variant.io.VariantWriter;
import org.opencb.biodata.formats.variant.vcf4.io.VariantVcfReader;
import org.opencb.biodata.models.variant.*;
import org.opencb.commons.containers.list.SortedList;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.Task;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.lib.common.TimeUtils;
import org.opencb.opencga.storage.core.*;
import org.opencb.opencga.storage.core.runner.SimpleThreadRunner;
import org.opencb.opencga.storage.core.runner.StringDataReader;
import org.opencb.opencga.storage.core.runner.StringDataWriter;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.json.*;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsCalculator;
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
    public static final String CALCULATE_STATS = "calculateStats";          //Calculate stats on the postLoad step
    public static final String VARIANT_SOURCE = "variantSource";            //VariantSource object
    public static final String DB_NAME = "dbName";

    public static final String SPECIES = "species";
    public static final String ASSEMBLY = "assembly";

    public static final String ANNOTATE = "annotate";
    public static final String ANNOTATION_SOURCE = "annotationSource";
    public static final String ANNOTATOR_PROPERTIES = "annotatorProperties";
    public static final String OVERWRITE_ANNOTATIONS = "overwriteAnnotations";
    public static final String BATCH_SIZE = "batchSize";

    public static final String OPENCGA_STORAGE_VARIANT_TRANSFORM_BATCH_SIZE   = "OPENCGA.STORAGE.VARIANT.TRANSFORM.BATCH_SIZE";
    public static final String OPENCGA_STORAGE_VARIANT_INCLUDE_SRC            = "OPENCGA.STORAGE.VARIANT.INCLUDE_SRC";
    public static final String OPENCGA_STORAGE_VARIANT_INCLUDE_SAMPLES        = "OPENCGA.STORAGE.VARIANT.INCLUDE_SAMPLES";
    public static final String OPENCGA_STORAGE_VARIANT_INCLUDE_STATS          = "OPENCGA.STORAGE.VARIANT.INCLUDE_STATS";

    protected Properties properties;
    protected static Logger logger = LoggerFactory.getLogger(VariantStorageManager.class);


    public VariantStorageManager() {
        this.properties = new Properties();
    }

    //@Override
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
//        boolean includeEffect = params.getBoolean(INCLUDE_EFFECT, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_EFFECT, "false")));
        boolean includeStats = params.getBoolean(INCLUDE_STATS, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_STATS, "false")));
        boolean includeSrc = params.getBoolean(INCLUDE_SRC, Boolean.parseBoolean(properties.getProperty(OPENCGA_STORAGE_VARIANT_INCLUDE_SRC, "false")));

        VariantSource source = params.get(VARIANT_SOURCE, VariantSource.class);
        //VariantSource source = new VariantSource(input.getFileName().toString(), params.get("fileId").toString(), params.get("studyId").toString(), params.get("study").toString());

        int batchSize = params.getInt(BATCH_SIZE, Integer.parseInt(properties.getProperty(OPENCGA_STORAGE_VARIANT_TRANSFORM_BATCH_SIZE, "100")));
        String extension = params.getString("compressExtension", "snappy");
        int numTasks = params.getInt("transformThreads", 8);
        int capacity = params.getInt("blockingQueueCapacity", numTasks*2);

        if (!extension.startsWith(".") && !extension.isEmpty()) {
            extension = "." + extension;
        }
        //Reader
//        VariantReader reader = null;
//        PedigreeReader pedReader = null;
//        if(pedigree != null && pedigree.toFile().exists()) {    //FIXME Add "endsWith(".ped") ??
//            pedReader = new PedigreePedReader(pedigree.toString());
//        }

        // TODO Create a utility to determine which extensions are variants files
        final VariantVcfFactory factory;
        if (source.getFileName().endsWith(".vcf") || source.getFileName().endsWith(".vcf.gz")) {
            switch (source.getAggregation()) {
                default:
                case NONE:
                    factory = new VariantVcfFactory();
                    break;
                case BASIC:
                    factory = new VariantAggregatedVcfFactory();
                    break;
                case EVS:
                    factory = new VariantVcfEVSFactory();
                    break;
            }
        } else {
            throw new IOException("Variants input file format not supported");
        }
        source = readVariantSource(input, source);
        //Tasks
        List<Task<Variant>> taskList = new SortedList<>();

//        todo remove:
//        if (includeStats) {
//            taskList.add(new VariantStatsTask(reader, source));
//        }

        //Writers
        List<VariantWriter> writers = new ArrayList<>();
        VariantJsonWriter jsonWriter = new VariantJsonWriter(source, output);
        jsonWriter.includeSrc(includeSrc);
        writers.add(jsonWriter);

        for (VariantWriter variantWriter : writers) {
            variantWriter.includeSamples(includeSamples);
//            variantWriter.includeEffect(includeEffect);   //Deprecated
            variantWriter.includeStats(includeStats);
        }

        //Runner
//        VariantRunner vr = new VariantRunner(source, reader, pedReader, writers, taskList, batchSize);
//
//        logger.info("Transforming variants...");
//        long start = System.currentTimeMillis();
//        vr.run();
//        long end = System.currentTimeMillis();
//        logger.info("end - start = " + (end - start) / 1000.0 + "s");
//        logger.info("Variants transformed!");



        Path outputVariantJsonFile = output.resolve(input.getFileName().toString() + ".variants.json" + extension);
        Path outputFileJsonFile = output.resolve(input.getFileName().toString() + ".files.json" + extension);

        StringDataReader dataReader = new StringDataReader(input);
        StringDataWriter dataWriter = new StringDataWriter(outputVariantJsonFile);



        logger.info("Transforming variants...");
        long start = System.currentTimeMillis();
        SimpleThreadRunner runner = new SimpleThreadRunner(
                dataReader,
                Collections.<Task>singletonList(new TransformVariantTask(factory, source, outputFileJsonFile)),
                dataWriter,
                batchSize,
                capacity,
                numTasks);
        runner.run();
        long end = System.currentTimeMillis();
        logger.info("end - start = " + (end - start) / 1000.0 + "s");
        logger.info("Variants transformed!");

        return outputUri.resolve(outputVariantJsonFile.getFileName().toString());
    }

    private VariantSource readVariantSource(Path input, VariantSource source) {
        DataReader reader = new VariantVcfReader(source, input.toAbsolutePath().toString());
        reader.open();
        reader.pre();
        reader.post();
        reader.close();
        return source;
    }

    @Override
    public URI postTransform(URI input, ObjectMap params) throws IOException, FileFormatException {
        return input;
    }

    protected VariantJsonReader getVariantJsonReader(Path input, VariantSource source) throws IOException {
        VariantJsonReader variantJsonReader;
        if (source.getFileName().endsWith(".json") || source.getFileName().endsWith(".json.gz")) {
            String sourceFile = input.toAbsolutePath().toString().replace("variants.json", "file.json");
            variantJsonReader = new VariantJsonReader(source, input.toAbsolutePath().toString(), sourceFile);
        } else {
            throw new IOException("Variants input file format not supported");
        }
        return variantJsonReader;
    }

    @Override
    public URI postLoad(URI input, URI output, ObjectMap params) throws IOException {
        boolean annotate = params.getBoolean(ANNOTATE);
        VariantAnnotationManager.AnnotationSource annotationSource = params.get(ANNOTATION_SOURCE, VariantAnnotationManager.AnnotationSource.class);
        Properties annotatorProperties = params.get(ANNOTATOR_PROPERTIES, Properties.class);

        if (annotate) {
            String dbName = params.getString(DB_NAME, null);
            String species = params.getString(SPECIES, "hsapiens");
            String assembly = params.getString(ASSEMBLY, "");
            VariantSource variantSource = params.get(VARIANT_SOURCE, VariantSource.class);

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
            annotationOptions.put(VariantDBAdaptor.FILES, Collections.singletonList(variantSource.getFileId()));    // annotate just the indexed variants

            annotationOptions.add(VariantAnnotationManager.OUT_DIR, output.getPath());
            annotationOptions.add(VariantAnnotationManager.FILE_NAME, dbName + "." + TimeUtils.getTime());
            variantAnnotationManager.annotate(annotationOptions);
//            URI annotationFile = variantAnnotationManager.createAnnotation(Paths.get(output.getPath()), dbName + "." + TimeUtils.getTime(), annotationOptions);
//            variantAnnotationManager.loadAnnotation(annotationFile, annotationOptions);
        }

        if (params.getBoolean(CALCULATE_STATS)) {
            // TODO add filters
            logger.debug("about to calculate stats");
            String dbName = params.getString(DB_NAME, "defaultDatabase");
            VariantStatisticsCalculator variantStatisticsCalculator = new VariantStatisticsCalculator();
            URI statsUri = variantStatisticsCalculator.createStats(getDBAdaptor(dbName, params), output.resolve(dbName + "." + TimeUtils.getTime()), new QueryOptions(params));
            variantStatisticsCalculator.loadStats(getDBAdaptor(dbName, params), statsUri, new QueryOptions(params));
        }

        return input;
    }

//    @Override
//    public void preLoad(URI inputUri, URI outputUri, ObjectMap params) throws IOException {
//        // input: JsonVariatnReader
//        // output: getDBSchemaWriter
//
//        Path input = Paths.get(inputUri);
//        Path output = Paths.get(outputUri);
//
//        //Writers
//        VariantWriter dbSchemaWriter = this.getDBSchemaWriter(outputUri);
//        if(dbSchemaWriter == null){
//            System.out.println("[ALERT] preLoad method not supported in this plugin");
//            return;
//        }
//        List<VariantWriter> writers = Arrays.asList(dbSchemaWriter);
//
//        //VariantSource source = new VariantSource(input.getFileName().toString(), params.get("fileId").toString(), params.get("studyId").toString(), params.get("study").toString());
//        VariantSource source = (VariantSource) params.get("source");
//
//        //Reader
//        String sourceFile = input.toAbsolutePath().toString().replace("variants.json", "file.json");
//        VariantReader jsonReader = new VariantJsonReader(source, input.toAbsolutePath().toString() , sourceFile);
//
//
//        //Tasks
//        List<Task<Variant>> taskList = new SortedList<>();
//
//
//        //Runner
//        VariantRunner vr = new VariantRunner(source, jsonReader, null, writers, taskList);
//
//        logger.info("Preloading variants...");
//        long start = System.currentTimeMillis();
//        vr.run();
//        long end = System.currentTimeMillis();
//        logger.info("end - start = " + (end - start) / 1000.0 + "s");
//        logger.info("Variants preloaded!");
//
//    }



}
