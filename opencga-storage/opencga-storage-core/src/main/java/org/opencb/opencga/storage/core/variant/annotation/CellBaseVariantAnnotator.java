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

package org.opencb.opencga.storage.core.variant.annotation;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.biodata.models.variation.GenomicVariant;
import org.opencb.cellbase.core.client.CellBaseClient;
import org.opencb.cellbase.core.lib.DBAdaptorFactory;
import org.opencb.cellbase.core.lib.api.variation.VariantAnnotationDBAdaptor;
import org.opencb.cellbase.core.lib.api.variation.VariationDBAdaptor;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResponse;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.config.CellBaseConfiguration;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.VariantAnnotationMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jacobo on 9/01/15.
 */
public class CellBaseVariantAnnotator implements VariantAnnotator {


    private final JsonFactory factory;
    private VariantAnnotationDBAdaptor variantAnnotationDBAdaptor;
    private CellBaseClient cellBaseClient;
    private ObjectMapper jsonObjectMapper;
    private VariationDBAdaptor variationDBAdaptor;
    private DBAdaptorFactory dbAdaptorFactory;

//    public static final String CELLBASE_VERSION = "CELLBASE.VERSION";
//    public static final String CELLBASE_REST_URL = "CELLBASE.REST.URL";
//
//    public static final String CELLBASE_DB_HOST = "CELLBASE.DB.HOST";
//    public static final String CELLBASE_DB_NAME = "CELLBASE.DB.NAME";
//    public static final String CELLBASE_DB_PORT = "CELLBASE.DB.PORT";
//    public static final String CELLBASE_DB_USER = "CELLBASE.DB.USER";
//    public static final String CELLBASE_DB_PASSWORD = "CELLBASE.DB.PASSWORD";
//    public static final String CELLBASE_DB_MAX_POOL_SIZE = "CELLBASE.DB.MAX_POOL_SIZE";
//    public static final String CELLBASE_DB_TIMEOUT = "CELLBASE.DB.TIMEOUT";



    protected static Logger logger = LoggerFactory.getLogger(CellBaseVariantAnnotator.class);

    public CellBaseVariantAnnotator() {
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(factory);
        this.dbAdaptorFactory = null;
        this.cellBaseClient = null;
        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
    }

//    public CellBaseVariantAnnotator(CellBaseConfiguration cellbaseConfiguration, String cellbaseSpecies, String cellbaseAssembly) {
//        this();
//        /**
//         * Connecting to CellBase database
//         */
//        dbAdaptorFactory = new MongoDBAdaptorFactory(cellbaseConfiguration);
//        variantAnnotationDBAdaptor = dbAdaptorFactory.getVariantAnnotationDBAdaptor(cellbaseSpecies, cellbaseAssembly);
//        variationDBAdaptor = dbAdaptorFactory.getVariationDBAdaptor(cellbaseSpecies, cellbaseAssembly);
//    }

    public CellBaseVariantAnnotator(CellBaseClient cellBaseClient) {
        this();
        if(cellBaseClient == null) {
            throw new NullPointerException("CellBaseClient can not be null");
        }
        this.cellBaseClient = cellBaseClient;
        cellBaseClient.getObjectMapper().addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
    }

    public static CellBaseVariantAnnotator buildCellbaseAnnotator(CellBaseConfiguration cellBaseConfiguration, String species, String assembly, boolean restConnection)
            throws VariantAnnotatorException {
        if (restConnection) {
            String cellbaseVersion = cellBaseConfiguration.getVersion();
            List<String> hosts = cellBaseConfiguration.getHosts();
            if (hosts.isEmpty()) {
                throw new VariantAnnotatorException("Missing defaultValue \"CellBase Hosts\"");
            }
            String cellbaseRest = hosts.get(0);

            checkNotNull(cellbaseVersion, "cellbase version");
            checkNotNull(cellbaseRest, "cellbase hosts");
            checkNotNull(species, "species");

            CellBaseClient cellBaseClient;
            try {
                URI url = new URI(cellbaseRest);
                cellBaseClient = new CellBaseClient(url, cellbaseVersion, species);
            } catch (URISyntaxException e) {
                e.printStackTrace();
                throw new VariantAnnotatorException("Invalid URL : " + cellbaseRest, e);
            }
            return new CellBaseVariantAnnotator(cellBaseClient);
        } else {
            throw new UnsupportedOperationException("Unimplemented CellBase dbAdaptor connection. Use CellBaseClient instead");
//            String cellbaseHost = annotatorProperties.getProperty(CELLBASE_DB_HOST, "");
//            String cellbaseDatabase = annotatorProperties.getProperty(CELLBASE_DB_NAME, "");
//            int cellbasePort = Integer.parseInt(annotatorProperties.getProperty(CELLBASE_DB_PORT, "27017"));
//            String cellbaseUser = annotatorProperties.getProperty(CELLBASE_DB_USER, "");
//            String cellbasePassword = annotatorProperties.getProperty(CELLBASE_DB_PASSWORD, "");
//            int maxPoolSize = Integer.parseInt(annotatorProperties.getProperty(CELLBASE_DB_MAX_POOL_SIZE, "10"));
//            int timeout = Integer.parseInt(annotatorProperties.getProperty(CELLBASE_DB_TIMEOUT, "200"));
//
//            checkNotNull(cellbaseHost, CELLBASE_DB_HOST);
//            checkNotNull(cellbaseDatabase, CELLBASE_DB_NAME);
//            checkNotNull(cellbaseUser, CELLBASE_DB_USER);
//            checkNotNull(cellbasePassword, CELLBASE_DB_PASSWORD);
//
//
//            CellBaseConfiguration cellbaseConfiguration = new CellBaseConfiguration();
//            cellbaseConfiguration.addSpeciesConnection(
//                    species,
//                    assembly,
//                    cellbaseHost,
//                    cellbaseDatabase,
//                    cellbasePort,
//                    "mongo",    //TODO: Change to "mongodb"
//                    cellbaseUser,
//                    cellbasePassword,
//                    maxPoolSize,
//                    timeout);
//            cellbaseConfiguration.addSpeciesAlias(species, species);
//
//            return new CellBaseVariantAnnotator(cellbaseConfiguration, species, assembly);
        }
    }

    private static void checkNotNull(String value, String name) throws VariantAnnotatorException {
        if(value == null || value.isEmpty()) {
            throw new VariantAnnotatorException("Missing defaultValue: " + name);
        }
    }

    /////// CREATE ANNOTATION

    @Override
    public URI createAnnotation(VariantDBAdaptor variantDBAdaptor, Path outDir, String fileName, QueryOptions options)
            throws IOException {
        if(cellBaseClient == null && dbAdaptorFactory == null) {
            throw new IllegalStateException("Cant createAnnotation without a CellBase source (DBAdaptorFactory or a CellBaseClient)");
        }

        boolean gzip = options == null || options.getBoolean("gzip", true);
        Path path = Paths.get(outDir != null? outDir.toString() : "/tmp" ,fileName + ".annot.json" + (gzip? ".gz" : ""));
        URI fileUri = path.toUri();

        /** Open output stream **/
        final OutputStream outputStream;
        if(gzip) {
            outputStream = new GZIPOutputStream(new FileOutputStream(path.toFile()));
        } else {
            outputStream = new FileOutputStream(path.toFile());
        }

        /** Initialize Json serializer**/
        ObjectWriter writer = jsonObjectMapper.writerFor(VariantAnnotation.class);

        /** Getting iterator from OpenCGA Variant database. **/
        QueryOptions iteratorQueryOptions;
        if(options == null) {
            iteratorQueryOptions = new QueryOptions();
        } else {
            iteratorQueryOptions = new QueryOptions(options);
        }
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference");
        iteratorQueryOptions.add("include", include);

        int batchSize = 200;
        int numThreads = 8;
        if(options != null) { //Parse query options
            batchSize = options.getInt(VariantAnnotationManager.BATCH_SIZE, batchSize);
            numThreads = options.getInt(VariantAnnotationManager.NUM_THREADS, numThreads);
        }


        Iterator<Variant> iterator = variantDBAdaptor.iterator(iteratorQueryOptions);

        try {
            final int[] readsCounter = {0};
            DataReader<GenomicVariant> genomicVariantDataReader = readBatchSize -> {
                List<GenomicVariant> genomicVariantList = new ArrayList<>(readBatchSize);
                int i = 0;
                while(iterator.hasNext() && i++ < readBatchSize) {
                    Variant variant = iterator.next();
                    readsCounter[0]++;
                    if (readsCounter[0] % 1000 == 0) {
                        logger.info("Element {}", readsCounter[0]);
                    }

                    // If Variant is SV some work is needed
                    if(variant.getAlternate().length() + variant.getReference().length() > Variant.SV_THRESHOLD*2) {       //TODO: Manage SV variants
//                logger.info("Skip variant! {}", genomicVariant);
                        logger.info("Skip variant! {}", variant.getChromosome() + ":" +
                                        variant.getStart() + ":" +
                                        (variant.getReference().length() > 10? variant.getReference().substring(0,10) + "...[" + variant.getReference().length() + "]" : variant.getReference()) + ":" +
                                        (variant.getAlternate().length() > 10? variant.getAlternate().substring(0,10) + "...[" + variant.getAlternate().length() + "]" : variant.getAlternate())
                        );
                        logger.debug("Skip variant! {}", variant);
                    } else {
                        GenomicVariant genomicVariant = new GenomicVariant(variant.getChromosome(), variant.getStart(),
                                variant.getReference().isEmpty() && variant.getType() == Variant.VariantType.INDEL ? "-" : variant.getReference(),
                                variant.getAlternate().isEmpty() && variant.getType() == Variant.VariantType.INDEL ? "-" : variant.getAlternate());
                        genomicVariantList.add(genomicVariant);
                    }
                }

                return genomicVariantList;
            };

            ParallelTaskRunner.Task<GenomicVariant,VariantAnnotation> annotationTask = genomicVariantList -> {
                List<VariantAnnotation> variantAnnotationList;
                try {
                    if(cellBaseClient != null) {
                        variantAnnotationList = getVariantAnnotationsREST(genomicVariantList);
                    } else {
                        variantAnnotationList = getVariantAnnotationsDbAdaptor(genomicVariantList);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                return variantAnnotationList;
            };

            DataWriter<VariantAnnotation> variantAnnotationDataWriter = variantAnnotationList -> {
                try {
                    for (VariantAnnotation variantAnnotation : variantAnnotationList) {
                        outputStream.write(writer.writeValueAsString(variantAnnotation).getBytes());
                        outputStream.write('\n');
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
                return true;
            };

            ParallelTaskRunner.Config config = new ParallelTaskRunner.Config(numThreads, batchSize, numThreads * 2, true, false);
            ParallelTaskRunner<GenomicVariant, VariantAnnotation> parallelTaskRunner = new ParallelTaskRunner<>(genomicVariantDataReader, annotationTask, variantAnnotationDataWriter, config);
            parallelTaskRunner.run();
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            outputStream.close();
        }

        return fileUri;
    }

    /////// CREATE ANNOTATION - AUX METHODS

    private List<VariantAnnotation> getVariantAnnotationsREST(List<GenomicVariant> genomicVariantList) throws IOException {
        QueryResponse<QueryResult<VariantAnnotation>> queryResponse;
        List<String> genomicVariantStringList = new ArrayList<>(genomicVariantList.size());
        for (GenomicVariant genomicVariant : genomicVariantList) {
            genomicVariantStringList.add(genomicVariant.toString());
        }

        boolean queryError = false;
        try {
            queryResponse = cellBaseClient.get(
                    CellBaseClient.Category.genomic,
                    CellBaseClient.SubCategory.variant,
                    genomicVariantStringList,
                    CellBaseClient.Resource.fullAnnotation,
                    new QueryOptions("post", true));
            if (queryResponse == null) {
                logger.warn("CellBase REST fail. Returned null. {}", cellBaseClient.getLastQuery());
                queryError = true;
            }
        } catch (JsonProcessingException e ) {
            logger.warn("CellBase REST fail. Error parsing " + cellBaseClient.getLastQuery(), e);
            queryError = true;
            queryResponse = null;
        }

        if(queryResponse != null && queryResponse.getResponse().size() != genomicVariantList.size()) {
            logger.warn("QueryResult size (" + queryResponse.getResponse().size() + ") != genomicVariantList size (" + genomicVariantList.size() + ").");
            //throw new IOException("QueryResult size != " + genomicVariantList.size() + ". " + queryResponse);
            queryError = true;
        }

        if(queryError) {
//            logger.warn("CellBase REST error. {}", cellBaseClient.getLastQuery());

            if (genomicVariantList.size() == 1) {
                logger.error("CellBase REST error. Skipping variant. {}", genomicVariantList.get(0));
                return Collections.emptyList();
            }

            List<VariantAnnotation> variantAnnotationList = new LinkedList<>();
            List<GenomicVariant> genomicVariants1 = genomicVariantList.subList(0, genomicVariantList.size() / 2);
            if (!genomicVariants1.isEmpty()) {
                variantAnnotationList.addAll(getVariantAnnotationsREST(genomicVariants1));
            }
            List<GenomicVariant> genomicVariants2 = genomicVariantList.subList(genomicVariantList.size() / 2, genomicVariantList.size());
            if (!genomicVariants2.isEmpty()) {
                variantAnnotationList.addAll(getVariantAnnotationsREST(genomicVariants2));
            }
            return variantAnnotationList;
        }

        Collection<QueryResult<VariantAnnotation>> response = queryResponse.getResponse();

        QueryResult<VariantAnnotation>[] queryResults = response.toArray(new QueryResult[1]);
        List<VariantAnnotation> variantAnnotationList = new ArrayList<>(genomicVariantList.size());
        for (QueryResult<VariantAnnotation> queryResult : queryResults) {
            variantAnnotationList.addAll(queryResult.getResult());
        }
        return variantAnnotationList;
    }

    private List<VariantAnnotation> getVariantAnnotationsDbAdaptor(List<GenomicVariant> genomicVariantList) throws IOException {
//        QueryOptions queryOptions = new QueryOptions();
//
//        List<VariantAnnotation> variantAnnotationList = new ArrayList<>(genomicVariantList.size());
//        Map<String, List<ConsequenceType>> consequenceTypes = getConsequenceTypes(genomicVariantList, queryOptions);
//        Map<String, String> variantIds = getVariantId(genomicVariantList, queryOptions);
//        for (GenomicVariant genomicVariant : genomicVariantList) {
//            VariantAnnotation variantAnnotation = new VariantAnnotation(
//                    genomicVariant.getChromosome(),
//                    genomicVariant.getPosition(),
//                    genomicVariant.getPosition(),   //TODO: ¿?¿?
//                    genomicVariant.getReference(),
//                    genomicVariant.getAlternative());
//
//            String key = genomicVariant.toString();
//            variantAnnotation.setConsequenceTypes(consequenceTypes.get(key));
//            variantAnnotation.setId(variantIds.get(key));
//
//            variantAnnotationList.add(variantAnnotation);
//        }
//        return variantAnnotationList;
        throw new UnsupportedOperationException("Unsupported operation. Try with REST annotation");
    }
//
//    // FIXME To delete when available in cellbase
//    private Map<String, List<ConsequenceType>> getConsequenceTypes(List<GenomicVariant> genomicVariants,
//                                                                   QueryOptions queryOptions) throws IOException {
//        Map<String, List<ConsequenceType>> map = new HashMap<>(genomicVariants.size());
//        List<QueryResult> queryResultList = variantAnnotationDBAdaptor.getAllConsequenceTypesByVariantList(genomicVariants, queryOptions);
//        for (QueryResult queryResult : queryResultList) {
//            Object result = queryResult.getResult();
//            List list = result instanceof Collection ? new ArrayList((Collection) result) : Collections.singletonList(result);
//
//            if(list.get(0) instanceof ConsequenceType) {
//                map.put(queryResult.getId(), list);
//            } else {
//                throw new IOException("queryResult result : " + queryResult + " is not a ConsequenceType");
//            }
//        }
//        return map;
//    }
//
//    // FIXME To delete when available in cellbase
//    private Map<String, String> getVariantId(List<GenomicVariant> genomicVariant, QueryOptions queryOptions) throws IOException {
//        List<QueryResult> variationQueryResultList = variationDBAdaptor.getIdByVariantList(genomicVariant, queryOptions);
//        Map<String, String> map = new HashMap<>(genomicVariant.size());
//        for (QueryResult queryResult : variationQueryResultList) {
//            map.put(queryResult.getId(), queryResult.getResult().toString());
//        }
//        return map;
//    }
//

    /////// LOAD ANNOTATION

    @Override
    public void loadAnnotation(final VariantDBAdaptor variantDBAdaptor, final URI uri, QueryOptions options) throws IOException {

        final int batchSize = options.getInt(VariantAnnotationManager.BATCH_SIZE, 100);
        final int numConsumers = options.getInt(VariantAnnotationManager.NUM_WRITERS, 6);
        final int numProducers = 1;
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers + numProducers);
        final BlockingQueue<VariantAnnotation> queue = new ArrayBlockingQueue<>(batchSize*numConsumers*2);
        final VariantAnnotation lastElement = new VariantAnnotation();

        executor.execute(new Runnable() {   // producer
            @Override
            public void run() {
                try {
                    int readsCounter = 0;

                    /** Open input stream **/
                    InputStream inputStream;
                    inputStream = new FileInputStream(Paths.get(uri).toFile());
                    inputStream = new GZIPInputStream(inputStream);

                    /** Innitialice Json parse**/
                    JsonParser parser = factory.createParser(inputStream);

                    while (parser.nextToken() != null) {
                        VariantAnnotation variantAnnotation = parser.readValueAs(VariantAnnotation.class);
                        queue.put(variantAnnotation);
                        readsCounter++;
                        if (readsCounter % 1000 == 0) {
                            logger.info("Element {}", readsCounter);
                        }
                    }
                    for (int i = 0; i < numConsumers; i++) {    //Add a lastElement marker. Consumers will stop reading when read this element.
                        queue.put(lastElement);
                    }
                    logger.debug("Put Last element. queue size = {}", queue.size());
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        for (int i = 0; i < numConsumers; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        List<VariantAnnotation> batch = new ArrayList<>(batchSize);
                        VariantAnnotation elem = queue.take();
                        while (elem != lastElement) {
                            batch.add(elem);
                            if (batch.size() == batchSize) {
                                variantDBAdaptor.updateAnnotations(batch, new QueryOptions());
                                batch.clear();
                                logger.debug("thread updated batch");
                            }
                            elem = queue.take();
                        }
                        if (!batch.isEmpty()) { //Upload remaining elements
                            variantDBAdaptor.updateAnnotations(batch, new QueryOptions());
                        }
                        logger.debug("thread finished updating annotations");
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("annotation interrupted");
            e.printStackTrace();
        }

        /** Join
        try {
            producerThread.join();
            for (Thread consumerThread : consumers) {
                consumerThread.join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        **/
//        while (parser.nextToken() != null) {
//            VariantAnnotation variantAnnotation = parser.readValueAs(VariantAnnotation.class);
////            System.out.println("variantAnnotation = " + variantAnnotation);
//            batch.add(variantAnnotation);
//            if(batch.size() == batchSize || parser.nextToken() == null) {
//                variantDBAdaptor.updateAnnotations(batch, new QueryOptions());
//                batch.clear();
//            }
//        }

    }
}
