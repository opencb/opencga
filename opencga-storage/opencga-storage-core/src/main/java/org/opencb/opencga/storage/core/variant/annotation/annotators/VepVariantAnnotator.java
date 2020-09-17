/*
 * Copyright 2015-2017 OpenCB
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

package org.opencb.opencga.storage.core.variant.annotation.annotators;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opencb.biodata.formats.variant.annotation.io.VepFormatReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.models.ProjectMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotatorException;
import org.opencb.opencga.storage.core.variant.io.json.mixin.VariantAnnotationMixin;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by fjlopez on 10/04/15.
 */
public class VepVariantAnnotator extends VariantAnnotator {
    private final JsonFactory factory;
    private ObjectMapper jsonObjectMapper;

    protected static Logger logger = LogManager.getLogger(AbstractCellBaseVariantAnnotator.class);

    public VepVariantAnnotator() throws VariantAnnotatorException {
        super(null, null, null);
        this.factory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(factory);
        jsonObjectMapper.addMixIn(VariantAnnotation.class, VariantAnnotationMixin.class);
    }

    public static VepVariantAnnotator buildVepAnnotator() {
        try {
            return new VepVariantAnnotator();
        } catch (VariantAnnotatorException ignore) {
            return null;
        }
    }

    private static void checkNull(String value, String name) throws VariantAnnotatorException {
        if (value == null || value.isEmpty()) {
            throw new VariantAnnotatorException("Missing defaultValue: " + name);
        }
    }

    /////// CREATE ANNOTATION: empty. Vep annotation must be created beforehand by using VEP's cli and stored in a vep format file

    @Override
    public List<VariantAnnotation> annotate(List<Variant> variants) throws VariantAnnotatorException {
        return null;
    }

    @Override
    public ProjectMetadata.VariantAnnotatorProgram getVariantAnnotatorProgram() throws IOException {
        return null;
    }

    @Override
    public List<ObjectMap> getVariantAnnotatorSourceVersion() throws IOException {
        return null;
    }

    /////// LOAD ANNOTATION

    public void loadAnnotation(final VariantDBAdaptor variantDBAdaptor, final URI uri, QueryOptions options) throws IOException {

        final int batchSize = options.getInt(VariantStorageOptions.LOAD_BATCH_SIZE.key(), 100);
        final int numConsumers = options.getInt(VariantStorageOptions.LOAD_THREADS.key(), 6);
        final int numProducers = 1;
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers + numProducers);
        final BlockingQueue<VariantAnnotation> queue = new ArrayBlockingQueue<>(batchSize * numConsumers * 2);
        final VariantAnnotation lastElement = new VariantAnnotation();

        executor.execute(new Runnable() {   // producer
            @Override
            public void run() {
                try {
                    int annotationsCounter = 0;

                    /** Open vep reader **/
                    VepFormatReader vepFormatReader = new VepFormatReader(Paths.get(uri).toFile().toString());
                    vepFormatReader.open();
                    vepFormatReader.pre();

                    /** Read annotations **/
                    List<VariantAnnotation> variantAnnotation;
                    //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME
                    if (true) {
                        throw new UnsupportedOperationException();
                    }
//                    while ((variantAnnotation = vepFormatReader.read()) != null) {
//                        queue.put(variantAnnotation.get(0));  // read() method always returns a list of just one element
//                        annotationsCounter++;
//                        if (annotationsCounter % 1000 == 0) {
//                            logger.info("Element {}", annotationsCounter);
//                        }
//                    }
                    //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME //FIXME
                    // Add a lastElement marker. Consumers will stop reading when read this element.
                    for (int i = 0; i < numConsumers; i++) {
                        queue.put(lastElement);
                    }
                    logger.debug("Put Last element. queue size = {}", queue.size());
                    vepFormatReader.post();
                    vepFormatReader.close();
                } catch (InterruptedException e) {
                    logger.error("Interrupted!", e);
                    Thread.currentThread().interrupt();
                }
            }
        });

        long timestamp = System.currentTimeMillis();
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
                                variantDBAdaptor.updateAnnotations(batch, timestamp, new QueryOptions());
                                batch.clear();
                                logger.debug("thread updated batch");
                            }
                            elem = queue.take();
                        }
                        if (!batch.isEmpty()) { //Upload remaining elements
                            variantDBAdaptor.updateAnnotations(batch, timestamp, new QueryOptions());
                        }
                        logger.debug("thread finished updating annotations");
                    } catch (InterruptedException e) {
                        logger.error("interrupt", e);
                        Thread.currentThread().interrupt();
                    }
                }
            });
        }

        executor.shutdown();
        try {
            executor.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("annotation interrupted");
            Thread.currentThread().interrupt();
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
