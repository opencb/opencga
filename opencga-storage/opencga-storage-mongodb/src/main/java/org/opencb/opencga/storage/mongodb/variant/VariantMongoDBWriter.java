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

package org.opencb.opencga.storage.mongodb.variant;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.io.VariantDBWriter;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cyenyxe@ebi.ac.uk>
 */
public class VariantMongoDBWriter extends VariantDBWriter {

    public static final int CHUNK_SIZE_SMALL = 1000;
    public static final int CHUNK_SIZE_BIG = 10000;
    protected static org.slf4j.Logger logger = LoggerFactory.getLogger(VariantDBWriter.class);
    private final VariantMongoDBAdaptor dbAdaptor;


    private boolean includeStats;
    private boolean includeSrc = true;
    private boolean includeSamples;

    private DBObjectToVariantConverter variantConverter;
    private DBObjectToVariantStatsConverter statsConverter;
    private DBObjectToVariantSourceConverter sourceConverter;
    private DBObjectToStudyVariantEntryConverter sourceEntryConverter;
    private DBObjectToSamplesConverter sampleConverter;

//    private long numVariantsWritten;
    private static long staticNumVariantsWritten;

//    private long checkExistsTime = 0;
//    private long checkExistsDBTime = 0;
    private long insertionTime = 0;
    private StudyConfiguration studyConfiguration;

    private int fileId;
    private boolean writeStudyConfiguration = true;

    private AtomicBoolean variantSourceWritten = new AtomicBoolean(false);
    private MongoDBVariantWriteResult writeResult = new MongoDBVariantWriteResult();
    private HashSet<String> coveredChromosomes = new HashSet<>();
    private List<Integer> fileSampleIds;
    private List<Integer> loadedSampleIds;


    public VariantMongoDBWriter(Integer fileId, StudyConfiguration studyConfiguration, VariantMongoDBAdaptor dbAdaptor,
                                boolean includeSamples, boolean includeStats) {
        this.dbAdaptor = dbAdaptor;
        this.studyConfiguration = studyConfiguration;
        this.fileId = fileId;

        this.includeSamples = includeSamples;
        this.includeStats = includeStats;
    }

    @Override
    public boolean open() {
        staticNumVariantsWritten = 0;
        insertionTime = 0;
        coveredChromosomes.clear();
//        numVariantsWritten = 0;

        return true;
    }

    @Override
    public boolean pre() {
        this.fileSampleIds = new LinkedList<>(studyConfiguration.getSamplesInFiles().get(fileId));
        loadedSampleIds = VariantMongoDBAdaptor.getLoadedSamples(fileId, studyConfiguration);

        setConverters();

        return true;
    }

    @Override
    public boolean write(Variant variant) {
        return write(Collections.singletonList(variant));
    }

    @Override
    public boolean write(List<Variant> data) {
//        return write_setOnInsert(data);
        synchronized (variantSourceWritten) {
            long l = staticNumVariantsWritten/1000;
            staticNumVariantsWritten += data.size();
            if (staticNumVariantsWritten/1000 != l) {
                logger.info("Num variants written " + staticNumVariantsWritten);
            }
        }

        if (!data.isEmpty()) {
            coveredChromosomes.add(data.get(0).getChromosome());
        }
        QueryResult<MongoDBVariantWriteResult> queryResult = dbAdaptor.insert(data, fileId, this.variantConverter, this.sourceEntryConverter, studyConfiguration, loadedSampleIds);

        MongoDBVariantWriteResult batchWriteResult = queryResult.first();
        logger.debug("New batch of {} elements. WriteResult: {}", data.size(), batchWriteResult);
        writeResult.merge(batchWriteResult);

        insertionTime += queryResult.getDbTime();
        return true;
    }


    @Override @Deprecated
    protected boolean buildBatchRaw(List<Variant> data) {
        return true;
    }

    @Override @Deprecated
    protected boolean buildEffectRaw(List<Variant> variants) {
        return false;
    }

    @Override @Deprecated
    protected boolean buildBatchIndex(List<Variant> data) {
        return false;
    }

    @Override @Deprecated
    protected boolean writeBatch(List<Variant> batch) {
        return true;
    }

    private boolean writeStudyConfiguration() {
        dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, new QueryOptions());
        return true;
    }

    @Override
    public boolean post() {
//        if (currentBulkSize != 0) {
//            executeBulk();
//        }
        logger.debug("POST");
        if (!variantSourceWritten.getAndSet(true)) {
            if (writeStudyConfiguration) {
                writeStudyConfiguration();
            }
//            if (writeVariantSource) {
//                writeSourceSummary(source);
//            }

            List<Region> regions = coveredChromosomes.stream().map(Region::new).collect(Collectors.toList());
            dbAdaptor.fillFileGaps(fileId, new LinkedList<>(coveredChromosomes), fileSampleIds, studyConfiguration);
            dbAdaptor.createIndexes(new QueryOptions());

        }

//        logger.debug("checkExistsTime " + checkExistsTime / 1000000.0 + "ms ");
//        logger.debug("checkExistsDBTime " + checkExistsDBTime / 1000000.0 + "ms ");
//        logger.debug("bulkTime " + bulkTime / 1000000.0 + "ms ");
        logger.debug("insertionTime " + insertionTime / 1000000.0 + "ms ");
        return true;
    }

    @Override
    public boolean close() {
//        this.mongoDataStoreManager.close(this.mongoDataStore.getDb().getName());
        return true;
    }

    @Override
    public final void includeStats(boolean b) {
        includeStats = b;
    }

    public final void includeSrc(boolean b) {
        includeSrc = b;
    }

    @Override
    public final void includeSamples(boolean b) {
        includeSamples = b;
    }

    @Override @Deprecated
    public final void includeEffect(boolean b) {
    }

    public void setThreadSynchronizationBoolean(AtomicBoolean atomicBoolean) {
        this.variantSourceWritten = atomicBoolean;
    }

    private void setConverters() {

        sourceConverter = new DBObjectToVariantSourceConverter();
        statsConverter = includeStats ? new DBObjectToVariantStatsConverter(dbAdaptor.getStudyConfigurationManager()) : null;
        sampleConverter = includeSamples ? new DBObjectToSamplesConverter(studyConfiguration) : null;

        sourceEntryConverter = new DBObjectToStudyVariantEntryConverter(includeSrc, sampleConverter);

        sourceEntryConverter.setIncludeSrc(includeSrc);

        // Do not create the VariantConverter with the sourceEntryConverter.
        // The variantSourceEntry conversion will be done on demand to create a proper mongoDB update query.
        // variantConverter = new DBObjectToVariantConverter(sourceEntryConverter);
        variantConverter = new DBObjectToVariantConverter(null, statsConverter);
    }


    public void setWriteStudyConfiguration(boolean writeStudyConfiguration) {
        this.writeStudyConfiguration = writeStudyConfiguration;
    }

    public MongoDBVariantWriteResult getWriteResult() {
        return writeResult;
    }
}
