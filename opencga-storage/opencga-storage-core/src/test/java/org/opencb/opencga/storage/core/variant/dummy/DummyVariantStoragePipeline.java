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

package org.opencb.opencga.storage.core.variant.dummy;

import htsjdk.variant.vcf.VCFConstants;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantFileMetadata;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.SampleEntry;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.UriUtils;
import org.opencb.opencga.core.common.YesNoAuto;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.io.managers.IOConnectorProvider;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.VariantStorageOptions;
import org.opencb.opencga.storage.core.variant.VariantStoragePipeline;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.dedup.AbstractDuplicatedVariantsResolver;
import org.opencb.opencga.storage.core.variant.dedup.DuplicatedVariantsResolverFactory;
import org.opencb.opencga.storage.core.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.core.variant.index.sample.genotype.SampleIndexVariantWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.opencga.storage.core.variant.VariantStorageOptions.LOAD_SAMPLE_INDEX;

/**
 * Created on 28/11/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class DummyVariantStoragePipeline extends VariantStoragePipeline {

    public static final String VARIANTS_LOAD_FAIL = "dummy.variants.load.fail";
    public static final String LOAD_SLEEP = "dummy.variants.load.sleep";
    private final Logger logger = LoggerFactory.getLogger(DummyVariantStoragePipeline.class);
    private final String dbName;
    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;

    public DummyVariantStoragePipeline(StorageConfiguration configuration, String storageEngineId, VariantDBAdaptor dbAdaptor,
                                       IOConnectorProvider ioConnectorProvider, String dbName, SampleIndexDBAdaptor sampleIndexDBAdaptor) {
        super(configuration, storageEngineId, dbAdaptor, ioConnectorProvider, new ObjectMap());
        this.dbName = dbName;
        this.sampleIndexDBAdaptor = sampleIndexDBAdaptor;
    }

    public void init(ObjectMap options) {
        getOptions().clear();
        getOptions().putAll(options);
    }

    @Override
    protected void securePreLoad(StudyMetadata studyMetadata, VariantFileMetadata variantFileMetadata) throws StorageEngineException {
        super.securePreLoad(studyMetadata, variantFileMetadata);

        List<Integer> fileIds = Collections.singletonList(getFileId());
        getMetadataManager().addRunningTask(getStudyId(), "load", fileIds, false, TaskMetadata.Type.LOAD);
    }

    @Override
    public URI load(URI input, URI outdir) throws IOException, StorageEngineException {
        logger.info("Loading file " + input);
        List<Integer> fileIds = Collections.singletonList(getFileId());
        String fileName = getMetadataManager().getFileName(getStudyId(), getFileId());
        List<Integer> sampleIds = new ArrayList<>(getMetadataManager().getFileMetadata(getStudyId(), getFileId()).getSamples());
        GetLargestVariantTask largestVariantTask = new GetLargestVariantTask();
        boolean loadSampleIndex = YesNoAuto.parse(getOptions(), LOAD_SAMPLE_INDEX.key()).orYes().booleanValue();

        if (getOptions().getInt(LOAD_SLEEP) > 0) {
            try {
                Thread.sleep(getOptions().getInt(LOAD_SLEEP));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new StorageEngineException("Interrupted", e);
            }
        }
        DuplicatedVariantsResolverFactory factory = new DuplicatedVariantsResolverFactory(getOptions(), ioConnectorProvider);
        AbstractDuplicatedVariantsResolver resolver = factory.getResolver(UriUtils.fileName(input), outdir);

        SampleIndexVariantWriter indexWriter;
        if (loadSampleIndex) {
            indexWriter = sampleIndexDBAdaptor.newSampleIndexVariantWriter(getStudyId(), getFileId(), sampleIds,
                    sampleIndexDBAdaptor.getSchemaLatest(getStudyId()), getOptions(), VariantStorageEngine.SplitData.from(getOptions()));
            indexWriter.open();
            indexWriter.pre();
        } else {
            indexWriter = null;
        }
        largestVariantTask.pre();

        VariantFileMetadata variantFileMetadata = variantReaderUtils.readVariantFileMetadata(input);
        ProgressLogger progressLogger = new ProgressLogger("Variants loaded:", variantFileMetadata.getStats().getVariantCount());
        Map<String, Variant> db = DummyVariantStorageEngine.VARIANTS.computeIfAbsent(dbName, k -> new TreeMap<>());
        for (Variant variant : variantReaderUtils.getVariantReader(input, variantFileMetadata.toVariantStudyMetadata(""),
                        getOptions().getBoolean(VariantStorageOptions.STDIN.key())).then(factory.getTask(resolver))) {
            progressLogger.increment(1, () -> "up to variant " + variant);
            if (variant.getType() == VariantType.NO_VARIATION) {
                // Skip NO_VARIATION variants
                continue;
            }
            largestVariantTask.apply(Arrays.asList(variant));
            if (indexWriter != null) {
                indexWriter.write(variant);
            }
            List<StudyEntry> studies = variant.getStudies();
            StudyEntry studyEntry = studies.get(0);
            studyEntry.setStudyId(getStudyMetadata().getName());
            studyEntry.getFiles().get(0).setFileId(fileName);
            studyEntry.getFiles().get(0).getData().remove(VCFConstants.END_KEY);
            for (SampleEntry sample : studyEntry.getSamples()) {
                sample.setFileIndex(0);
            }
            variant.setStudies(studies);
            variant.setId(variant.toString());
            if (db.containsKey(variant.toString())) {
                FileEntry fileEntry = studyEntry.getFiles().get(0);
                List<SampleEntry> samples = studyEntry.getSamples();

                Variant storedVariant = db.get(variant.toString());
                StudyEntry storedStudy = storedVariant.getStudy(getStudyMetadata().getName());
                if (storedStudy == null) {
                    storedVariant.addStudyEntry(studyEntry);
                } else {
                    storedStudy.getFiles().add(fileEntry);
                    if (!samples.isEmpty()) {
                        if (!storedStudy.getSampleDataKeys().equals(studyEntry.getSampleDataKeys())) {
                            // Map sample data keys
                            List<String> newSampleDataKeys = new ArrayList<>(storedStudy.getSampleDataKeys());
                            for (String key : studyEntry.getSampleDataKeys()) {
                                if (!newSampleDataKeys.contains(key)) {
                                    newSampleDataKeys.add(key);
                                }
                            }
                            Map<Integer, Integer> sampleDataKeyMap = new HashMap<>();
                            for (int i = 0; i < studyEntry.getSampleDataKeys().size(); i++) {
                                String key = studyEntry.getSampleDataKeys().get(i);
                                sampleDataKeyMap.put(i, newSampleDataKeys.indexOf(key));
                            }
                            storedStudy.setSampleDataKeys(newSampleDataKeys);
                            for (SampleEntry sample : samples) {
                                List<String> newData = new ArrayList<>(Collections.nCopies(newSampleDataKeys.size(), ""));
                                for (int i = 0; i < sample.getData().size(); i++) {
                                    Integer newIndex = sampleDataKeyMap.get(i);
                                    newData.set(newIndex, sample.getData().get(i));
                                }
                                sample.setData(newData);
                            }

                        }
                        LinkedHashMap<String, Integer> samplesPosition = new LinkedHashMap<>(storedStudy.getSamplesPosition());
                        for (SampleEntry sample : samples) {
                            sample.setFileIndex(storedStudy.getFiles().size() - 1);
                        }
                        for (String sample : studyEntry.getOrderedSamplesName()) {
                            if (!samplesPosition.containsKey(sample)) {
                                samplesPosition.put(sample, samplesPosition.size());
                            }
                        }
                        storedStudy.getSamples().addAll(samples);
                        storedStudy.setSortedSamplesPosition(samplesPosition);
                    }
                }
            } else {
                db.put(variant.toString(), variant);
            }
        }
        if (indexWriter != null) {
            indexWriter.post();
            indexWriter.close();
            this.loadedGenotypes = indexWriter.getLoadedGenotypes();
            this.sampleIndexVersion = indexWriter.getSampleIndexVersion();
        }
        largestVariantTask.post();
        largestVariantLength = largestVariantTask.getMaxLength();

        getLoadStats().put("duplicatedVariants", resolver.getDuplicatedVariants());
        getLoadStats().put("duplicatedLocus", resolver.getDuplicatedLocus());
        getLoadStats().put("discardedVariants", resolver.getDiscardedVariants());
        if (getOptions().getBoolean(VARIANTS_LOAD_FAIL) || getOptions().getString(VARIANTS_LOAD_FAIL).equals(Paths.get(input).getFileName().toString())) {
            getMetadataManager().atomicSetStatus(getStudyId(), TaskMetadata.Status.ERROR, "load", fileIds);
            throw new StorageEngineException("Error loading file " + input);
        } else {
            getMetadataManager().atomicSetStatus(getStudyId(), TaskMetadata.Status.DONE, "load", fileIds);
        }
        return input;
    }

    @Override
    public URI postLoad(URI input, URI output) throws StorageEngineException {
        logger.info("Post load file " + input);

        VariantFileMetadata fileMetadata = readVariantFileMetadata(input);
        fileMetadata.setId(String.valueOf(getFileId()));
        dbAdaptor.getMetadataManager().updateVariantFileMetadata(getStudyId(), fileMetadata);

        return super.postLoad(input, output);
    }

    @Override
    protected void securePostLoad(List<Integer> fileIds, StudyMetadata studyMetadata) throws StorageEngineException {
        super.securePostLoad(fileIds, studyMetadata);
        TaskMetadata.Status status = dbAdaptor.getMetadataManager()
                .setStatus(studyMetadata.getId(), "load", fileIds, TaskMetadata.Status.READY);
        if (status != TaskMetadata.Status.DONE) {
            logger.warn("Unexpected status " + status);
        }
    }

    @Override
    protected void checkLoadedVariants(int fileId, StudyMetadata studyMetadata) throws StorageEngineException {

    }
}
