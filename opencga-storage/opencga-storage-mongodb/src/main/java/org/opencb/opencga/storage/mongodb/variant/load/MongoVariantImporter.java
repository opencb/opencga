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

package org.opencb.opencga.storage.mongodb.variant.load;

import org.bson.Document;
import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.metadata.VariantFileMetadata;
import org.opencb.biodata.models.variant.metadata.VariantMetadata;
import org.opencb.biodata.models.variant.metadata.VariantStudyMetadata;
import org.opencb.biodata.tools.variant.metadata.VariantMetadataManager;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.io.DataWriter;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.io.VariantImporter;
import org.opencb.opencga.storage.core.variant.io.avro.VariantAvroReader;
import org.opencb.opencga.storage.mongodb.variant.adaptors.VariantMongoDBAdaptor;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToSamplesConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToStudyVariantEntryConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantConverter;
import org.opencb.opencga.storage.mongodb.variant.converters.DocumentToVariantStatsConverter;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

/**
 * Created on 07/12/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class MongoVariantImporter extends VariantImporter {

    private final MongoDBCollection variantsCollection;

    public MongoVariantImporter(VariantMongoDBAdaptor dbAdaptor) {
        super(dbAdaptor);
        this.variantsCollection = dbAdaptor.getVariantsCollection();
    }


    @Override
    public void importData(URI inputUri, VariantMetadata metadata, List<StudyConfiguration> studyConfigurations)
            throws StorageEngineException, IOException {

        Path input = Paths.get(inputUri.getPath());

        Map<String, LinkedHashMap<String, Integer>> samplesPositions = new HashMap<>();
        for (StudyConfiguration sc : studyConfigurations) {
            LinkedHashMap<String, Integer> map = StudyConfiguration.getSortedIndexedSamplesPosition(sc);
            samplesPositions.put(sc.getName(), map);
            samplesPositions.put(String.valueOf(sc.getId()), map);
        }
        VariantReader variantReader = new VariantAvroReader(input.toAbsolutePath().toFile(), samplesPositions);

        ProgressLogger progressLogger = new ProgressLogger("Loaded variants");
        ParallelTaskRunner.Task<Variant, Document> converterTask =
                new VariantToDocumentConverter(studyConfigurations, metadata, progressLogger);

        DataWriter<Document> writer = new MongoDBVariantDocumentDBWriter(variantsCollection);

        ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().setNumTasks(5).setSorted(false).setBatchSize(200).build();

        ParallelTaskRunner<Variant, Document> ptr = new ParallelTaskRunner<>(variantReader, converterTask, writer, config);

        try {
            ptr.run();
        } catch (ExecutionException e) {
            throw new StorageEngineException("Error importing variants", e);
        }
    }

    /**
     * Simple TaskMetadata for converting Variants into MongoDB Documents.
     */
    private static class VariantToDocumentConverter implements ParallelTaskRunner.Task<Variant, Document> {
        private final DocumentToVariantConverter variantConverter;
        // Remap input studyId and fileId to internal numerical Ids.
        private final Map<String, String> studiesIdRemap;
        private final Map<String, String> fileIdRemap;
        private ProgressLogger progressLogger;

        VariantToDocumentConverter(List<StudyConfiguration> studies, VariantMetadata metadata, ProgressLogger progressLogger) {
            DocumentToSamplesConverter samplesConverter = new DocumentToSamplesConverter(studies);
            DocumentToStudyVariantEntryConverter studyConverter = new DocumentToStudyVariantEntryConverter(false, samplesConverter);
            DocumentToVariantStatsConverter statsConverter = new DocumentToVariantStatsConverter();
            variantConverter = new DocumentToVariantConverter(studyConverter, statsConverter);
            this.studiesIdRemap = new HashMap<>();
            this.fileIdRemap = new HashMap<>();
            VariantMetadataManager metadataManager = new VariantMetadataManager().setVariantMetadata(metadata);

            studies.forEach((sc) -> {
                VariantStudyMetadata studyMetadata = metadataManager.getVariantStudyMetadata(sc.getName());
                this.studiesIdRemap.put(sc.getName(), String.valueOf(sc.getId()));

                sc.getFileIds().forEach((name, id) -> fileIdRemap.put(name, String.valueOf(id)));
                for (VariantFileMetadata fileMetadata : studyMetadata.getFiles()) {
                    String id = fileIdRemap.get(fileMetadata.getPath());
                    if (id != null) {
                        fileIdRemap.put(fileMetadata.getId(), id);
                    }
                }
            });

            this.progressLogger = progressLogger;
        }

        @Override
        public List<Document> apply(List<Variant> batch) {
            progressLogger.increment(batch.size(), () -> "up to position " + batch.get(batch.size() - 1));
            return batch.stream().map(variant -> {
                for (StudyEntry studyEntry : variant.getStudies()) {
                    studyEntry.setStudyId(studiesIdRemap.getOrDefault(studyEntry.getStudyId(), studyEntry.getStudyId()));
                    for (FileEntry file : studyEntry.getFiles()) {
                        if (file.getFileId().isEmpty()) {
                            file.setFileId("-1");
                        } else if (fileIdRemap.containsKey(file.getFileId())) {
                            file.setFileId(fileIdRemap.get(file.getFileId()));
                        }
                    }
                    if (studyEntry.getSamples() == null) {
                        studyEntry.setSamples(Collections.emptyList());
                    }
                }
                return variant;
            }).map(variantConverter::convertToStorageType).collect(Collectors.toList());
        }
    }

    /**
     * Simple DataWriter for importing data into MongoDB.
     */
    private static class MongoDBVariantDocumentDBWriter implements DataWriter<Document> {

        private final MongoDBCollection collection;
        private int insertedCount = 0;

        MongoDBVariantDocumentDBWriter(MongoDBCollection collection) {
            this.collection = collection;
        }

        @Override
        public boolean write(List<Document> batch) {

            DataResult result = collection.insert(batch, QueryOptions.empty());
            insertedCount += result.getNumInserted();

            return true;
        }

        @Override
        public boolean post() {
            VariantMongoDBAdaptor.createIndexes(new QueryOptions(), collection);
            return true;
        }

        public int getInsertedCount() {
            return insertedCount;
        }
    }
}
