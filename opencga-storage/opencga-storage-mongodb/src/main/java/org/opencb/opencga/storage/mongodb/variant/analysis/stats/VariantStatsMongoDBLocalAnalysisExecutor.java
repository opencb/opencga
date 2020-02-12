package org.opencb.opencga.storage.mongodb.variant.analysis.stats;

import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.stats.writer.VariantStatsTsvExporter;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.VariantStatsAnalysisExecutor;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.analysis.MongoDBVariantStorageToolExecutor;
import org.opencb.opencga.storage.mongodb.variant.stats.MongoDBVariantStatsCalculator;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;

@ToolExecutor(id = "mongodb-local", tool = "variant-stats",
        framework = ToolExecutor.Framework.LOCAL,
        source = ToolExecutor.Source.MONGODB)
public class VariantStatsMongoDBLocalAnalysisExecutor extends VariantStatsAnalysisExecutor implements MongoDBVariantStorageToolExecutor {

    @Override
    public void run() throws ToolException {
        MongoDBVariantStorageEngine engine = getMongoDBVariantStorageEngine();

        VariantStorageMetadataManager metadataManager = engine.getMetadataManager();
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(getStudy());

        Map<String, List<String>> cohortsMap = getCohorts();
        List<CohortMetadata> cohorts = new ArrayList<>();
        Set<String> allSamples = new HashSet<>();
        for (Map.Entry<String, List<String>> entry : cohortsMap.entrySet()) {
            allSamples.addAll(entry.getValue());
            List<Integer> sampleIds = new ArrayList<>(entry.getValue().size());
            for (String sample : entry.getValue()) {
                Integer sampleId = metadataManager.getSampleId(studyMetadata.getId(), sample);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sample, getStudy());
                }
                sampleIds.add(sampleId);
            }
            List<Integer> files = new ArrayList<>(metadataManager.getFileIdsFromSampleIds(studyMetadata.getId(), sampleIds));

            cohorts.add(new CohortMetadata(studyMetadata.getId(), -(cohorts.size() + 1), entry.getKey(), sampleIds, files));
        }

        Query query = new Query(getVariantsQuery())
                .append(VariantQueryParam.STUDY.key(), getStudy())
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), allSamples);

        QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE,
                Arrays.asList(VariantField.STUDIES_STATS, VariantField.ANNOTATION));
        try (MongoDBIterator<Document> it = engine.getDBAdaptor().nativeIterator(query, queryOptions, true);
             OutputStream os = new BufferedOutputStream(FileUtils.openOutputStream(getOutputFile().toFile()))) {
            // reader
            DataReader<Document> reader = i -> {
                List<Document> documents = new ArrayList<>(i);
                while (it.hasNext() && i-- > 0) {
                    documents.add(it.next());
                }
                return documents;
            };

            MongoDBVariantStatsCalculator calculator = new MongoDBVariantStatsCalculator(studyMetadata, cohorts, "0/0", false);

            ProgressLogger progressLogger = new ProgressLogger("Variants processed:");
            Task<Document, Variant> task = calculator.then((Task<VariantStatsWrapper, Variant>) batch -> {
                progressLogger.increment(batch.size());
                List<Variant> variants = new ArrayList<>(batch.size());
                for (VariantStatsWrapper s : batch) {
                    Variant variant = s.toVariant();
                    StudyEntry studyEntry = new StudyEntry(getStudy());
                    studyEntry.setStats(s.getCohortStats());
                    variant.setStudies(Collections.singletonList(studyEntry));
                    variants.add(variant);
                }
                return variants;
            });


            VariantStatsTsvExporter writer = new VariantStatsTsvExporter(os, getStudy(), new ArrayList<>(cohortsMap.keySet()));

            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().build();
            ParallelTaskRunner<Document, Variant> ptr = new ParallelTaskRunner<>(reader, task, writer, config);

            ptr.run();

            addAttribute("numVariantStats", writer.getWrittenVariants());
        } catch (ExecutionException | IOException e) {
            throw new ToolExecutorException(e);
        }
    }

}
