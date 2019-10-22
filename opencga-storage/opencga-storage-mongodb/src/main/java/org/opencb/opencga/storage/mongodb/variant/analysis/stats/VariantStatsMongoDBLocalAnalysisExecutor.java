package org.opencb.opencga.storage.mongodb.variant.analysis.stats;

import com.mongodb.client.MongoCursor;
import org.apache.commons.io.FileUtils;
import org.bson.Document;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.variant.stats.writer.VariantStatsTsvExporter;
import org.opencb.commons.ProgressLogger;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.io.DataReader;
import org.opencb.commons.run.ParallelTaskRunner;
import org.opencb.commons.run.Task;
import org.opencb.opencga.core.analysis.variant.VariantStatsAnalysisExecutor;
import org.opencb.opencga.core.annotations.AnalysisExecutor;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.exception.AnalysisExecutorException;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.CohortMetadata;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.mongodb.variant.MongoDBVariantStorageEngine;
import org.opencb.opencga.storage.mongodb.variant.analysis.MongoDBAnalysisExecutor;
import org.opencb.opencga.storage.mongodb.variant.stats.MongoDBVariantStatsCalculator;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

@AnalysisExecutor(id = "mongodb-local", analysis = "variant-stats",
        framework = AnalysisExecutor.Framework.ITERATOR,
        source = AnalysisExecutor.Source.MONGODB)
public class VariantStatsMongoDBLocalAnalysisExecutor extends VariantStatsAnalysisExecutor implements MongoDBAnalysisExecutor {
    @Override
    public void exec() throws AnalysisException {
        MongoDBVariantStorageEngine engine = getMongoDBVariantStorageEngine();

        Query query = new Query(getVariantsQuery())
                .append(VariantQueryParam.STUDY.key(), getStudy())
                .append(VariantQueryParam.INCLUDE_SAMPLE.key(), getSamples());

        QueryOptions queryOptions = new QueryOptions(QueryOptions.EXCLUDE,
                Arrays.asList(VariantField.STUDIES_STATS, VariantField.ANNOTATION));
        VariantStorageMetadataManager metadataManager = engine.getMetadataManager();
        StudyMetadata studyMetadata = metadataManager.getStudyMetadata(getStudy());
        List<Integer> sampleIds = new ArrayList<>(getSamples().size());
        for (String sample : getSamples()) {
            Integer sampleId = metadataManager.getSampleId(studyMetadata.getId(), sample);
            if (sampleId == null) {
                throw VariantQueryException.sampleNotFound(sample, getStudy());
            }
            sampleIds.add(sampleId);
        }

        String cohort = getCohort();

        try (MongoCursor<Document> cursor = engine.getDBAdaptor().nativeIterator(query, queryOptions, true);
             OutputStream os = new BufferedOutputStream(FileUtils.openOutputStream(getOutputFile().toFile()))) {
            // reader
            DataReader<Document> reader = i -> {
                List<Document> documents = new ArrayList<>(i);
                while (cursor.hasNext() && i-- > 0) {
                    documents.add(cursor.next());
                }
                return documents;
            };

            List<CohortMetadata> cohorts = Collections.singletonList(new CohortMetadata(studyMetadata.getId(), -1, cohort, sampleIds));
            MongoDBVariantStatsCalculator calculator = new MongoDBVariantStatsCalculator(studyMetadata, cohorts, "0/0");

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


            VariantStatsTsvExporter writer = new VariantStatsTsvExporter(os, getStudy(), Collections.singletonList(cohort));

            ParallelTaskRunner.Config config = ParallelTaskRunner.Config.builder().build();
            ParallelTaskRunner<Document, Variant> ptr = new ParallelTaskRunner<>(reader, task, writer, config);

            ptr.run();

            arm.updateResult(analysisResult ->
                    analysisResult.getAttributes().put("numVariantStats", writer.getWrittenVariants()));
        } catch (ExecutionException | IOException e) {
            throw new AnalysisExecutorException(e);
        }
    }
}
