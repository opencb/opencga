package org.opencb.opencga.storage.hadoop.variant.analysis.stats;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.tools.variant.SampleVariantStatsAnalysisExecutor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantStorageToolExecutor;
import org.opencb.opencga.storage.hadoop.variant.stats.SampleVariantStatsDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

@ToolExecutor(id="hbase-mapreduce", tool = "sample-variant-stats",
        framework = ToolExecutor.Framework.MAP_REDUCE,
        source = ToolExecutor.Source.HBASE)
public class SampleVariantStatsHBaseMapReduceAnalysisExecutor
        extends SampleVariantStatsAnalysisExecutor implements HadoopVariantStorageToolExecutor {

    private static final int SAMPLES_BATCH_SIZE = 5000;
    private static Logger logger = LoggerFactory.getLogger(SampleVariantStatsHBaseMapReduceAnalysisExecutor.class);

    @Override
    public void run() throws ToolException {
        String study = getStudy();
        List<String> sampleNames = getSampleNames();

        HadoopVariantStorageEngine engine = getHadoopVariantStorageEngine();

        try {
            VariantHadoopDBAdaptor dbAdaptor = engine.getDBAdaptor();
            int studyId = engine.getMetadataManager().getStudyId(study);

            for (String sampleName : sampleNames) {
                Integer sampleId = engine.getMetadataManager().getSampleId(studyId, sampleName, true);
                if (sampleId == null) {
                    throw VariantQueryException.sampleNotFound(sampleName, study);
                }
            }
            Path output = getOutputFile().toAbsolutePath();
            Path outdir = output.getParent();
            int numBatches = (int) (Math.ceil(sampleNames.size() / ((float) SAMPLES_BATCH_SIZE)));
            int batchSize = (int) (Math.ceil(sampleNames.size() / (float) numBatches));
            if (numBatches > 1) {
                logger.info("Execute sample stats in {} batches of {} samples", numBatches, batchSize);
            }
            for (int batch = 0; batch < numBatches; batch++) {
                List<String> batchSamples;
                if (numBatches > 1) {
                    batchSamples = sampleNames.subList(batch * batchSize, Math.min(sampleNames.size(), (batch + 1) * batchSize));
                    logger.info("Sample stats batch {}/{} with {} samples", batch + 1, numBatches, batchSamples.size());
                } else {
                    batchSamples = sampleNames;
                }
                Path tmpOutput = batch == 0 ? output : outdir.resolve(output.getFileName() + "." + batch);

                ObjectMap params = new ObjectMap(engine.getOptions())
                        .appendAll(getVariantQuery())
                        .append(SampleVariantStatsDriver.SAMPLES, batchSamples)
                        .append(SampleVariantStatsDriver.OUTPUT, tmpOutput.toAbsolutePath().toUri());
                engine.getMRExecutor().run(SampleVariantStatsDriver.class, SampleVariantStatsDriver.buildArgs(
                        dbAdaptor.getArchiveTableName(studyId),
                        dbAdaptor.getVariantTable(),
                        studyId,
                        null,
                        params
                ), "Calculate sample variant stats");
                if (batch != 0) {
                    try (OutputStream os = Files.newOutputStream(output, StandardOpenOption.APPEND)) {
                        Files.copy(tmpOutput, os);
                        Files.delete(tmpOutput);
                    }
                }
            }
        } catch (Exception e) {
            throw new ToolExecutorException(e);
        }
    }

}
