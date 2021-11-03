package org.opencb.opencga.storage.hadoop.variant.analysis.gwas;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.tools.variant.GwasAnalysisExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.exceptions.ToolExecutorException;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.analysis.HadoopVariantStorageToolExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;

import java.util.List;

@ToolExecutor(id = "hbase-mapreduce", tool = "gwas",
        framework = ToolExecutor.Framework.MAP_REDUCE,
        source = ToolExecutor.Source.HBASE)
public class GwasHBaseMapReduceAnalysisExecutor extends GwasAnalysisExecutor implements HadoopVariantStorageToolExecutor {

    @Override
    public void run() throws ToolException {
        String study = getStudy();
        List<String> samples1 = getSampleList1();
        List<String> samples2 = getSampleList2();

        if (getConfiguration().getMethod().equals(GwasConfiguration.Method.CHI_SQUARE_TEST)) {
            addWarning("Unable to calculate chi-square test.");
        }

        HadoopVariantStorageEngine engine = getHadoopVariantStorageEngine();

        try {
            VariantHadoopDBAdaptor dbAdaptor = engine.getDBAdaptor();
            int studyId = engine.getMetadataManager().getStudyId(study);

            checkSamples(engine, study, samples1);
            checkSamples(engine, study, samples2);

            ObjectMap params = new ObjectMap()
                    .append(FisherTestDriver.CASE_COHORT, samples1)
                    .append(FisherTestDriver.CONTROL_COHORT, samples2)
                    .append(FisherTestDriver.OUTPUT, getOutputFile().toAbsolutePath().toUri());
            engine.getMRExecutor().run(FisherTestDriver.class, FisherTestDriver.buildArgs(
                    dbAdaptor.getArchiveTableName(studyId),
                    dbAdaptor.getVariantTable(),
                    studyId,
                    null,
                    params
            ), "Calculate sample variant stats");
        } catch (VariantQueryException | StorageEngineException e) {
            throw new ToolExecutorException(e);
        }
    }

    public void checkSamples(HadoopVariantStorageEngine engine, String study, List<String> samples) throws StorageEngineException {
        int studyId = engine.getMetadataManager().getStudyId(study);
        for (String sampleName : samples) {
            Integer sampleId = engine.getMetadataManager().getSampleId(studyId, sampleName, true);
            if (sampleId == null) {
                throw VariantQueryException.sampleNotFound(sampleName, study);
            }
        }
    }
}
