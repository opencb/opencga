package org.opencb.opencga.analysis.variant.stats;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.analysis.variant.VariantStatsAnalysisExecutor;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.models.Cohort;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.analysis.result.FileResult;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Analysis(id = VariantStatsAnalysis.ID, data = Analysis.AnalysisData.VARIANT,
        description = "Compute variant stats for any cohort and any set of variants.")
public class VariantStatsAnalysis extends OpenCgaAnalysis {

    public final static String ID = "variant-stats";

    private String study;
    private String cohortName;
    private Query samplesQuery;
    private Query variantsQuery;
    private Path outputFile;
    private List<String> sampleNames;

    public VariantStatsAnalysis() {
    }

    /**
     * Study of the samples.
     * @param study Study id
     * @return this
     */
    public VariantStatsAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    /**
     * Samples query selecting samples of the cohort.
     * Optional if provided {@link #cohortName}.
     *
     * @param samplesQuery sample query
     * @return this
     */
    public VariantStatsAnalysis setSamplesQuery(Query samplesQuery) {
        this.samplesQuery = samplesQuery;
        return this;
    }

    /**
     * Name of the cohort.
     * Optional if provided {@link #samplesQuery}.
     * When used without {@link #samplesQuery}, the cohort must be defined in catalog.
     * It's samples will be used to calculate the variant stats.
     * When used together with {@link #samplesQuery}, this name will be just an alias to be used in the output file.
     *
     * @param cohortName cohort name
     * @return this
     */
    public VariantStatsAnalysis setCohortName(String cohortName) {
        this.cohortName = cohortName;
        return this;
    }

    /**
     * Variants query. If not provided, all variants from the study will be used.
     * @param variantsQuery variants query.
     * @return this
     */
    public VariantStatsAnalysis setVariantsQuery(Query variantsQuery) {
        this.variantsQuery = variantsQuery;
        return this;
    }

    @Override
    protected void check() throws AnalysisException {
        super.check();
        setUpStorageEngineExecutor(study);

        if ((samplesQuery == null || samplesQuery.isEmpty()) && StringUtils.isEmpty(cohortName)) {
            throw new AnalysisException("Unspecified cohort or list of samples");
        }
        if (StringUtils.isEmpty(cohortName)) {
            cohortName = "COHORT";
        }
        try {
            study = catalogManager.getStudyManager().get(study, new QueryOptions(QueryOptions.INCLUDE, "fqn"), sessionId).first().getFqn();
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }

        try {
            List<Sample> samples;
            if (samplesQuery == null || samplesQuery.isEmpty()) {
                Cohort cohort = catalogManager.getCohortManager()
                        .get(study, cohortName, new QueryOptions(), sessionId).first();
                samples = cohort.getSamples();
                arm.updateResult(analysisResult -> analysisResult.getAttributes().put("cohortName", cohortName));
            } else {
                samples = catalogManager.getSampleManager()
                        .search(study, new Query(samplesQuery), new QueryOptions(QueryOptions.INCLUDE, "id"), sessionId).getResults();
            }
            sampleNames = samples.stream().map(Sample::getId).collect(Collectors.toList());

            // Remove non-indexed samples
            Set<String> indexedSamples = variantStorageManager.getIndexedSamples(study, sessionId);
            sampleNames.removeIf(s -> !indexedSamples.contains(s));

            arm.updateResult(analysisResult -> analysisResult.getAttributes().put("sampleNames", sampleNames));
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }

        if (sampleNames.size() <= 1) {
            throw new AnalysisException("Unable to compute variant stats with cohort of size " + sampleNames.size());
        }

        if (variantsQuery == null) {
            variantsQuery = new Query();
        }
        variantsQuery.putIfAbsent(VariantQueryParam.STUDY.key(), study);

        arm.updateResult(r -> r.getAttributes().append("variantsQuery", variantsQuery));

        // check read permission
        try {

            variantStorageManager.checkQueryPermissions(
                    new Query(variantsQuery)
                            .append(VariantQueryParam.STUDY.key(), study)
                            .append(VariantQueryParam.INCLUDE_SAMPLE.key(), sampleNames),
                    new QueryOptions(),
                    sessionId);
        } catch (CatalogException | StorageEngineException e) {
            throw new AnalysisException(e);
        }

        if (StringUtils.isEmpty(cohortName)) {
            outputFile = outDir.resolve("variant_stats.tsv");
        } else {
            outputFile = outDir.resolve("variant_stats_" + cohortName + ".tsv");
        }

    }

    @Override
    protected void exec() throws AnalysisException {

        arm.startStep("variant-stats");
        VariantStatsAnalysisExecutor executor = getAnalysisExecutor(VariantStatsAnalysisExecutor.class);

        executor.setStudy(study)
                .setCohort(cohortName)
                .setSamples(sampleNames)
                .setOutputFile(outputFile)
                .setVariantsQuery(variantsQuery);

        executor.exec();
        arm.endStep(100);

        if (outputFile.toFile().exists()) {
            arm.addFile(outputFile, FileResult.FileType.TAB_SEPARATED);
        } else {
            arm.addWarning("Output file not generated");
        }
    }

}
