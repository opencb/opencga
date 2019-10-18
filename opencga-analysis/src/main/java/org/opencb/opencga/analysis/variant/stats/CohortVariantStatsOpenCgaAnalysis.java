package org.opencb.opencga.analysis.variant.stats;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.AnnotationSet;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.models.Variable;
import org.opencb.opencga.core.models.VariableSet;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.variant.stats.CohortVariantStatsAnalysis;
import org.opencb.oskar.core.annotations.Analysis;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

@Analysis(id = CohortVariantStatsAnalysis.ID, data = Analysis.AnalysisData.VARIANT)
public class CohortVariantStatsOpenCgaAnalysis extends OpenCgaAnalysis {

    public static final String VARIABLE_SET_ID = "OPENCGA_COHORT_VARIANT_STATS";
    private String study;
    private List<String> sampleNames;
    private Query samplesQuery;
    private String cohortName;
    private boolean indexResults;

    private List<String> checkedSamplesList;

    /**
     * Study of the samples.
     * @param study Study id
     * @return this
     */
    public CohortVariantStatsOpenCgaAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    /**
     * List of samples.
     * @param sampleNames Sample names
     * @return this
     */
    public CohortVariantStatsOpenCgaAnalysis setSampleNames(List<String> sampleNames) {
        this.sampleNames = sampleNames;
        return this;
    }

    /**
     * Samples query to select samples to be used.
     * @param samplesQuery Samples query
     * @return this
     */
    public CohortVariantStatsOpenCgaAnalysis setSamplesQuery(Query samplesQuery) {
        this.samplesQuery = samplesQuery;
        return this;
    }


    /**
     * Name of the cohort.
     *
     * @param cohortName cohort name
     * @return this
     */
    public CohortVariantStatsOpenCgaAnalysis setCohortName(String cohortName) {
        this.cohortName = cohortName;
        return this;
    }

    /**
     * Index results in catalog.
     * Create an AnnotationSet for the VariableSet {@link #VARIABLE_SET_ID}
     * containing the stats of the cohort.
     * Requires parameter cohortName to exist.
     *
     * @param indexResults index results
     * @return boolean
     */
    public CohortVariantStatsOpenCgaAnalysis setIndexResults(boolean indexResults) {
        this.indexResults = indexResults;
        return this;
    }

    @Override
    protected void check() throws AnalysisException {
        super.check();
        setUpStorageEngineExecutor(study);

        Set<String> allSamples = new HashSet<>();

        if (study == null || study.isEmpty()) {
            throw new AnalysisException("Missing study");
        }
        if (indexResults) {
            if (StringUtils.isEmpty(cohortName)) {
                throw new AnalysisException("Unable to index CohortVariantStats without a cohort");
            }
            if (samplesQuery != null && !samplesQuery.isEmpty() || CollectionUtils.isNotEmpty(sampleNames)) {
                throw new AnalysisException("Unable to index CohortVariantStats mixing cohort with sampleNames or samplesQuery");
            }
        }
        try {
            study = catalogManager.getStudyManager().get(study, null, sessionId).first().getFqn();

            if (CollectionUtils.isNotEmpty(sampleNames)) {
                catalogManager.getSampleManager().get(study, sampleNames, new QueryOptions(), sessionId)
                        .getResults()
                        .stream()
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }
            if (samplesQuery != null && !samplesQuery.isEmpty()) {
                catalogManager.getSampleManager().search(study, samplesQuery, new QueryOptions(), sessionId)
                        .getResults()
                        .stream()
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }
            if (StringUtils.isNotEmpty(cohortName)) {
                catalogManager.getCohortManager().get(study, cohortName, new QueryOptions(), sessionId)
                        .getResults()
                        .stream()
                        .flatMap(c -> c.getSamples().stream())
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }

            // Remove non-indexed samples
            Set<String> indexedSamples = variantStorageManager.getIndexedSamples(study, sessionId);
            allSamples.removeIf(s -> !indexedSamples.contains(s));

            arm.updateResult(analysisResult -> analysisResult.getAttributes().put("sampleNames", allSamples));
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }

        if (allSamples.size() <= 1) {
            throw new AnalysisException("Unable to compute variant stats with cohort of size " + allSamples.size());
        }

        checkedSamplesList = new ArrayList<>(allSamples);
        checkedSamplesList.sort(String::compareTo);
    }

    @Override
    protected void exec() throws AnalysisException {

        CohortVariantStatsAnalysis analysis = new CohortVariantStatsAnalysis();
        analysis.setUp(executorParams, outDir, sourceTypes, availableFrameworks);
        analysis.setStudy(study).setSampleNames(checkedSamplesList);
        analysis.execute(arm);

        if (indexResults) {
            try {
                Path file = outDir.resolve(arm.read().getOutputFiles().get(0).getPath());
                VariantSetStats stats = JacksonUtils.getDefaultObjectMapper().readValue(file.toFile(), VariantSetStats.class);

                try {
                    catalogManager.getStudyManager().getVariableSet(study, VARIABLE_SET_ID, new QueryOptions(), sessionId);
                } catch (CatalogException e) {
                    // Assume variable set not found. Try to create
                    List<Variable> variables = AvroToAnnotationConverter.convertToVariableSet(VariantSetStats.getClassSchema());
                    catalogManager.getStudyManager()
                            .createVariableSet(study, VARIABLE_SET_ID, VARIABLE_SET_ID, true, false, "", Collections.emptyMap(), variables,
                                    Arrays.asList(VariableSet.AnnotableDataModels.COHORT, VariableSet.AnnotableDataModels.FILE), sessionId);
                }

                AnnotationSet annotationSet = AvroToAnnotationConverter.convertToAnnotationSet(stats, VARIABLE_SET_ID);
                catalogManager.getCohortManager()
                        .addAnnotationSet(study, cohortName, annotationSet, new QueryOptions(), sessionId);
            } catch (IOException | CatalogException e) {
                throw new AnalysisException(e);
            }
        }

    }
}

