package org.opencb.opencga.analysis.variant.stats;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.variant.stats.SampleVariantStatsAnalysis;
import org.opencb.oskar.core.annotations.Analysis;

import java.io.IOException;
import java.util.*;

@Analysis(id = SampleVariantStatsAnalysis.ID, data = Analysis.AnalysisData.VARIANT,
        description = "Compute sample variant stats for the selected list of samples.")
public class SampleVariantStatsOpenCgaAnalysis extends OpenCgaAnalysis {

    public static final String VARIABLE_SET_ID = "OPENCGA_SAMPLE_VARIANT_STATS";
    private String study;
    private Query samplesQuery;
    private List<String> sampleNames;
    private String individual;
    private String family;
    private ArrayList<String> checkedSamplesList;
    private boolean indexResults = false;

    /**
     * Study of the samples.
     * @param study Study id
     * @return this
     */
    public SampleVariantStatsOpenCgaAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    /**
     * List of samples.
     * @param sampleNames Sample names
     * @return this
     */
    public SampleVariantStatsOpenCgaAnalysis setSampleNames(List<String> sampleNames) {
        this.sampleNames = sampleNames;
        return this;
    }

    /**
     * Samples query to select samples to be used.
     * @param samplesQuery Samples query
     * @return this
     */
    public SampleVariantStatsOpenCgaAnalysis setSamplesQuery(Query samplesQuery) {
        this.samplesQuery = samplesQuery;
        return this;
    }

    /**
     * Select samples from this individual.
     * @param individualId individual
     * @return this
     */
    public SampleVariantStatsOpenCgaAnalysis setIndividual(String individualId) {
        this.individual = individualId;
        return this;
    }

    /**
     * Select samples form the individuals of this family.
     * @param family family
     * @return this
     */
    public SampleVariantStatsOpenCgaAnalysis setFamily(String family) {
        this.family = family;
        return this;
    }

    /**
     * Index results in catalog.
     * Create an AnnotationSet for the VariableSet {@link #VARIABLE_SET_ID}
     * containing the SampleVariantStats.
     *
     * @param indexResults index results
     * @return boolean
     */
    public SampleVariantStatsOpenCgaAnalysis setIndexResults(boolean indexResults) {
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
        try {
            study = catalogManager.getStudyManager().get(study, null, sessionId).first().getFqn();
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }
        arm.startStep("Check samples");

        try {
            if (CollectionUtils.isNotEmpty(sampleNames)) {
                catalogManager.getSampleManager().get(study, sampleNames, new QueryOptions(), sessionId)
                        .stream()
                        .map(QueryResult::first)
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }
            if (samplesQuery != null) {
                catalogManager.getSampleManager().get(study, samplesQuery, new QueryOptions(), sessionId)
                        .getResult()
                        .stream()
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }
            if (StringUtils.isNotEmpty(individual)) {
                Query query = new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), individual);
                catalogManager.getSampleManager().get(study, query, new QueryOptions(), sessionId)
                        .getResult()
                        .stream()
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }
            if (StringUtils.isNotEmpty(family)) {
                Family family = catalogManager.getFamilyManager().get(study, this.family, null, sessionId).first();
                List<String> individualIds = new ArrayList<>(family.getMembers().size());
                for (Individual member : family.getMembers()) {
                    individualIds.add(member.getId());
                }
                Query query = new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL.key(), individualIds);
                catalogManager.getSampleManager().get(study, query, new QueryOptions(), sessionId)
                        .getResult()
                        .stream()
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }

        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }

        checkedSamplesList = new ArrayList<>(allSamples);
        checkedSamplesList.sort(String::compareTo);

        if (allSamples.isEmpty()) {
            throw new AnalysisException("Missing samples!");
        }

        // check read permission
        try {
            variantStorageManager.checkQueryPermissions(
                    new Query()
                            .append(VariantQueryParam.STUDY.key(), study)
                            .append(VariantQueryParam.INCLUDE_SAMPLE.key(), checkedSamplesList),
                    new QueryOptions(),
                    sessionId);
        } catch (CatalogException | StorageEngineException e) {
            throw new AnalysisException(e);
        }

        arm.endStep(5);
    }

    @Override
    protected void exec() throws AnalysisException {
        SampleVariantStatsAnalysis oskarAnalysis = new SampleVariantStatsAnalysis()
                .setStudy(study)
                .setSampleNames(checkedSamplesList);
        oskarAnalysis.setUp(executorParams, outDir, sourceTypes, availableFrameworks);
        oskarAnalysis.execute(arm);

        if (indexResults) {
            arm.startStep("index results", 95f);
            indexResults(oskarAnalysis);
        }

        arm.endStep(100);
    }

    private void indexResults(SampleVariantStatsAnalysis oskarAnalysis) throws AnalysisException {
        List<SampleVariantStats> stats = new ArrayList<>(checkedSamplesList.size());
        try {
            JacksonUtils.getDefaultObjectMapper()
                    .readerFor(SampleVariantStats.class)
                    .<SampleVariantStats>readValues(oskarAnalysis.getOutputFile().toFile())
                    .forEachRemaining(stats::add);

            try {
                catalogManager.getStudyManager().getVariableSet(study, VARIABLE_SET_ID, new QueryOptions(), sessionId);
            } catch (CatalogException e) {
                // Assume variable set not found. Try to create
                List<Variable> variables = AvroToAnnotationConverter.convertToVariableSet(SampleVariantStats.getClassSchema());
                catalogManager.getStudyManager()
                        .createVariableSet(study, VARIABLE_SET_ID, VARIABLE_SET_ID, true, false, "", Collections.emptyMap(), variables,
                        Collections.singletonList(VariableSet.AnnotableDataModels.SAMPLE), sessionId);
            }

            for (SampleVariantStats sampleStats : stats) {
                AnnotationSet annotationSet = AvroToAnnotationConverter.convertToAnnotationSet(sampleStats, VARIABLE_SET_ID);
                catalogManager.getSampleManager()
                        .addAnnotationSet(study, sampleStats.getId(), annotationSet, new QueryOptions(), sessionId);
            }
        } catch (IOException | CatalogException e) {
            throw new AnalysisException(e);
        }
    }
}
