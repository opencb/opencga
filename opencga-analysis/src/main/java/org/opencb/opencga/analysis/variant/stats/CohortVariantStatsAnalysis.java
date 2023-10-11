/*
 * Copyright 2015-2020 OpenCB
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

package org.opencb.opencga.analysis.variant.stats;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.metadata.VariantSetStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.CohortDBAdaptor;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.cohort.Cohort;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.CohortVariantStatsAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.core.tools.variant.CohortVariantStatsAnalysisExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tool(id = CohortVariantStatsAnalysis.ID, resource = Enums.Resource.VARIANT)
public class CohortVariantStatsAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "cohort-variant-stats";
    public static final String DESCRIPTION = "Compute cohort variant stats for the selected list of samples.";
    public static final String VARIABLE_SET_ID = "opencga_cohort_variant_stats";

    @ToolParams
    protected final CohortVariantStatsAnalysisParams toolParams = new CohortVariantStatsAnalysisParams();

    private Query samplesQuery;

    private List<String> checkedSamplesList;
    private Path outputFile;

    /**
     * Study of the samples.
     * @param study Study id
     * @return this
     */
    public CohortVariantStatsAnalysis setStudy(String study) {
        super.setStudy(study);
        return this;
    }

    /**
     * List of samples.
     * @param sampleNames Sample names
     * @return this
     */
    public CohortVariantStatsAnalysis setSampleNames(List<String> sampleNames) {
        toolParams.setSamples(sampleNames);
        return this;
    }

    /**
     * Samples query to select samples to be used.
     * @param samplesQuery Samples query
     * @return this
     * @deprecated use {@link #setSampleAnnotation(String)}
     */
    @Deprecated
    public CohortVariantStatsAnalysis setSamplesQuery(Query samplesQuery) {
        this.samplesQuery = samplesQuery;
        return this;
    }

    /**
     * Samples annotation query filter to select samples to be used.
     * @param sampleAnnotation Sample annotation query filter
     * @return this
     */
    public CohortVariantStatsAnalysis setSampleAnnotation(String sampleAnnotation) {
        toolParams.setSampleAnnotation(sampleAnnotation);
        return this;
    }


    /**
     * Name of the cohort.
     *
     * @param cohortName cohort name
     * @return this
     */
    public CohortVariantStatsAnalysis setCohortName(String cohortName) {
        toolParams.setCohort(cohortName);
        return this;
    }

    /**
     * Index results in catalog.
     * Create an AnnotationSet for the VariableSet {@link #VARIABLE_SET_ID}
     * containing the stats of the cohort.
     * Requires parameter cohortName to exist.
     *
     * @param index index results
     * @return boolean
     */
    public CohortVariantStatsAnalysis setIndex(boolean index) {
        toolParams.setIndex(index);
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        setUpStorageEngineExecutor(study);

        Set<String> allSamples = new HashSet<>();

        if (StringUtils.isNotEmpty(toolParams.getSampleAnnotation())) {
            samplesQuery = new Query();
            samplesQuery.append(SampleDBAdaptor.QueryParams.STUDY.key(), study);
            samplesQuery.append(SampleDBAdaptor.QueryParams.ANNOTATION.key(), toolParams.getSampleAnnotation());
        }

        if (study == null || study.isEmpty()) {
            throw new ToolException("Missing study");
        }
        if (toolParams.isIndex()) {
            if (StringUtils.isEmpty(toolParams.getCohort())) {
                throw new ToolException("Unable to index CohortVariantStats without a cohort");
            }
            if (samplesQuery != null && !samplesQuery.isEmpty() || CollectionUtils.isNotEmpty(toolParams.getSamples())) {
                throw new ToolException("Unable to index CohortVariantStats mixing cohort with sampleNames or samplesQuery");
            }
        }
        try {
            study = catalogManager.getStudyManager().get(study, null, token).first().getFqn();

            if (CollectionUtils.isNotEmpty(toolParams.getSamples())) {
                catalogManager.getSampleManager().get(study, toolParams.getSamples(), new QueryOptions(), token)
                        .getResults()
                        .stream()
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }
            if (samplesQuery != null && !samplesQuery.isEmpty()) {
                catalogManager.getSampleManager()
                        .search(study, samplesQuery, new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID.key()), token)
                        .getResults()
                        .stream()
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }
            if (StringUtils.isNotEmpty(toolParams.getCohort())) {
                catalogManager.getCohortManager()
                        .get(study, toolParams.getCohort(), new QueryOptions(QueryOptions.INCLUDE,
                                CohortDBAdaptor.QueryParams.SAMPLES.key() + "." + SampleDBAdaptor.QueryParams.ID.key()), token)
                        .getResults()
                        .stream()
                        .flatMap(c -> c.getSamples().stream())
                        .map(Sample::getId)
                        .forEach(allSamples::add);
            }

            // Remove non-indexed samples
            Set<String> indexedSamples = variantStorageManager.getIndexedSamples(study, token);
            allSamples.removeIf(s -> !indexedSamples.contains(s));

            addAttribute("sampleNames", allSamples);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }

        if (allSamples.size() <= 1) {
            throw new ToolException("Unable to compute variant stats with cohort of size " + allSamples.size());
        }

        outputFile = getOutDir().resolve("cohort_stats.json");

        checkedSamplesList = new ArrayList<>(allSamples);
        checkedSamplesList.sort(String::compareTo);
    }

    @Override
    protected List<String> getSteps() {
        List<String> steps = super.getSteps();
        if (toolParams.isIndex()) {
            steps.add("index");
        }
        return steps;
    }

    @Override
    protected void run() throws ToolException {
        step(getId(), () -> {
            getToolExecutor(CohortVariantStatsAnalysisExecutor.class)
                    .setStudy(study)
                    .setOutputFile(outputFile)
                    .setSampleNames(checkedSamplesList)
                    .execute();
        });

        if (toolParams.isIndex()) {
            step("index", () -> {
                try {
                    VariantSetStats stats = JacksonUtils.getDefaultObjectMapper().readValue(outputFile.toFile(), VariantSetStats.class);

                    try {
                        catalogManager.getStudyManager().getVariableSet(study, VARIABLE_SET_ID, new QueryOptions(), token);
                    } catch (CatalogException e) {
                        // Assume variable set not found. Try to create
                        catalogManager.getStudyManager().createDefaultVariableSets(study, token);
                    }

                    Cohort cohort = catalogManager.getCohortManager()
                            .get(study, toolParams.getCohort(),
                                    new QueryOptions(QueryOptions.INCLUDE, CohortDBAdaptor.QueryParams.ANNOTATION_SETS.key()), token)
                            .first();
                    if (cohort.getAnnotationSets().stream().anyMatch(a -> a.getId().equals(VARIABLE_SET_ID))) {
                        catalogManager.getCohortManager()
                                .removeAnnotationSet(study, toolParams.getCohort(), VARIABLE_SET_ID, new QueryOptions(), token);
                    }
                    AnnotationSet annotationSet = AvroToAnnotationConverter.convertToAnnotationSet(stats, VARIABLE_SET_ID);
                    catalogManager.getCohortManager()
                            .addAnnotationSet(study, toolParams.getCohort(), annotationSet, new QueryOptions(), token);
                } catch (IOException | CatalogException e) {
                    throw new ToolException(e);
                }
            });
        }
    }
}

