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

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.variant.metadata.SampleVariantStats;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.tools.OpenCgaToolScopeStudy;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.utils.AvroToAnnotationConverter;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.variant.SampleVariantStatsAnalysisParams;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.variant.SampleVariantStatsAnalysisExecutor;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Tool(id = SampleVariantStatsAnalysis.ID, resource = Enums.Resource.VARIANT, description = SampleVariantStatsAnalysis.DESCRIPTION)
public class SampleVariantStatsAnalysis extends OpenCgaToolScopeStudy {

    public static final String ID = "sample-variant-stats";
    public static final String DESCRIPTION = "Compute sample variant stats for the selected list of samples.";
    public static final String VARIABLE_SET_ID = "opencga_sample_variant_stats";
    private String study;

    private final SampleVariantStatsAnalysisParams toolParams = new SampleVariantStatsAnalysisParams();
    private ArrayList<String> checkedSamplesList;
    private Path outputFile;

    /**
     * List of samples.
     * @param sampleNames Sample names
     * @return this
     */
    public SampleVariantStatsAnalysis setSampleNames(List<String> sampleNames) {
        toolParams.setSample(sampleNames);
        return this;
    }

//    /**
//     * Samples query to select samples to be used.
//     * @param samplesQuery Samples query
//     * @return this
//     */
//    public SampleVariantStatsAnalysis setSamplesQuery(Query samplesQuery) {
//        this.samplesQuery = samplesQuery;
//        return this;
//    }
//
//    /**
//     * Select samples from this individual.
//     * @param individualId individual
//     * @return this
//     */
//    public SampleVariantStatsAnalysis setIndividual(String individualId) {
//        this.individual = individualId;
//        return this;
//    }

    /**
     * Select samples form the individuals of this family.
     * @param family family
     * @return this
     */
    public SampleVariantStatsAnalysis setFamily(String family) {
        this.toolParams.setFamily(family);
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
    public SampleVariantStatsAnalysis setIndexResults(boolean indexResults) {
        toolParams.setIndex(indexResults);
        return this;
    }

    @Override
    protected void check() throws Exception {
        super.check();
        toolParams.updateParams(params);
        study = getStudyFqn();
        setUpStorageEngineExecutor(study);
        Set<String> allSamples = new HashSet<>();

        Set<String> indexedSamples = variantStorageManager.getIndexedSamples(study, token);
        List<String> sampleNames = toolParams.getSample();
        if (CollectionUtils.isNotEmpty(sampleNames)) {
            catalogManager.getSampleManager().get(study, sampleNames, new QueryOptions(), token)
                    .getResults()
                    .stream()
                    .map(Sample::getId)
                    .forEach(allSamples::add);
        }

//            if (samplesQuery != null) {
//                List<String> samplesFromQuery = new LinkedList<>();
//                catalogManager.getSampleManager().search(study, samplesQuery, new QueryOptions(), token)
//                        .getResults()
//                        .stream()
//                        .map(Sample::getId)
//                        .forEach(samplesFromQuery::add);
//                // Remove non-indexed samples
//                samplesFromQuery.removeIf(s -> !indexedSamples.contains(s));
//                allSamples.addAll(samplesFromQuery);
//            }
//            if (StringUtils.isNotEmpty(individual)) {
//                Query query = new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individual);
//                catalogManager.getSampleManager().search(study, query, new QueryOptions(), token)
//                        .getResults()
//                        .stream()
//                        .map(Sample::getId)
//                        .forEach(allSamples::add);
//            }
        if (StringUtils.isNotEmpty(toolParams.getFamily())) {
            Family family = catalogManager.getFamilyManager().get(study, toolParams.getFamily(), null, token).first();
            List<String> individualIds = new ArrayList<>(family.getMembers().size());
            for (Individual member : family.getMembers()) {
                individualIds.add(member.getId());
            }
            Query query = new Query(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualIds);
            catalogManager.getSampleManager().search(study, query, new QueryOptions(), token)
                    .getResults()
                    .stream()
                    .map(Sample::getId)
                    .forEach(allSamples::add);
        }

        List<String> nonIndexedSamples = new ArrayList<>();
        // Remove non-indexed samples
        for (String sample : allSamples) {
            if (!indexedSamples.contains(sample)) {
                nonIndexedSamples.add(sample);
            }
        }
        if (!nonIndexedSamples.isEmpty()) {
            throw new IllegalArgumentException("Samples " + nonIndexedSamples + " are not indexed into the Variant Storage");
        }

        checkedSamplesList = new ArrayList<>(allSamples);
        checkedSamplesList.sort(String::compareTo);

        if (allSamples.isEmpty()) {
            throw new ToolException("Missing samples!");
        }

        // check read permission
        variantStorageManager.checkQueryPermissions(
                new Query()
                        .append(VariantQueryParam.STUDY.key(), study)
                        .append(VariantQueryParam.INCLUDE_SAMPLE.key(), checkedSamplesList),
                new QueryOptions(),
                token);

        outputFile = getOutDir().resolve(getId() + ".json");
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
            getToolExecutor(SampleVariantStatsAnalysisExecutor.class)
                    .setOutputFile(outputFile)
                    .setStudy(study)
                    .setSampleNames(checkedSamplesList)
                    .setVariantQuery(toolParams.getVariantQuery() == null ? new Query() : toolParams.getVariantQuery().toQuery())
                    .execute();
        });

        if (toolParams.isIndex()) {
            step("index", () -> indexResults(outputFile));
        }
    }

    private void indexResults(Path outputFile) throws ToolException {
        List<SampleVariantStats> stats = new ArrayList<>(checkedSamplesList.size());
        try {
            JacksonUtils.getDefaultObjectMapper()
                    .readerFor(SampleVariantStats.class)
                    .<SampleVariantStats>readValues(outputFile.toFile())
                    .forEachRemaining(stats::add);

            try {
                catalogManager.getStudyManager().getVariableSet(study, VARIABLE_SET_ID, new QueryOptions(), token);
            } catch (CatalogException e) {
                // Assume variable set not found. Try to create
                catalogManager.getStudyManager().createDefaultVariableSets(study, token);
            }

            for (SampleVariantStats sampleStats : stats) {
                AnnotationSet annotationSet = AvroToAnnotationConverter.convertToAnnotationSet(sampleStats, VARIABLE_SET_ID);
                catalogManager.getSampleManager()
                        .addAnnotationSet(study, sampleStats.getId(), annotationSet, new QueryOptions(), token);
            }
        } catch (IOException | CatalogException e) {
            throw new ToolException(e);
        }
    }

}
