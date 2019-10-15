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

package org.opencb.opencga.analysis.variant.gwas;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.analysis.variant.gwas.Gwas;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;
import org.opencb.oskar.core.annotations.Analysis;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Analysis(id = Gwas.ID, data = Analysis.AnalysisData.VARIANT,
        description = "Run a Genome Wide Association Study between two cohorts.")
public class GwasOpenCgaAnalysis extends OpenCgaAnalysis {

    private GwasConfiguration gwasConfiguration;
    private String study;
    private String caseCohort;
    private String controlCohort;
    private Query caseCohortSamplesQuery;
    private Query controlCohortSamplesQuery;
    private List<String> caseCohortSamples;
    private List<String> controlCohortSamples;
    private String scoreName;
    private boolean index;

    public GwasOpenCgaAnalysis() {
    }

    /**
     * Provide the GWAS analysis configuration.
     * @param gwasConfiguration configuration
     * @return this
     */
    public GwasOpenCgaAnalysis setGwasConfiguration(GwasConfiguration gwasConfiguration) {
        this.gwasConfiguration = gwasConfiguration;
        return this;
    }

    /**
     * Study of the samples.
     * @param study Study id
     * @return this
     */
    public GwasOpenCgaAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    /**
     * Samples query selecting samples of the case cohort.
     * This parameter is an alternative to {@link #setCaseCohort}
     *
     * @param caseCohortSamplesQuery sample query
     * @return this
     */
    public GwasOpenCgaAnalysis setCaseCohortSamplesQuery(Query caseCohortSamplesQuery) {
        this.caseCohortSamplesQuery = caseCohortSamplesQuery;
        return this;
    }

    /**
     * Samples query selecting samples of the control cohort.
     * This parameter is an alternative to {@link #setControlCohort}
     *
     * @param controlCohortSamplesQuery sample query
     * @return this
     */
    public GwasOpenCgaAnalysis setControlCohortSamplesQuery(Query controlCohortSamplesQuery) {
        this.controlCohortSamplesQuery = controlCohortSamplesQuery;
        return this;
    }

    /**
     * Cohort from catalog to be used as case cohort.
     * This parameter is an alternative to {@link #setCaseCohortSamplesQuery}
     *
     * @param caseCohort cohort name
     * @return this
     */
    public GwasOpenCgaAnalysis setCaseCohort(String caseCohort) {
        this.caseCohort = caseCohort;
        return this;
    }

    /**
     * Cohort from catalog to be used as control cohort.
     * This parameter is an alternative to {@link #setControlCohortSamplesQuery}
     *
     * @param controlCohort cohort name
     * @return this
     */
    public GwasOpenCgaAnalysis setControlCohort(String controlCohort) {
        this.controlCohort = controlCohort;
        return this;
    }

    /**
     * Name to be used to index que score in the variant storage.
     * Must be unique in the study. If provided, the control/case cohorts must be registered in catalog.
     *
     * @param scoreName score name
     * @return this
     */
    public GwasOpenCgaAnalysis setScoreName(String scoreName) {
        this.scoreName = scoreName;
        return this;
    }

    @Override
    protected void check() throws AnalysisException {
        super.check();
        setUpStorageEngineExecutor(study);

        if (gwasConfiguration == null) {
            gwasConfiguration = new GwasConfiguration()
                    .setMethod(GwasConfiguration.Method.FISHER_TEST)
                    .setFisherMode(GwasConfiguration.FisherMode.TWO_SIDED);
        }

        if (StringUtils.isEmpty(study)) {
            throw new AnalysisException("Missing study!");
        }

        try {
            study = catalogManager.getStudyManager().get(study, null, sessionId).first().getFqn();
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }

        caseCohortSamples = getCohortSamples(caseCohort, caseCohortSamplesQuery, "case");
        controlCohortSamples = getCohortSamples(controlCohort, controlCohortSamplesQuery, "control");

        if (!Collections.disjoint(caseCohortSamples, controlCohortSamples)) {
            List<String> overlapping = new ArrayList<>();
            for (String caseCohortSample : caseCohortSamples) {
                if (controlCohortSamples.contains(caseCohortSample)) {
                    overlapping.add(caseCohortSample);
                }
            }
            throw new AnalysisException("Unable to run Gwas analysis with overlapping cohorts. "
                    + (overlapping.size() < 10
                        ? "Samples " + overlapping + " are shared between both cohorts."
                        : "There are " + overlapping.size() + " overlapping samples between the cohorts."));
        }

        // check read permission
        try {
            List<String> allSamples = new ArrayList<>();
            allSamples.addAll(caseCohortSamples);
            allSamples.addAll(controlCohortSamples);
            variantStorageManager.checkQueryPermissions(
                    new Query()
                            .append(VariantQueryParam.STUDY.key(), study)
                            .append(VariantQueryParam.INCLUDE_SAMPLE.key(), allSamples),
                    new QueryOptions(),
                    sessionId);
        } catch (CatalogException | StorageEngineException e) {
            throw new AnalysisException(e);
        }

        if (StringUtils.isNotEmpty(scoreName)) {
            if (StringUtils.isEmpty(caseCohort) || StringUtils.isEmpty(controlCohort)) {
                throw new AnalysisException("Unable to index gwas result as VariantScore if the cohorts are not defined in catalog");
            }

            // check score is not already indexed
            try {
                List<VariantScoreMetadata> scores = variantStorageManager.listVariantScores(study, sessionId);
                for (VariantScoreMetadata score : scores) {
                    if (score.getName().equals(scoreName)) {
                        if (score.getIndexStatus().equals(TaskMetadata.Status.READY)) {
                            throw new AnalysisException("Score name '" + scoreName + "' already exists in the database. "
                                    + "The score name must be unique.");
                        }
                    }
                }
            } catch (CatalogException | StorageEngineException e) {
                throw new AnalysisException(e);
            }

            // TODO: Check score index permissions

            index = true;
        }

        arm.updateResult(analysisResult ->
                analysisResult.getAttributes()
                        .append("index", index)
                        .append("scoreName", scoreName)
                        .append("caseCohort", caseCohort)
                        .append("caseCohortSamples", caseCohortSamples)
                        .append("controlCohort", controlCohort)
                        .append("controlCohortSamples", controlCohortSamples)
        );
    }

    @Override
    protected void exec() throws AnalysisException {
        Gwas gwas = new Gwas()
                .setConfiguration(gwasConfiguration)
                .setStudy(study)
                .setSampleList1(controlCohortSamples)
                .setSampleList2(caseCohortSamples);
        gwas.setUp(executorParams, outDir, sourceTypes, availableFrameworks);
        gwas.execute(arm);

        if (index) {
            arm.startStep("index-score", 80f);

            try {
                Path file = outDir.resolve(Paths.get(arm.read().getOutputFiles().get(0).getPath()));
                VariantScoreFormatDescriptor formatDescriptor = new VariantScoreFormatDescriptor(1, 16, 15);
                variantStorageManager.loadVariantScore(study, file.toUri(), scoreName, caseCohort, controlCohort, formatDescriptor, executorParams, sessionId);
            } catch (CatalogException | StorageEngineException e) {
                throw new AnalysisException(e);
            }
        }
        arm.endStep(100);
    }

    private List<String> getCohortSamples(String cohort, Query samplesQuery, String cohortType) throws AnalysisException {
        if (caseCohortSamplesQuery == null && StringUtils.isEmpty(caseCohort)) {
            throw new AnalysisException("Missing " + cohortType + " cohort!");
        }
        if (caseCohortSamplesQuery != null && !caseCohortSamplesQuery.isEmpty() && StringUtils.isNotEmpty(caseCohort)) {
            throw new AnalysisException("Provide either " + cohortType + " cohort name or " + cohortType + " cohort samples query,"
                    + " but not both.");
        }
        List<String> samples;
        try {
            if (StringUtils.isEmpty(cohort)) {
                samples = catalogManager.getSampleManager()
                        .get(study, samplesQuery, new QueryOptions(QueryOptions.INCLUDE, "id"), sessionId)
                        .getResult()
                        .stream()
                        .map(Sample::getId)
                        .collect(Collectors.toList());
            } else {
                samples = catalogManager.getCohortManager()
                        .get(study, cohort, new QueryOptions(), sessionId)
                        .first()
                        .getSamples()
                        .stream()
                        .map(Sample::getId)
                        .collect(Collectors.toList());
            }
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }
        if (samples.size() <= 1) {
            throw new AnalysisException("Unable to run GWAS analysis with " + cohortType + "cohort of size " + samples.size());
        }
        return samples;
    }

}
