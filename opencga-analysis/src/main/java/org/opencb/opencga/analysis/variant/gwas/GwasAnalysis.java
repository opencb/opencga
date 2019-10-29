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
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.analysis.result.FileResult;
import org.opencb.opencga.core.analysis.variant.GwasAnalysisExecutor;
import org.opencb.opencga.core.annotations.Analysis;
import org.opencb.opencga.core.annotations.Analysis.AnalysisType;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.metadata.models.VariantScoreMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.score.VariantScoreFormatDescriptor;
import org.opencb.oskar.analysis.variant.gwas.GwasConfiguration;

import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@Analysis(id = GwasAnalysis.ID, type = AnalysisType.VARIANT,
        description = GwasAnalysis.DESCRIPTION)
public class GwasAnalysis extends OpenCgaAnalysis {

    public static final String ID = "gwas";
    public static final String DESCRIPTION = "Run a Genome Wide Association Study between two cohorts.";

    private GwasConfiguration gwasConfiguration = new GwasConfiguration();
    private String study;
    private String phenotype;
    private String caseCohort;
    private String controlCohort;
    private Query caseCohortSamplesQuery;
    private Query controlCohortSamplesQuery;
    private List<String> caseCohortSamples;
    private List<String> controlCohortSamples;
    private String scoreName;
    private boolean index;
    private Path outputFile;

    public GwasAnalysis() {
    }

    /**
     * Provide the GWAS analysis configuration.
     * @param gwasConfiguration configuration
     * @return this
     */
    public GwasAnalysis setGwasConfiguration(GwasConfiguration gwasConfiguration) {
        this.gwasConfiguration = gwasConfiguration;
        return this;
    }

    public GwasAnalysis setGwasMethod(GwasConfiguration.Method method) {
        this.gwasConfiguration.setMethod(method);
        return this;
    }

    public GwasAnalysis setFisherMode(GwasConfiguration.FisherMode fisherMode) {
        this.gwasConfiguration.setFisherMode(fisherMode);
        return this;
    }

    /**
     * Study of the samples.
     * @param study Study id
     * @return this
     */
    public GwasAnalysis setStudy(String study) {
        this.study = study;
        return this;
    }

    /**
     * Use this phenotype to divide all the samples from the study.
     * Samples with the phenotype will be used as Case Cohort. Rest will be used as Control Cohort.
     *
     * This parameter can not be mixed with other parameters to define the cohorts.
     *
     * @param phenotype phenotype
     * @return this
     */
    public GwasAnalysis setPhenotype(String phenotype) {
        this.phenotype = phenotype;
        return this;
    }

    /**
     * Samples query selecting samples of the case cohort.
     * This parameter is an alternative to {@link #setCaseCohort}
     *
     * @param caseCohortSamplesQuery sample query
     * @return this
     */
    public GwasAnalysis setCaseCohortSamplesQuery(Query caseCohortSamplesQuery) {
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
    public GwasAnalysis setControlCohortSamplesQuery(Query controlCohortSamplesQuery) {
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
    public GwasAnalysis setCaseCohort(String caseCohort) {
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
    public GwasAnalysis setControlCohort(String controlCohort) {
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
    public GwasAnalysis setScoreName(String scoreName) {
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

        caseCohortSamples = getCohortSamples(caseCohort, caseCohortSamplesQuery, "case", true);
        controlCohortSamples = getCohortSamples(controlCohort, controlCohortSamplesQuery, "control", false);

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

        outputFile = getOutDir().resolve(buildOutputFilename());

        executorParams.append("index", index)
                .append("phenotype", phenotype)
                .append("scoreName", scoreName)
                .append("caseCohort", caseCohort)
                .append("caseCohortSamples", caseCohortSamples)
                .append("controlCohort", controlCohort)
                .append("controlCohortSamples", controlCohortSamples);
    }

    @Override
    public List<String> getSteps() {
        List<String> steps = super.getSteps();
        if (index) {
            steps.add("index");
        }
        return steps;
    }

    @Override
    protected void run() throws AnalysisException {

        step("gwas", () -> {
            GwasAnalysisExecutor gwasExecutor = getAnalysisExecutor(GwasAnalysisExecutor.class);

            gwasExecutor.setConfiguration(gwasConfiguration)
                    .setStudy(study)
                    .setSampleList1(caseCohortSamples)
                    .setSampleList2(controlCohortSamples)
                    .setOutputFile(outputFile)
                    .execute();

            if (outputFile.toFile().exists()) {
                addFile(outputFile, FileResult.FileType.TAB_SEPARATED);
            }
        });

//        step("manhattan-plot", this::createManhattanPlot);

        if (index) {
            step("index", () -> {
                try {
                    VariantScoreFormatDescriptor formatDescriptor = new VariantScoreFormatDescriptor(1, 16, 15);
                    variantStorageManager.loadVariantScore(study, outputFile.toUri(), scoreName, caseCohort, controlCohort, formatDescriptor,
                            executorParams, sessionId);
                } catch (CatalogException | StorageEngineException e) {
                    throw new AnalysisException(e);
                }
            });
        }
    }

    private void createManhattanPlot() throws AnalysisException {
    }

    protected String buildOutputFilename() throws AnalysisException {
        GwasConfiguration.Method method = gwasConfiguration.getMethod();
        switch (method) {
            case CHI_SQUARE_TEST:
            case FISHER_TEST:
                return method.label + ".tsv";
            default:
                throw new AnalysisException("Unknown GWAS method: " + method);
        }
    }

    private List<String> getCohortSamples(String cohort, Query samplesQuery, String cohortType, boolean observedPhenotype) throws AnalysisException {
        boolean validSampleQuery = samplesQuery != null && !samplesQuery.isEmpty();
        boolean validCohort = StringUtils.isNotEmpty(cohort);
        boolean validPhenotype = StringUtils.isNotEmpty(phenotype);
        if (validPhenotype) {
            if (validSampleQuery || validCohort) {
                throw new AnalysisException("Unable to mix phenotype parameter with " + cohortType + " cohort definition.");
            }
        } else {
            if (!validSampleQuery && !validCohort) {
                throw new AnalysisException("Missing " + cohortType + " cohort!");
            }
        }
        if (validSampleQuery && validCohort) {
            throw new AnalysisException("Provide either " + cohortType + " cohort name or " + cohortType + " cohort samples query,"
                    + " but not both.");
        }
        List<String> samples;
        try {
            if (validPhenotype) {
                Set<Phenotype.Status> expectedStatus = observedPhenotype
                        ? Collections.singleton(Phenotype.Status.OBSERVED)
                        : new HashSet<>(Arrays.asList(null, Phenotype.Status.NOT_OBSERVED));
                Query query;
                if (observedPhenotype) {
                    query = new Query(SampleDBAdaptor.QueryParams.PHENOTYPES_NAME.key(), phenotype);
                } else {
                    // If not observed, fetch all samples, and filter manually.
                    query = new Query();
                }
                QueryOptions options = new QueryOptions(
                        QueryOptions.INCLUDE, Arrays.asList(
                                SampleDBAdaptor.QueryParams.ID.key(),
                        SampleDBAdaptor.QueryParams.PHENOTYPES.key()));

                samples = new ArrayList<>();
                catalogManager.getSampleManager()
                        .iterator(study, query, options, sessionId)
                        .forEachRemaining(sample -> {
                            Phenotype.Status status = null;
                            for (Phenotype p : sample.getPhenotypes()) {
                                if (p.getId().equals(phenotype) || p.getName().equals(phenotype)) {
                                    status = p.getStatus();
                                }
                            }
                            if (expectedStatus.contains(status)) {
                                samples.add(sample.getId());
                            }
                        });
            } else if (validCohort) {
                samples = catalogManager.getCohortManager()
                        .get(study, cohort, new QueryOptions(), sessionId)
                        .first()
                        .getSamples()
                        .stream()
                        .map(Sample::getId)
                        .collect(Collectors.toList());
            } else {
                samples = catalogManager.getSampleManager()
                        .search(study, samplesQuery, new QueryOptions(QueryOptions.INCLUDE, "id"), sessionId)
                        .getResults()
                        .stream()
                        .map(Sample::getId)
                        .collect(Collectors.toList());
            }

            // Remove non-indexed samples
            Set<String> indexedSamples = variantStorageManager.getIndexedSamples(study, sessionId);
            samples.removeIf(s -> !indexedSamples.contains(s));
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }
        if (samples.size() <= 1) {
            throw new AnalysisException("Unable to run GWAS analysis with " + cohortType + " cohort of size " + samples.size());
        }
        return samples;
    }

}
