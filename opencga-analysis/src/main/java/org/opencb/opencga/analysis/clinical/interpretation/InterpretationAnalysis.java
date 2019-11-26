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

package org.opencb.opencga.analysis.clinical.interpretation;

import org.apache.commons.collections.CollectionUtils;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.analysis.result.FileResult;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.exception.AnalysisException;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.config.StorageConfiguration;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static org.opencb.opencga.analysis.clinical.ClinicalUtils.readReportedVariants;

public abstract class InterpretationAnalysis extends OpenCgaAnalysis {


    public static String PRIMARY_FINDINGS_FILENAME = "primary-findings.json";
    public static String SECONDARY_FINDINGS_FILENAME = "secondary-findings.json";
    public static String INTERPRETATION_FILENAME = "interpretation.json";

    public final static String STUDY_PARAM_NAME = "study-id";
    public final static String CLINICAL_ANALYISIS_PARAM_NAME = "clinical-analysis-id";
    public static final String PANELS_PARAM_NAME = "panel-ids";
    public static final String FAMILY_SEGREGATION_PARAM_NAME = "family-segregation";
    public static final String PENETRANCE_PARAM_NAME = "penetrance";
    public final static String VARIANTS_TO_DISCARD_PARAM_NAME = "variant-ids-to-discard";

    public final static String MAX_LOW_COVERAGE_PARAM_NAME = "max-low-coverage";
    public final static String INCLUDE_LOW_COVERAGE_PARAM_NAME = "include-low-coverage";
    public final static String INCLUDE_UNTIERED_VARIANTS_PARAM_NAME = "include-untiered-variants";

    protected ClinicalInterpretationManager clinicalInterpretationManager;

    public InterpretationAnalysis() {
    }

    public void setUp(String opencgaHome, ObjectMap params, Path outDir, String token)
            throws AnalysisException {
        super.setUp(opencgaHome, params, outDir, token);
        this.clinicalInterpretationManager = getClinicalInterpretationManager(opencgaHome);
    }

    protected void saveInterpretation(String studyId, ClinicalAnalysis clinicalAnalysis, List<DiseasePanel> diseasePanels, Query query,
                                      InterpretationAnalysisConfiguration config) throws AnalysisException {

        // Software
        Software software = new Software().setName(getId());

        // Analyst
        Analyst analyst = clinicalInterpretationManager.getAnalyst(token);

        List<ReportedLowCoverage> reportedLowCoverages = null;
        if (config.isIncludeLowCoverage() && CollectionUtils.isNotEmpty(diseasePanels)) {
            reportedLowCoverages = (clinicalInterpretationManager.getReportedLowCoverage(config.getMaxLowCoverage(), clinicalAnalysis,
                    diseasePanels, studyId, token));
        }

        List<ReportedVariant> primaryFindings = readReportedVariants(Paths.get(getOutDir().toString() + "/"
                + PRIMARY_FINDINGS_FILENAME));
        List<ReportedVariant> secondaryFindings = readReportedVariants(Paths.get(getOutDir().toString() + "/"
                + SECONDARY_FINDINGS_FILENAME));

        Interpretation interpretation = new Interpretation()
                .setId(getId() + "__" + TimeUtils.getTimeMillis())
                .setPrimaryFindings(primaryFindings)
                .setSecondaryFindings(secondaryFindings)
                .setLowCoverageRegions(reportedLowCoverages)
                .setAnalyst(analyst)
                .setClinicalAnalysisId(clinicalAnalysis.getId())
                .setCreationDate(TimeUtils.getTime())
                .setPanels(diseasePanels)
                .setFilters(query)
                .setSoftware(software);

        // Store interpretation analysis in DB
        try {
            catalogManager.getInterpretationManager().create(studyId, clinicalAnalysis.getId(), new Interpretation(interpretation),
                    QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new AnalysisException("Error saving interpretation into database", e);
        }

        // Save interpretation analysis in JSON file
        Path path = Paths.get(getOutDir().toAbsolutePath() + "/" + INTERPRETATION_FILENAME);
        try {
            JacksonUtils.getDefaultObjectMapper().writer().writeValue(path.toFile(), interpretation);
            addFile(path, FileResult.FileType.JSON);
        } catch (IOException e) {
            throw new AnalysisException(e);
        }
    }

    public static ClinicalInterpretationManager getClinicalInterpretationManager(String opencgaHome) throws AnalysisException {
        try {
            Configuration configuration = ConfigurationUtils.loadConfiguration(opencgaHome);
            StorageConfiguration storageConfiguration = ConfigurationUtils.loadStorageConfiguration(opencgaHome);

            CatalogManager catalogManager = new CatalogManager(configuration);
            StorageEngineFactory engineFactory = StorageEngineFactory.get(storageConfiguration);

            return new ClinicalInterpretationManager(catalogManager, engineFactory,
                    Paths.get(opencgaHome + "/analysis/resources/roleInCancer.txt"),
                    Paths.get(opencgaHome + "/analysis/resources/"));

        } catch (CatalogException | IOException e) {
            throw new AnalysisException(e);
        }
    }

}
