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

package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections4.CollectionUtils;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.interpretation.ClinicalVariant;
import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.biodata.models.clinical.interpretation.Software;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.ConfigurationUtils;
import org.opencb.opencga.analysis.tools.OpenCgaTool;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.config.storage.StorageConfiguration;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.storage.core.StorageEngineFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

import static org.opencb.opencga.analysis.clinical.ClinicalUtils.readClinicalVariants;

public abstract class InterpretationAnalysis extends OpenCgaTool {

    public static String PRIMARY_FINDINGS_FILENAME = "primary-findings.json";
    public static String SECONDARY_FINDINGS_FILENAME = "secondary-findings.json";
    public static String INTERPRETATION_FILENAME = "interpretation.json";

    public final static String STUDY_PARAM_NAME = "study";
    public final static String CLINICAL_ANALYISIS_PARAM_NAME = "clinical-analysis";
    public static final String PANELS_PARAM_NAME = "panels";
    public static final String FAMILY_SEGREGATION_PARAM_NAME = "family-segregation";
    public static final String PENETRANCE_PARAM_NAME = "penetrance";
    public final static String DISCARDED_VARIANTS_PARAM_NAME = "discarded-variants";

    public final static String PRIMARY_INTERPRETATION_PARAM_NAME = "secondary";

    protected boolean primary;
    protected ClinicalInterpretationManager clinicalInterpretationManager;

    public InterpretationAnalysis() {
    }

    /**
     * Method to be implemented by subclasses with the actual method of the interpretation.
     *
     * @throws Exception on error
     */
    protected abstract InterpretationMethod getInterpretationMethod() throws Exception;

    protected InterpretationMethod getInterpretationMethod(String name) {
        InterpretationMethod method = new InterpretationMethod()
                .setName(name)
                .setDependencies(Collections.singletonList(new Software()
                        .setName("OpenCGA")
                        .setVersion(GitRepositoryState.get().getBuildVersion())
                        .setCommit(GitRepositoryState.get().getCommitId())));
        return method;
    }

    @Override
    protected void check() throws Exception {
        this.clinicalInterpretationManager = getClinicalInterpretationManager(getOpencgaHome().toString());
    }

    protected void checkPrimaryInterpretation(ClinicalAnalysis clinicalAnalysis) throws ToolException {
        if (primary && clinicalAnalysis.getPriority() != null) {
            throw new ToolException("Primary interpretation already exists");
        }
    }

    protected void checkInterpretationMethod(String methodName, ClinicalAnalysis clinicalAnalysis) throws ToolException {
        if (clinicalAnalysis.getInterpretation() != null && clinicalAnalysis.getInterpretation().getMethod() != null
                && methodName.equals(clinicalAnalysis.getInterpretation().getMethod().getName())) {
            throw new ToolException("Interpretation (primary) with method name '" + methodName + "' already exists");
        }

        if (CollectionUtils.isNotEmpty(clinicalAnalysis.getSecondaryInterpretations())) {
            for (Interpretation secondaryInterpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                if (secondaryInterpretation.getMethod() != null && methodName.equals(secondaryInterpretation.getMethod().getName())) {
                    throw new ToolException("Interpretation (secondary) with method name '" + methodName + "' already exists");
                }
            }
        }
    }

    protected void saveInterpretation(String studyId, ClinicalAnalysis clinicalAnalysis, Query query) throws ToolException {

        // Interpretation method
        InterpretationMethod method = new InterpretationMethod(getId(), null, null,
                Collections.singletonList(new Software().setName("OpenCGA").setVersion(GitRepositoryState.get().getBuildVersion())
                        .setCommit(GitRepositoryState.get().getCommitId())));

        // Analyst
        ClinicalAnalyst analyst = clinicalInterpretationManager.getAnalyst(token);

        List<ClinicalVariant> primaryFindings = readClinicalVariants(Paths.get(getOutDir().toString() + "/"
                + PRIMARY_FINDINGS_FILENAME));
        List<ClinicalVariant> secondaryFindings = readClinicalVariants(Paths.get(getOutDir().toString() + "/"
                + SECONDARY_FINDINGS_FILENAME));

        for (ClinicalVariant primaryFinding : primaryFindings) {
            primaryFinding.setFilters(query);
        }
        for (ClinicalVariant secondaryFinding : secondaryFindings) {
            secondaryFinding.setFilters(query);
        }

        org.opencb.biodata.models.clinical.interpretation.Interpretation interpretation = new Interpretation()
                .setPrimaryFindings(primaryFindings)
                .setSecondaryFindings(secondaryFindings)
                .setAnalyst(analyst)
                .setClinicalAnalysisId(clinicalAnalysis.getId())
                .setCreationDate(TimeUtils.getTime())
                .setMethod(method);

        // Store interpretation analysis in DB
        try {
            catalogManager.getInterpretationManager().create(studyId, clinicalAnalysis.getId(), new Interpretation(interpretation),
                    ParamUtils.SaveInterpretationAs.SECONDARY, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new ToolException("Error saving interpretation into database", e);
        }

        // Save interpretation analysis in JSON file
        Path path = getOutDir().resolve(INTERPRETATION_FILENAME);
        try {
            JacksonUtils.getDefaultObjectMapper().writer().writeValue(path.toFile(), interpretation);
        } catch (IOException e) {
            throw new ToolException(e);
        }
    }

    public static ClinicalInterpretationManager getClinicalInterpretationManager(String opencgaHome) throws ToolException {
        try {
            Configuration configuration = ConfigurationUtils.loadConfiguration(opencgaHome);
            StorageConfiguration storageConfiguration = ConfigurationUtils.loadStorageConfiguration(opencgaHome);

            CatalogManager catalogManager = new CatalogManager(configuration);
            StorageEngineFactory engineFactory = StorageEngineFactory.get(storageConfiguration);

            return new ClinicalInterpretationManager(catalogManager, engineFactory, Paths.get(opencgaHome));

        } catch (CatalogException | IOException e) {
            throw new ToolException(e);
        }
    }

    public boolean isPrimary() {
        return primary;
    }

    public InterpretationAnalysis setPrimary(boolean primary) {
        this.primary = primary;
        return this;
    }
}
