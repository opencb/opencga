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

package org.opencb.opencga.analysis.clinical.custom;

import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.DefaultReportedVariantCreator;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.analysis.clinical.ClinicalUtils;
import org.opencb.opencga.analysis.clinical.ClinicalInterpretationAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.tools.OpenCgaToolExecutor;
import org.opencb.opencga.core.tools.annotations.ToolExecutor;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.analysis.clinical.InterpretationAnalysis.PRIMARY_FINDINGS_FILENAME;
import static org.opencb.opencga.analysis.clinical.InterpretationAnalysis.SECONDARY_FINDINGS_FILENAME;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.FAMILY_SEGREGATION;

@ToolExecutor(id = "opencga-local",
        tool = CustomInterpretationAnalysis.ID,
        source = ToolExecutor.Source.STORAGE,
        framework = ToolExecutor.Framework.LOCAL)
public class CustomInterpretationAnalysisExecutor extends OpenCgaToolExecutor implements ClinicalInterpretationAnalysisExecutor {


    private String clinicalAnalysisId;
    private Query query;
    private QueryOptions queryOptions;
    private CustomInterpretationConfiguration config;

    private String sessionId;
    private ClinicalInterpretationManager clinicalInterpretationManager;

    private String studyId;
    private DefaultReportedVariantCreator reportedVariantCreator;

    public CustomInterpretationAnalysisExecutor() {
    }

    @Override
    public void run() throws ToolException {
        sessionId = getToken();
        clinicalInterpretationManager = getClinicalInterpretationManager();

        List<Variant> variants;
        List<ReportedVariant> reportedVariants;

        studyId = query.getString(VariantQueryParam.STUDY.key());

        String assembly;
        try {
            assembly = clinicalInterpretationManager.getAssembly(studyId, sessionId);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
        reportedVariantCreator = clinicalInterpretationManager.createReportedVariantCreator(query,
                assembly, queryOptions.getBoolean(ClinicalUtils.SKIP_UNTIERED_VARIANTS_PARAM), sessionId);

        ClinicalProperty.ModeOfInheritance moi = ClinicalProperty.ModeOfInheritance.UNKNOWN;
        if (query.containsKey(FAMILY_SEGREGATION.key())) {
            moi = ClinicalProperty.ModeOfInheritance.valueOf(query.getString(FAMILY_SEGREGATION.key()));
        }
        try {
            switch (moi) {
                case DE_NOVO:
                    variants = clinicalInterpretationManager.getDeNovoVariants(clinicalAnalysisId, studyId, query, queryOptions, sessionId);
                    reportedVariants = reportedVariantCreator.create(variants);
                    break;
                case COMPOUND_HETEROZYGOUS:
                    Map<String, List<Variant>> chVariants;
                    chVariants = clinicalInterpretationManager.getCompoundHeterozigousVariants(clinicalAnalysisId, studyId, query,
                            queryOptions, sessionId);
                    reportedVariants = ClinicalUtils.getCompoundHeterozygousReportedVariants(chVariants, reportedVariantCreator);
                    break;
                default:
                    VariantQueryResult<Variant> variantQueryResult = clinicalInterpretationManager.getVariantStorageManager()
                            .get(query, queryOptions, sessionId);
                    variants = variantQueryResult.getResults();
                    reportedVariants = reportedVariantCreator.create(variants);
                    break;
            }
        } catch (CatalogException | StorageEngineException | IOException | InterpretationAnalysisException e) {
            throw new ToolException("Error retrieving primary findings variants", e);
        }

        // Write primary findings
        ClinicalUtils.writeReportedVariants(reportedVariants, Paths.get(getOutDir() + "/" + PRIMARY_FINDINGS_FILENAME));

        // Get secondary findings
        try {
            variants = clinicalInterpretationManager.getSecondaryFindings(query.getString(VariantQueryParam.SAMPLE.key()),
                    clinicalAnalysisId, query.getString(VariantQueryParam.STUDY.key()), sessionId);
        } catch (CatalogException | IOException | StorageEngineException e) {
            throw new ToolException("Error retrieving secondary findings variants", e);
        }
        reportedVariants = reportedVariantCreator.create(variants);

        // Write secondary findings
        ClinicalUtils.writeReportedVariants(reportedVariants, Paths.get(getOutDir() + "/" + SECONDARY_FINDINGS_FILENAME));
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public CustomInterpretationAnalysisExecutor setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public Query getQuery() {
        return query;
    }

    public CustomInterpretationAnalysisExecutor setQuery(Query query) {
        this.query = query;
        return this;
    }

    public QueryOptions getQueryOptions() {
        return queryOptions;
    }

    public CustomInterpretationAnalysisExecutor setQueryOptions(QueryOptions queryOptions) {
        this.queryOptions = queryOptions;
        return this;
    }

    public CustomInterpretationConfiguration getConfig() {
        return config;
    }

    public CustomInterpretationAnalysisExecutor setConfig(CustomInterpretationConfiguration config) {
        this.config = config;
        return this;
    }
}
