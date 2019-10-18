package org.opencb.opencga.analysis.clinical.interpretation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.tools.clinical.DefaultReportedVariantCreator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.storage.core.manager.clinical.ClinicalUtils;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.AnalysisExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.valueOf;
import static org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils.FAMILY_DISORDER;

@AnalysisExecutor(id = "CustomInterpretation", analysis = "CustomInterpretation", source = AnalysisExecutor.Source.MONGODB,
        framework = AnalysisExecutor.Framework.ITERATOR)
public class CustomInterpretationAnalysisExecutor extends OpenCgaAnalysisExecutor {

    public final static String SESSION_ID = "SESSION_ID";
    public final static String CLINICAL_INTERPRETATION_MANAGER = "CLINICAL_INTERPRETATION_MANAGER";

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

    public void setup(String clinicalAnalysisId, Query query, QueryOptions queryOptions, Path outDir, ObjectMap executorParams,
                      CustomInterpretationConfiguration config) throws AnalysisException {
        super.setup(executorParams, outDir);
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.query = query;
        this.queryOptions = queryOptions;
        this.config = config;

        // Sanity check
        sessionId = executorParams.getString(SESSION_ID, "");
        if (StringUtils.isEmpty(sessionId)) {
            throw new AnalysisException("Missing executor parameter: " + SESSION_ID);
        }
        clinicalInterpretationManager = (ClinicalInterpretationManager) executorParams.getOrDefault(CLINICAL_INTERPRETATION_MANAGER, null);
        if (clinicalInterpretationManager == null) {
            throw new AnalysisException("Missing executor parameter: " + CLINICAL_INTERPRETATION_MANAGER);
        }
    }

    @Override
    public void exec() throws AnalysisException {
        List<Variant> variants;
        List<ReportedVariant> reportedVariants;

        studyId = query.getString(VariantQueryParam.STUDY.key());
        reportedVariantCreator = createReportedVariantCreator();

        ClinicalProperty.ModeOfInheritance moi = valueOf(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key());
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
                    VariantQueryResult<Variant> variantQueryResult = variantStorageManager.get(query, queryOptions, sessionId);
                    variants = variantQueryResult.getResult();
                    reportedVariants = reportedVariantCreator.create(variants);
                    break;
            }
        } catch (CatalogException | StorageEngineException | IOException | InterpretationAnalysisException e) {
            throw new AnalysisException("Error retrieving primary findings variants", e);
        }

        // Write primary findings
        ClinicalUtils.writeReportedVariants(reportedVariants, Paths.get(outDir + "/primary-findings.json"));

        // Get secondary findings
        try {
            variants = clinicalInterpretationManager.getSecondaryFindings(query.getString(VariantQueryParam.SAMPLE.key()),
                    clinicalAnalysisId, query.getString(VariantQueryParam.STUDY.key()), sessionId);
        } catch (CatalogException | IOException | StorageEngineException e) {
            throw new AnalysisException("Error retrieving secondary findings variants", e);
        }
        reportedVariants = reportedVariantCreator.create(variants);

        // Write secondary findings
        ClinicalUtils.writeReportedVariants(reportedVariants, Paths.get(outDir + "/secondary-findings.json"));
    }

    private DefaultReportedVariantCreator createReportedVariantCreator() throws AnalysisException {
        // Reported variant creator
        ClinicalProperty.ModeOfInheritance moi = ClinicalProperty.ModeOfInheritance.valueOf(query.getString(FAMILY_DISORDER.key(),
                ClinicalProperty.ModeOfInheritance.UNKNOWN.name()));
        String assembly;
        try {
            assembly = clinicalInterpretationManager.getAssembly(studyId, sessionId);
        } catch (CatalogException e) {
            throw new AnalysisException("Error retrieving assembly", e);
        }
        List<String> biotypes = query.getAsStringList(VariantQueryParam.ANNOT_BIOTYPE.key());
        List<String> soNames = new ArrayList<>();
        List<String>  consequenceTypes = query.getAsStringList(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key());
        if (CollectionUtils.isNotEmpty(consequenceTypes)) {
            for (String soName : consequenceTypes) {
                if (soName.startsWith("SO:")) {
                    try {
                        int soAcc = Integer.valueOf(soName.replace("SO:", ""));
                        soNames.add(ConsequenceTypeMappings.accessionToTerm.get(soAcc));
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                } else {
                    soNames.add(soName);
                }
            }
        }
        try {
            Disorder disorder = new Disorder().setId(query.getString(VariantCatalogQueryUtils.FAMILY_DISORDER.key()));
            List<DiseasePanel> diseasePanels = clinicalInterpretationManager.getDiseasePanels(query, sessionId);
            return new DefaultReportedVariantCreator(clinicalInterpretationManager.getRoleInCancerManager().getRoleInCancer(),
                    clinicalInterpretationManager.getActionableVariantManager().getActionableVariants(assembly), disorder, moi,
                    ClinicalProperty.Penetrance.COMPLETE, diseasePanels, biotypes, soNames, !config.isSkipUntieredVariants());
        } catch (IOException e) {
            throw new AnalysisException("Error creating reported variant creator", e);
        }
    }
}
