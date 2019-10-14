package org.opencb.opencga.analysis.clinical.interpretation;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.tools.clinical.DefaultReportedVariantCreator;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.storage.core.manager.clinical.ClinicalUtils;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.oskar.analysis.OskarAnalysisExecutor;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.AnalysisExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

@AnalysisExecutor(id = "CustomInterpretation", analysis = "CustomInterpretation", source = AnalysisExecutor.Source.MONGODB,
        framework = AnalysisExecutor.Framework.ITERATOR)
public class CustomInterpretationAnalysisExecutor extends OskarAnalysisExecutor {

    private String clinicalAnalysisId;
    private String studyId;
    private Query query;
    private QueryOptions queryOptions;
    private String opencgaHome;
    private String sessionId;
    private CustomInterpretationConfiguration configuration;

    private CatalogManager catalogManager = null;
    private ClinicalInterpretationManager clinicalInterpretationManager = null;
    private VariantStorageManager variantStorageManager = null;

    public CustomInterpretationAnalysisExecutor() {
    }

    public CustomInterpretationAnalysisExecutor(ObjectMap executorParams, Path outDir, CustomInterpretationConfiguration configuration) {
        this.setup(executorParams, outDir, configuration);
    }

    protected void setup(ObjectMap executorParams, Path outDir, CustomInterpretationConfiguration configuration) {
        super.setup(executorParams, outDir);
        this.configuration = configuration;
    }

    @Override
    public void exec() throws AnalysisException {
        StopWatch watcher = StopWatch.createStarted();

        ClinicalAnalysis clinicalAnalysis = null;
        String probandSampleId = null;
        Disorder disorder = null;
        ClinicalProperty.ModeOfInheritance moi = null;

        List<String> biotypes = null;
        List<String> soNames = null;

        Map<String, List<File>> files = null;

        // We allow query to be empty, it is likely that we will add some filters from CA
        if (query == null) {
            query = new Query(VariantQueryParam.STUDY.key(), studyId);
        }

        String segregation = this.query.getString(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key());

        if (!query.containsKey(VariantQueryParam.STUDY.key())) {
            query.put(VariantQueryParam.STUDY.key(), studyId);
        }

        // Check clinical analysis (only when sample proband ID is not provided)
        if (clinicalAnalysisId != null) {
            QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = null;
            try {
                clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager().get(studyId,
                        clinicalAnalysisId, QueryOptions.empty(), sessionId);
            } catch (CatalogException e) {
                throw new AnalysisException(e);
            }
            if (clinicalAnalysisQueryResult.getNumResults() != 1) {
                throw new AnalysisException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyId);
            }

            clinicalAnalysis = clinicalAnalysisQueryResult.first();

            // Proband ID
            if (clinicalAnalysis.getProband() != null) {
                probandSampleId = clinicalAnalysis.getProband().getId();
            }

            // Family parameter
            if (clinicalAnalysis.getFamily() != null) {
                // Query contains a different family than ClinicAnalysis
                if (query.containsKey(VariantCatalogQueryUtils.FAMILY.key())
                        && !clinicalAnalysis.getFamily().getId().equals(query.get(VariantCatalogQueryUtils.FAMILY.key()))) {
                    throw new AnalysisException("Two families passed");
                } else {
                    query.put(VariantCatalogQueryUtils.FAMILY.key(), clinicalAnalysis.getFamily().getId());
                }
            } else {
                // Individual parameter
                if (clinicalAnalysis.getProband() != null) {
                    // Query contains a different sample than ClinicAnalysis
                    if (query.containsKey(VariantQueryParam.SAMPLE.key())
                            && !clinicalAnalysis.getProband().getId().equals(query.get(VariantQueryParam.SAMPLE.key()))) {
                        throw new AnalysisException("Two samples passed");
                    } else {
                        query.put(VariantQueryParam.SAMPLE.key(), clinicalAnalysis.getProband().getId());
                    }
                }
            }

            if (clinicalAnalysis.getFiles() != null && clinicalAnalysis.getFiles().size() > 0) {
                files = clinicalAnalysis.getFiles();
            }

            if (clinicalAnalysis.getDisorder() != null) {
                disorder = clinicalAnalysis.getDisorder();
                if (StringUtils.isNotEmpty(segregation) && disorder != null) {
                    query.put(VariantCatalogQueryUtils.FAMILY_DISORDER.key(), disorder.getId());
                    query.put(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key(), segregation);
                }
            }
        }

        // Check Query looks fine for Interpretation
        if (!query.containsKey(VariantQueryParam.GENOTYPE.key()) && !query.containsKey(VariantQueryParam.SAMPLE.key())) {
            // TODO check query is correct
        }

        // Get and check panels
        List<DiseasePanel> diseasePanels = null;
        if (query.get(VariantCatalogQueryUtils.PANEL.key()) != null) {
            try {
                diseasePanels = ClinicalUtils.getDiseasePanels(studyId, Arrays.asList(query.getString(VariantCatalogQueryUtils.PANEL.key())
                        .split(",")), catalogManager, sessionId);
            } catch (Exception e) {
                throw new AnalysisException(e);
            }
        }

        QueryOptions options = new QueryOptions(queryOptions);

        List<Variant> variants = new ArrayList<>();
        boolean skipDiagnosticVariants = options.getBoolean(ClinicalUtils.SKIP_DIAGNOSTIC_VARIANTS_PARAM, false);
        boolean skipUntieredVariants = options.getBoolean(ClinicalUtils.SKIP_UNTIERED_VARIANTS_PARAM, false);

        // Diagnostic variants ?
        if (!skipDiagnosticVariants) {
            List<DiseasePanel.VariantPanel> diagnosticVariants = ClinicalUtils.getDiagnosticVariants(diseasePanels);
            query.put(VariantQueryParam.ID.key(), StringUtils.join(diagnosticVariants.stream()
                    .map(DiseasePanel.VariantPanel::getId).collect(Collectors.toList()), ","));
        }

        int dbTime = -1;
        long numTotalResult = -1;

        if (StringUtils.isNotEmpty(segregation) && (segregation.equalsIgnoreCase(ClinicalProperty.ModeOfInheritance.DE_NOVO.toString())
                || segregation.equalsIgnoreCase(ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS.toString()))) {
            if (segregation.equalsIgnoreCase(ClinicalProperty.ModeOfInheritance.DE_NOVO.toString())) {
                StopWatch watcher2 = StopWatch.createStarted();
                moi = ClinicalProperty.ModeOfInheritance.DE_NOVO;
                try {
                    variants = clinicalInterpretationManager.getDeNovoVariants(clinicalAnalysisId, studyId, query, sessionId);
                } catch (Exception e) {
                    throw new AnalysisException(e);
                }
                dbTime = Math.toIntExact(watcher2.getTime());
            } else {
                moi = ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS;
            }
        } else {
            if (StringUtils.isNotEmpty(segregation)) {
                try {
                    moi = ClinicalProperty.ModeOfInheritance.valueOf(segregation);
                } catch (IllegalArgumentException e) {
                    moi = null;
                }
            }

            // Execute query
            VariantQueryResult<Variant> variantQueryResult;
            try {
                variantQueryResult = variantStorageManager.get(query, options, sessionId);
            } catch (Exception e) {
                throw new AnalysisException(e);
            }
            dbTime = variantQueryResult.getDbTime();
            numTotalResult = variantQueryResult.getNumTotalResults();

            if (CollectionUtils.isNotEmpty(variantQueryResult.getResult())) {
                variants.addAll(variantQueryResult.getResult());
            }

            if (CollectionUtils.isNotEmpty(variants)) {
                // Get biotypes and SO names
                if (query.containsKey(VariantQueryParam.ANNOT_BIOTYPE.key())
                        && StringUtils.isNotEmpty(query.getString(VariantQueryParam.ANNOT_BIOTYPE.key()))) {
                    biotypes = Arrays.asList(query.getString(VariantQueryParam.ANNOT_BIOTYPE.key()).split(","));
                }
                if (query.containsKey(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key())
                        && StringUtils.isNotEmpty(query.getString(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key()))) {
                    soNames = new ArrayList<>();
                    for (String soName : query.getString(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key()).split(",")) {
                        if (soName.startsWith("SO:")) {
                            try {
                                int soAcc = Integer.valueOf(soName.replace("SO:", ""));
                                soNames.add(ConsequenceTypeMappings.accessionToTerm.get(soAcc));
                            } catch (NumberFormatException e) {
                                throw new AnalysisException("Unknown SO term: " + soName);
                            }
                        } else {
                            soNames.add(soName);
                        }
                    }
                }
            }
        }

        // Primary findings and creator
        List<ReportedVariant> primaryFindings;
        DefaultReportedVariantCreator creator;
        String assembly = ClinicalUtils.getAssembly(catalogManager, studyId, sessionId);
        try {
            creator = new DefaultReportedVariantCreator(clinicalInterpretationManager.getRoleInCancerManager().getRoleInCancer(),
                    clinicalInterpretationManager.getActionableVariantManager().getActionableVariants(assembly), disorder, moi,
                    ClinicalProperty.Penetrance.COMPLETE, diseasePanels, biotypes, soNames, !skipUntieredVariants);
        } catch (IOException e) {
            throw new AnalysisException(e);
        }

        if (moi == ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS) {
            // Add compound heterozyous variants
            StopWatch watcher2 = StopWatch.createStarted();
            Map<String, List<Variant>> chVariants;
            try {
                chVariants = clinicalInterpretationManager.getCompoundHeterozigousVariants(clinicalAnalysisId,
                        studyId, query, sessionId);
                primaryFindings = ClinicalUtils.getCompoundHeterozygousReportedVariants(chVariants, creator);
            } catch (CatalogException | StorageEngineException | IOException | InterpretationAnalysisException e) {
                throw new AnalysisException(e);
            }
            dbTime = Math.toIntExact(watcher2.getTime());
        } else {
            // Other mode of inheritance
            primaryFindings = creator.create(variants);
        }

        // Secondary findings, if clinical consent is TRUE
        List<ReportedVariant> secondaryFindings = null;
        if (clinicalAnalysis != null) {
            try {
                secondaryFindings = clinicalInterpretationManager.getSecondaryFindings(clinicalAnalysis,
                        query.getAsStringList(VariantQueryParam.SAMPLE.key()), studyId, creator, sessionId);
            } catch (Exception e) {
                throw new AnalysisException(e);
            }
        }

        // Low coverage support
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();
        List<String> genes = new ArrayList<>();
        clinicalInterpretationManager.calculateLowCoverageRegions(studyId, genes, diseasePanels, ClinicalUtils.LOW_COVERAGE_DEFAULT,
                probandSampleId, files, reportedLowCoverages, sessionId);

        Interpretation interpretation = null;
        try {
            interpretation = clinicalInterpretationManager.generateInterpretation("custom-interpretation",
                    clinicalAnalysisId, query, primaryFindings, secondaryFindings, diseasePanels, reportedLowCoverages, sessionId);
        } catch (CatalogException e) {
            throw new AnalysisException(e);
        }

        int numberOfResults = primaryFindings != null ? primaryFindings.size() : 0;


        // Return interpretation result
        InterpretationResult interpretationResult = new InterpretationResult(
                interpretation,
                Math.toIntExact(watcher.getTime()),
                new HashMap<>(),
                dbTime,
                numberOfResults,
                numTotalResult,
                "",
                "");
    }
}
