package org.opencb.opencga.analysis.clinical.interpretation;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ClinVar;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.analysis.OpenCgaAnalysisExecutor;
import org.opencb.opencga.catalog.db.api.PanelDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Individual;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.Sample;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.clinical.ClinicalInterpretationManager;
import org.opencb.opencga.storage.core.manager.clinical.ClinicalUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.oskar.analysis.exceptions.AnalysisException;
import org.opencb.oskar.core.annotations.AnalysisExecutor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static org.opencb.biodata.formats.variant.clinvar.v24jaxb.ReviewStatusType.*;
import static org.opencb.biodata.models.clinical.interpretation.VariantClassification.*;

@AnalysisExecutor(id = "CancerTieringInterpretation", analysis = "CancerTieringInterpretation", source = AnalysisExecutor.Source.MONGODB,
        framework = AnalysisExecutor.Framework.ITERATOR)
public class CancerTieringInterpretationAnalysisExecutor extends OpenCgaAnalysisExecutor {

    public final static String ID = "CancerTieringInterpretationAnalysis";

    public final static String SESSION_ID = "SESSION_ID";
    public final static String CLINICAL_INTERPRETATION_MANAGER = "CLINICAL_INTERPRETATION_MANAGER";

    private String clinicalAnalysisId;
    private String studyId;
    private List<String> variantIdsToDiscard;
    private CancerTieringInterpretationConfiguration config;

    private String sessionId;
    private ClinicalInterpretationManager clinicalInterpretationManager;

    private static final String OTHER_PHENOTYPE = "Other";
    private static final String CHILDHOOD_PHENOTYPE = "Childhood";
    private static final String HAEMONC_PHENOTYPE = "Haemonc";

    private static final String ADULT_PANEL_NAME = "Adult solid tumours cancer susceptibility";
    private static final String CHILDHOOD_PANEL_NAME = "Childhood solid tumours cancer susceptibility";
    private static final String HAEMONC_PANEL_NAME = "Haematological malignancies cancer susceptibility";

    private static final Set<String> somaticSOTerms = new HashSet<>(Arrays.asList("SO:0001893","SO:0001574","SO:0001575","SO:0001587","SO:0001589",
            "SO:0001578", "SO:0001582","SO:0002012","SO:0001889","SO:0001821","SO:0001822","SO:0001583","SO:0001630","SO:0001650",
            "SO:0001626"));

    private static final String CONSEQUENCE_RNA = "SO:0001792";

    private static final Set<String> lofSOTerms = new HashSet<>(Arrays.asList("SO:0001893","SO:0001574","SO:0001575","SO:0001587","SO:0001589",
            "SO:0001578", "SO:0001582","SO:0002012"));

    private static final List<String> pertinentFindingRNA = new ArrayList(Arrays.asList("ENSG00000269900", "ENSG00000270141"));

    public void setup(String clinicalAnalysisId, String studyId, List<String> variandIdsToDiscard, Path outDir, ObjectMap executorParams,
                      CancerTieringInterpretationConfiguration config) throws AnalysisException {
        super.setup(executorParams, outDir);
        this.clinicalAnalysisId = clinicalAnalysisId;
        this.studyId = studyId;
        this.variantIdsToDiscard = variandIdsToDiscard;
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
        // Get clinical analysis
        ClinicalAnalysis clinicalAnalysis = null;
        try {
            clinicalAnalysis = clinicalInterpretationManager.getClinicalAnalysis(studyId, clinicalAnalysisId, sessionId);
        } catch (CatalogException e) {
            throw new AnalysisException("Error accessing to the clinical analysis ID: " + clinicalAnalysisId, e);
        }

        Disorder disorder = clinicalAnalysis.getDisorder();
        if (disorder == null || StringUtils.isEmpty(disorder.getId())) {
            throw new AnalysisException("Missing disorder for clinical analysis " + clinicalAnalysisId);
        }

        // Primary findings consist of reported somatic and germline variants
        List<ReportedVariant> primaryFindings = new ArrayList<>();

        // Get somatic sample and then its reported variant
        List<Sample> somaticSamples = getSomaticSamples(clinicalAnalysis.getProband());
        if (CollectionUtils.isNotEmpty(somaticSamples)) {
            if (somaticSamples.size() != 1) {
                throw new AnalysisException("Found multiple somatic samples (" + somaticSamples.size() + "), only one is permitted.");
            }
            List<ReportedVariant> reportedSomaticVariants = null;
            try {
                reportedSomaticVariants = getReportedSomaticVariants(somaticSamples);
            } catch (CatalogException | IOException | StorageEngineException e) {
                throw new AnalysisException("Error retrieving somatic variants", e);
            }
            primaryFindings.addAll(reportedSomaticVariants);
        }

        // Get germline sample and then its reported variant
        List<Sample> germlineSamples = getGermlineSamples(clinicalAnalysis.getProband());
        if (CollectionUtils.isNotEmpty(somaticSamples)) {
            if (somaticSamples.size() != 1) {
                throw new AnalysisException("Found multiple germline samples(" + germlineSamples.size() + "), only one is permitted.");
            }
            // Get panels from that clinical analysis disorder
            List<String> phenotypes = new ArrayList<>();
            for (Phenotype evidence : disorder.getEvidences()) {
                if (StringUtils.isNotEmpty(evidence.getId())) {
                    phenotypes.add(evidence.getId());
                }
                if (StringUtils.isNotEmpty(evidence.getName())) {
                    phenotypes.add(evidence.getName());
                }
            }
            List<ReportedVariant> reportedGermlineVariants = null;
            try {
                reportedGermlineVariants = getReportedGermlineVariants(clinicalAnalysis.getProband(), germlineSamples,
                        phenotypes);
            } catch (CatalogException | IOException | StorageEngineException e) {
                throw new AnalysisException("Error retrieving germline variants", e);
            }
            primaryFindings.addAll(reportedGermlineVariants);
        }

        // Write primary findings
        ClinicalUtils.writeReportedVariants(primaryFindings, Paths.get(outDir + "/primary-findings.json"));
    }

    //-------------------------------------------------------------------------
    // P R I V A T E     M E T H O D S
    //-------------------------------------------------------------------------

    private List<Sample> getSomaticSamples(Individual proband) throws AnalysisException {
        if (proband == null) {
            throw new AnalysisException("Missing proband when retrieving somatic samples");
        }

        List<Sample> samples = new ArrayList<>();
        for (Sample sample : proband.getSamples()) {
            if (sample.isSomatic()) {
                samples.add(sample);
            }
        }
        return samples;
    }

    private List<Sample> getGermlineSamples(Individual proband) throws AnalysisException {
        if (proband == null) {
            throw new AnalysisException("Missing proband when retrieving germline samples");
        }

        List<Sample> samples = new ArrayList<>();
        for (Sample sample : proband.getSamples()) {
            if (!sample.isSomatic()) {
                samples.add(sample);
            }
        }
        return samples;
    }

    private List<ReportedVariant> getReportedSomaticVariants(List<Sample> somaticSamples)
            throws CatalogException, IOException, StorageEngineException {
        List<ReportedVariant> reportedVariants = new ArrayList<>();

        Query query = new Query();
        query.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.")
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), somaticSOTerms);

        // Sample and genotype managenement
        StringBuilder gt = new StringBuilder();
        for (Sample sample : somaticSamples) {
            if (gt.length() > 0) {
                gt.append(",");
            }
            gt.append(sample.getId() + ":!0/0;!./.;!./0");
        }
        query.put(VariantQueryParam.GENOTYPE.key(), gt.toString());

        // Execute query
        VariantQueryResult<Variant> variantVariantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), sessionId);

        if (CollectionUtils.isNotEmpty(variantVariantQueryResult.getResult())) {
            Map<String, ClinicalProperty.RoleInCancer> roleInCancer = clinicalInterpretationManager.getRoleInCancerManager()
                    .getRoleInCancer();
            for (Variant variant : variantVariantQueryResult.getResult()) {
                if (variant.getAnnotation() != null && CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                    List<ReportedEvent> reportedEvents = new ArrayList<>();
                    for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                        List<SequenceOntologyTerm> somaticSOTerms = getSomaticSequenceOntologyTerms(ct);
                        if (roleInCancer.containsKey(ct.getEnsemblGeneId()) || roleInCancer.containsKey(ct.getGeneName())) {
                            // Create reported event with TIER 2
                            ReportedEvent reportedEvent = new ReportedEvent();

                            reportedEvent.setGenomicFeature(new GenomicFeature(ct.getEnsemblGeneId(), "GENE",
                                    ct.getEnsemblTranscriptId(), ct.getGeneName(), null));
                            reportedEvent.setConsequenceTypes(somaticSOTerms);
                            VariantClassification classification = new VariantClassification();
                            classification.setTier(TIER_2);
                            List<String> acmg = calculateAcmgClassification(variant);
                            classification.setAcmg(acmg);
                            classification.setClinicalSignificance(computeClinicalSignificance(acmg));
                            reportedEvent.setClassification(classification);

                            reportedEvents.add(reportedEvent);
                        }
                    }
                    if (CollectionUtils.isNotEmpty(reportedEvents)) {
                        ReportedVariant reportedVariant = new ReportedVariant(variant.getImpl());
                        reportedVariant.setEvidences(reportedEvents);
                    }
                }
            }
        }

        // TODO: Mark reported variant as somatic
        return reportedVariants;
    }

    private List<ReportedVariant> getReportedGermlineVariants(Individual proband, List<Sample> germlineSamples, List<String> phenotypes)
            throws CatalogException, IOException, StorageEngineException {
        Set<DiseasePanel> diseasePanelSet = new HashSet<>();
        List<ReportedVariant> reportedVariants = new ArrayList<>();

        // Germline tiering: TIER 1

        Query query = new Query();
        query.append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), studyId)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");

        // Sample and genotype managenement
        StringBuilder gt = new StringBuilder();
        for (Sample sample : germlineSamples) {
            if (gt.length() > 0) {
                gt.append(",");
            }
            gt.append(sample.getId() + ":!0/0;!./.;!./0");
        }
        query.put(VariantQueryParam.GENOTYPE.key(), gt.toString());

        // Get disease panels from disorder according to Tier 1, and then get genes involved: these genes will be queried
        List<DiseasePanel> panels = getTier1GenePanels(proband, phenotypes);
        diseasePanelSet.addAll(panels);
        List<String> geneIds = ClinicalUtils.getGeneIds(panels);
        // We add the pertinent findings RNA
        geneIds.addAll(pertinentFindingRNA);
        query.put(VariantQueryParam.GENE.key(), geneIds);

        // Execute query
        VariantQueryResult<Variant> variantVariantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), sessionId);

        // Discard variants if:
        //    1) it is present in the black list
        //    2) or its allelic depth (AD) is 1,0
        List<Variant> variants = filterVariants(variantVariantQueryResult.getResult(), germlineSamples);

        // Create reported variants if necessary
        Set<String> reportedVariantIdSet = new HashSet<>();
        for (Variant variant : variants) {
            List<ReportedEvent> reportedEvents = new ArrayList<>();
            List<ClinVar> clinVars = getClinVars(variant);

            List<DiseasePanel> varDiseasePanels = getDiseasePanels(variant, panels);
            if (CollectionUtils.isNotEmpty(varDiseasePanels)) {
                for (DiseasePanel varDiseasePanel : varDiseasePanels) {
                    if (CollectionUtils.isNotEmpty(varDiseasePanel.getGenes())) {
                        for (DiseasePanel.GenePanel panelGene : varDiseasePanel.getGenes()) {
                            if (isLoF(panelGene) && variant.getAnnotation() != null
                                    && CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                                for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                                    List<SequenceOntologyTerm> lofSOTerms = getLoFSequenceOntologyTerms(consequenceType);
                                    if (CollectionUtils.isNotEmpty(lofSOTerms)) {
                                        if (CollectionUtils.isEmpty(clinVars) || !isBenign(clinVars)) {
                                            reportedEvents.add(createrReportEvent(varDiseasePanel, panelGene, lofSOTerms,
                                                    computeBiallelicTiering(panelGene, variant), consequenceType, variant));
                                        }
                                    } else {
                                        if (CollectionUtils.isNotEmpty(clinVars) && isPathogenic(clinVars)) {
                                            reportedEvents.add(createrReportEvent(varDiseasePanel, panelGene, lofSOTerms,
                                                    computeBiallelicTiering(panelGene, variant), consequenceType, variant));
                                        }
                                    }
                                }
                            } else {
                                if (CollectionUtils.isNotEmpty(clinVars) && isPathogenic(clinVars)) {
                                    for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                                        List<SequenceOntologyTerm> lofSOTerms = getLoFSequenceOntologyTerms(consequenceType);
                                        reportedEvents.add(createrReportEvent(varDiseasePanel, panelGene, lofSOTerms,
                                                computeBiallelicTiering(panelGene, variant), consequenceType, variant));
                                    }
                                }
                            }
                        }
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(reportedEvents)) {
                ReportedVariant reportedVariant = new ReportedVariant(variant.getImpl());
                reportedVariant.setEvidences(reportedEvents);

                // We must save these variants to discard them in the next processing step (germline tiering TIER 3)
                reportedVariantIdSet.add(variant.getId());
            }
        }

        // Germline tiering: TIER 3

        panels = getTier3GenePanels(proband, phenotypes);
        diseasePanelSet.addAll(panels);
        geneIds = ClinicalUtils.getGeneIds(panels);
        // We add the pertinent findings RNA to the gene list
        geneIds.addAll(pertinentFindingRNA);
        // Overwrite genes in the query (previous filters are fine)
        query.put(VariantQueryParam.GENE.key(), geneIds);
        query.put(VariantQueryParam.STATS_MAF.key(), "ALL<0.02");

        // Execute query
        variantVariantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), sessionId);

        // Discard variants if:
        //    1) it is present in the black list
        //    2) or its allelic depth (AD) is 1,0
        variants = filterVariants(variantVariantQueryResult.getResult(), germlineSamples);

        for (Variant variant : variants) {
            // Discard variant already reported
            if (reportedVariantIdSet.contains(variant.getId())) {
                continue;
            }

            List<ReportedEvent> reportedEvents = new ArrayList<>();
            List<ClinVar> clinVars = getClinVars(variant);

            List<DiseasePanel> varDiseasePanels = getDiseasePanels(variant, panels);
            if (CollectionUtils.isNotEmpty(varDiseasePanels)) {
                for (DiseasePanel varDiseasePanel : varDiseasePanels) {
                    if (CollectionUtils.isNotEmpty(varDiseasePanel.getGenes())) {
                        for (DiseasePanel.GenePanel panelGene : varDiseasePanel.getGenes()) {
                            if (variant.getAnnotation() != null
                                    && CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                                for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                                    // Get somatic SO terms and RNA SO term
                                    List<SequenceOntologyTerm> soTerms = new ArrayList<>();
                                    if (CollectionUtils.isNotEmpty(consequenceType.getSequenceOntologyTerms())) {
                                        for (SequenceOntologyTerm soTerm : consequenceType.getSequenceOntologyTerms()) {
                                            if (somaticSOTerms.contains(soTerm.getAccession())
                                                    || CONSEQUENCE_RNA.equals(soTerm.getAccession())) {
                                                soTerms.add(soTerm);
                                            }
                                        }
                                    }
                                    if (CollectionUtils.isNotEmpty(soTerms)) {
                                        if (CollectionUtils.isEmpty(clinVars) || !isBenign(clinVars)) {
                                            if ("BIALLELIC".equals(panelGene.getModeOfInheritance())) {
                                                reportedEvents.add(createrReportEvent(varDiseasePanel, panelGene, soTerms, TIER_3,
                                                        consequenceType, variant));
                                            } else {
                                                if (CollectionUtils.isNotEmpty(variant.getStudies())) {
                                                    VariantStats stats = variant.getStudies().get(0).getStats("ALL");
                                                    if (stats != null && stats.getMaf() < 0.005f) {
                                                        reportedEvents.add(createrReportEvent(varDiseasePanel, panelGene, soTerms, TIER_3,
                                                                consequenceType, variant));
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            if (CollectionUtils.isNotEmpty(reportedEvents)) {
                ReportedVariant reportedVariant = new ReportedVariant(variant.getImpl());
                reportedVariant.setEvidences(reportedEvents);
            }
        }

        return reportedVariants;
    }

    private ReportedEvent createrReportEvent(DiseasePanel diseasePanel, DiseasePanel.GenePanel panelGene,
                                             List<SequenceOntologyTerm> soTerms, String tier, ConsequenceType consequenceType,
                                             Variant variant) {
        ReportedEvent reportedEvent = new ReportedEvent();

        reportedEvent.setPanelId(diseasePanel.getId());
        reportedEvent.setGenomicFeature(new GenomicFeature(consequenceType.getEnsemblGeneId(), "GENE",
                consequenceType.getEnsemblTranscriptId(), consequenceType.getGeneName(), panelGene.getXrefs()));
        reportedEvent.setConsequenceTypes(soTerms);
        VariantClassification classification = new VariantClassification();
        classification.setTier(tier);
        classification.setAcmg(calculateAcmgClassification(variant));
        classification.setClinicalSignificance(computeClinicalSignificance(classification.getAcmg()));
        reportedEvent.setClassification(classification);

        return reportedEvent;
    }

    private String computeBiallelicTiering(DiseasePanel.GenePanel panelGene, Variant variant) {
        if ("BIALLELIC".equals(panelGene.getModeOfInheritance())) {
            if (CollectionUtils.isNotEmpty(variant.getStudies()) && CollectionUtils.isNotEmpty(variant.getStudies().get(0).getSamplesData())
                    && CollectionUtils.isNotEmpty(variant.getStudies().get(0).getSamplesData().get(0))) {
                int counter01 = 0;
                int counter11 = 0;
                for (String gt : variant.getStudies().get(0).getSamplesData().get(0)) {
                    if ("0/1".equals(gt)) {
                        if (++counter01 >= 2) {
                            return TIER_1;
                        }
                    } else if ("1/1".equals(gt)) {
                        if (++counter11 >= 1) {
                            return TIER_1;
                        }
                    }
                }
            }
            return TIER_3;
        } else {
            return TIER_1;
        }
    }

    private boolean isPathogenic(List<ClinVar> clinVar) {
        for (ClinVar cv : clinVar) {
            String clinicalSignificance = cv.getClinicalSignificance();
            String reviewStatus = cv.getReviewStatus();
            if (("Pathogenic".equals(clinicalSignificance) || "Likely Pathogenic".equals(clinicalSignificance))
                    && (PRACTICE_GUIDELINE.name().equals(reviewStatus)
                    || REVIEWED_BY_EXPERT_PANEL.name().equals(reviewStatus)
                    || CRITERIA_PROVIDED_MULTIPLE_SUBMITTERS_NO_CONFLICTS.name().equals(reviewStatus))) {
                return true;
            }
        }
        return false;
    }

    private boolean isBenign(List<ClinVar> clinVar) {
        for (ClinVar cv : clinVar) {
            String clinicalSignificance = cv.getClinicalSignificance();
            String reviewStatus = cv.getReviewStatus();
            if (("Benign".equals(clinicalSignificance) || "Likely Benign".equals(clinicalSignificance))
                    && (PRACTICE_GUIDELINE.name().equals(reviewStatus)
                    || REVIEWED_BY_EXPERT_PANEL.name().equals(reviewStatus)
                    || CRITERIA_PROVIDED_MULTIPLE_SUBMITTERS_NO_CONFLICTS.name().equals(reviewStatus))) {
                return true;
            }
        }
        return false;
    }

    private boolean isLoF(DiseasePanel.GenePanel panelGene) {
        // TODO: GenePanel class needs a new attribute in order to know if that gene is LoF
        return true;
    }

    private List<SequenceOntologyTerm> getLoFSequenceOntologyTerms(ConsequenceType consequenceType) {
        Set<SequenceOntologyTerm> soTermSet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(consequenceType.getSequenceOntologyTerms())) {
            for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                if (lofSOTerms.contains(sequenceOntologyTerm.getAccession())) {
                    soTermSet.add(sequenceOntologyTerm);
                }
            }
        }
        return new ArrayList<>(soTermSet);
    }

    private List<SequenceOntologyTerm> getSomaticSequenceOntologyTerms(ConsequenceType consequenceType) {
        Set<SequenceOntologyTerm> soTermSet = new HashSet<>();
        if (CollectionUtils.isNotEmpty(consequenceType.getSequenceOntologyTerms())) {
            for (SequenceOntologyTerm sequenceOntologyTerm : consequenceType.getSequenceOntologyTerms()) {
                if (somaticSOTerms.contains(sequenceOntologyTerm.getAccession())) {
                    soTermSet.add(sequenceOntologyTerm);
                }
            }
        }
        return new ArrayList<>(soTermSet);
    }

    private List<DiseasePanel> getTier1GenePanels(Individual proband, List<String> phenotypes) throws CatalogException {
        List<DiseasePanel> panels = new ArrayList<>();

        boolean other = false;
        for (String phenotype: phenotypes) {
            if (OTHER_PHENOTYPE.equals(phenotype)) {
                other = true;
                break;
            }
        }

        if (CollectionUtils.isNotEmpty(phenotypes)) {
            Query query = new Query();
            query.put(PanelDBAdaptor.QueryParams.PHENOTYPES_NAME.key(), phenotypes);
            addPanels(query, panels);
        }

        if (other || CollectionUtils.isEmpty(phenotypes)) {
            // No phenotypes or other, we add the ADULT panel
            Query query = new Query(PanelDBAdaptor.QueryParams.NAME.key(), ADULT_PANEL_NAME);
            addPanels(query, panels);
        }

        // TODO: check if proband is younger than 18 years old in order to add CHILDHOOD panel

        return panels;
    }

    private List<DiseasePanel> getTier3GenePanels(Individual proband, List<String> phenotypes) throws CatalogException {
        List<DiseasePanel> panels = new ArrayList<>();

        boolean childhood = false;
        boolean haemonc = false;
        for (String phenotype: phenotypes) {
            if (CHILDHOOD_PHENOTYPE.equals(phenotype)) {
                childhood = true;
            } else if (HAEMONC_PHENOTYPE.equals(phenotype)) {
                haemonc = true;
            }
        }

        if (CollectionUtils.isNotEmpty(phenotypes)) {
            addPanels(new Query(PanelDBAdaptor.QueryParams.PHENOTYPES_NAME.key(), phenotypes), panels);
        }

        Query query;
        if (childhood) {
            query = new Query(PanelDBAdaptor.QueryParams.NAME.key(), ADULT_PANEL_NAME + "," + CHILDHOOD_PANEL_NAME);
        } else if (haemonc) {
            query = new Query(PanelDBAdaptor.QueryParams.NAME.key(), ADULT_PANEL_NAME + "," + CHILDHOOD_PANEL_NAME + ","
                    + HAEMONC_PANEL_NAME);
        } else {
            query = new Query(PanelDBAdaptor.QueryParams.NAME.key(), ADULT_PANEL_NAME);
        }
        addPanels(query, panels);

        // TODO: check if proband is younger than 18 years old in order to add CHILDHOOD panel

        return panels;
    }

    private void addPanels(Query query, List<DiseasePanel> panels) throws CatalogException {
        QueryResult<Panel> panelQueryResult = catalogManager.getPanelManager().get(studyId, query, QueryOptions.empty(), sessionId);
        for (Panel panel : panelQueryResult.getResult()) {
            panels.add(panel);
        }
    }

    private List<DiseasePanel> getDiseasePanels(Variant variant, List<DiseasePanel> panels) {
        Set<DiseasePanel> panelSet = new HashSet<>();
        if (variant.getAnnotation() != null && CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
            for (DiseasePanel panel : panels) {
                for (DiseasePanel.GenePanel panelGene : panel.getGenes()) {
                    for (ConsequenceType consequenceType : variant.getAnnotation().getConsequenceTypes()) {
                        if (panelGene.getId() != null && consequenceType.getEnsemblGeneId() != null &&
                                panelGene.equals(consequenceType.getEnsemblGeneId())) {
                            panelSet.add(panel);
                            break;
                        }
                    }
                }
            }
        }

        return new ArrayList<>(panelSet);
    }

    private List<Variant> filterVariants(List<Variant> input, List<Sample> samples) {
        // These function discard variants if
        //    1) it is present in the black list
        //    2) or its allelic depth (AD) is 1,0
        List<Variant> variants = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(input)) {
            for (Variant variant : input) {
                // Remove variant in black list and allelic depth (AD) must be different to "0,1"
                if (!variantIdsToDiscard.contains(variant.getId())) {
                    for (Sample sample : samples) {
                        String allelicDepthField = variant.getStudies().get(0).getSampleData(sample.getId(), "AD");
                        if (!"1,0".equals(allelicDepthField)) {
                            variants.add(variant);
                            break;
                        }
                    }
                }
            }
        }
        return variants;
    }

    private List<ClinVar> getClinVars(Variant variant) {
        // Get ClinVar object for a given variant
        if (variant.getAnnotation() != null && variant.getAnnotation().getVariantTraitAssociation() != null
                && CollectionUtils.isNotEmpty(variant.getAnnotation().getVariantTraitAssociation().getClinvar())) {
            return variant.getAnnotation().getVariantTraitAssociation().getClinvar();
        }
        return null;
    }
}
