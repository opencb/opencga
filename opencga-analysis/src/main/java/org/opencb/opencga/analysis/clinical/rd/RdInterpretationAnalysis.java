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

package org.opencb.opencga.analysis.clinical.rd;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.clinical.ClinicalAcmg;
import org.opencb.biodata.models.clinical.ClinicalProperty;
import org.opencb.biodata.models.clinical.ClinicalProperty.ModeOfInheritance;
import org.opencb.biodata.models.clinical.ClinicalProperty.Penetrance;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.Xref;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.GeneCancerAssociation;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.analysis.AnalysisUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.core.common.GitRepositoryState;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.ClinicalProperty.ModeOfInheritance.*;
import static org.opencb.biodata.models.clinical.ClinicalProperty.Penetrance.UNKNOWN;
import static org.opencb.biodata.models.clinical.interpretation.VariantClassification.calculateAcmgClassification;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.*;

public class RdInterpretationAnalysis {

    public static final String PANEL_REGION_QUERY_KEY = "PANEL_REGION";
    public static final String REGION = "REGION";
    public static final String GENE = "GENE";
    public static final String VARIANT = "VARIANT";
    
    private String clinicalAnalysisId;
    
    private String probandId;
    private String familyId;
    private List<String> panelIds;

    private List<Panel> panels;
    private String disorderId;
    List<String> sampleIds;

    private Path configPath;
    private RdInterpretationConfiguration config;

    private String study;
    private CatalogManager catalogManager;
    private VariantStorageManager variantStorageManager;
    private String token;

    protected Logger logger = LoggerFactory.getLogger(this.getClass());   
    
    private String assembly;

    private static Map<ModeOfInheritance, List<ModeOfInheritance>> moiCompatibility;

    static {
        moiCompatibility = new EnumMap<>(ModeOfInheritance.class);

        moiCompatibility.put(AUTOSOMAL_DOMINANT, Arrays.asList(AUTOSOMAL_DOMINANT, DE_NOVO));
        moiCompatibility.put(AUTOSOMAL_RECESSIVE, Collections.singletonList(AUTOSOMAL_RECESSIVE));
        moiCompatibility.put(X_LINKED_DOMINANT, Arrays.asList(X_LINKED_DOMINANT, DE_NOVO));
        moiCompatibility.put(X_LINKED_RECESSIVE, Collections.singletonList(X_LINKED_RECESSIVE));
        moiCompatibility.put(Y_LINKED, Collections.singletonList(Y_LINKED));
        moiCompatibility.put(MITOCHONDRIAL, Collections.singletonList(MITOCHONDRIAL));
        moiCompatibility.put(DE_NOVO, Arrays.asList(DE_NOVO, AUTOSOMAL_DOMINANT, X_LINKED_DOMINANT, X_LINKED_RECESSIVE));
        moiCompatibility.put(MENDELIAN_ERROR, Collections.singletonList(MENDELIAN_ERROR));
        moiCompatibility.put(COMPOUND_HETEROZYGOUS, Arrays.asList(COMPOUND_HETEROZYGOUS, AUTOSOMAL_RECESSIVE));
        moiCompatibility.put(ModeOfInheritance.UNKNOWN, Collections.emptyList());
    }

    public RdInterpretationAnalysis(String probandId, String familyId, List<String> panelIds, Path configPath, String study,
                                    CatalogManager catalogManager, VariantStorageManager variantStorageManager, String token) {
        this.probandId = probandId;
        this.familyId = familyId;
        this.panelIds = panelIds;
        init(configPath, study, catalogManager, variantStorageManager, token);
    }
    
    public RdInterpretationAnalysis(String clinicalAnalysisId, Path configPath, String study, CatalogManager catalogManager,
                                    VariantStorageManager variantStorageManager, String token) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        init(configPath, study, catalogManager, variantStorageManager, token);
    }
    
    private void init(Path configPath, String study, CatalogManager catalogManager,
                      VariantStorageManager variantStorageManager, String token) {
        this.configPath = configPath;
        this.study = study;
        this.catalogManager = catalogManager;
        this.variantStorageManager = variantStorageManager;
        this.token = token;
    }
    
    public Interpretation run() throws CatalogException, ToolException {
        // Check parameters before running analysis
        check();

        // Create map to store the variants returned by each query
        Map<String, List<Variant>> variantsPerQuery = new HashMap<>();

        // Execute variant queries from the interpretation configuration
        Map<String, Object> queries = config.getQueries();
        for (Map.Entry<String, Object> entry : queries.entrySet()) {
            if (PANEL_REGION_QUERY_KEY.equalsIgnoreCase(entry.getKey())) {
                queryByPanelRegions(entry, variantsPerQuery);
            } else {
                query(entry, variantsPerQuery);
            }
        }

        // Log number of variants retrieved per query
        for (Map.Entry<String, List<Variant>> entry : variantsPerQuery.entrySet()) {
            logger.info("Query '{}' retrieved {} variants", entry.getKey(), entry.getValue().size());
        }

        // Create a list with all variants from the interpretation configuration queries
        Set<String> variantIds = new HashSet<>();
        Set<Variant> allVariants = new HashSet<>();
        for (Map.Entry<String, List<Variant>> entry : variantsPerQuery.entrySet()) {
            for (Variant variant : entry.getValue()) {
                if (variantIds.add(variant.toString())) {
                    allVariants.add(variant);
                }
            }
        }

        // Create a map key = variantId and value = set of mois
        Map<String, List<ModeOfInheritance>> variantMoisMap = getVariantMoisMap(variantsPerQuery);

        // Create a map where key = variantId and value = list of panels
        Map<String, List<DiseasePanel>> variantPanelsMap = getVariantPanelsMap(variantsPerQuery);

        // Create clinical variants from the queried variants
        List<ClinicalVariant> clinicalVariants = new ArrayList<>();
        for (Variant variant : allVariants) {
            List<ModeOfInheritance> mois = variantMoisMap.getOrDefault(variant.toString(), Collections.emptyList());
            List<DiseasePanel> panels = getDiseasePanels(variant, variantPanelsMap);
            clinicalVariants.add(createClinicalVariant(variant, mois, panels, config));
        }

        // Then, set tier in each clinical variant evidence  if tier configuration is provided
        List<ClinicalVariant> updatedClinicalVariants = setTierInClinicalVariants(clinicalVariants);

        // And finally, create interpretation
        Interpretation interpretation = new Interpretation()
                .setPrimaryFindings(updatedClinicalVariants)
                .setCreationDate(TimeUtils.getTime());

        // Set clinical analysis ID
        if (StringUtils.isNotEmpty(clinicalAnalysisId)) {
            interpretation.setClinicalAnalysisId(clinicalAnalysisId);
        }

        // Set method
        InterpretationMethod method = new InterpretationMethod(RdInterpretationAnalysisTool.ID, null, null,
                Collections.singletonList(new Software().setName("OpenCGA").setVersion(GitRepositoryState.getInstance().getBuildVersion())
                        .setCommit(GitRepositoryState.getInstance().getCommitId())));
        interpretation.setMethod(method);

        // Set attributes
        ObjectMap additionalAttributes = new ObjectMap();
        additionalAttributes.put("configuration", config);
        interpretation.setAttributes(additionalAttributes);

        return interpretation;
    }

    private void check() throws CatalogException {
        // Check clinical analysis
        if (StringUtils.isNotEmpty(clinicalAnalysisId)) {
            ClinicalAnalysis ca = catalogManager.getClinicalAnalysisManager().get(study, clinicalAnalysisId, QueryOptions.empty(), token)
                    .first();
            // Check proband in clinical analysis
            if (ca.getProband() == null || StringUtils.isEmpty(ca.getProband().getId())) {
                throw new IllegalArgumentException("Missing proband in clinical analysis " + ca.getId());
            }
            probandId = ca.getProband().getId();

            // Check family in clinical analysis
            if (ca.getFamily() == null || StringUtils.isEmpty(ca.getFamily().getId())) {
                throw new IllegalArgumentException("Missing family in clinical analysis " + ca.getId());
            }
            familyId = ca.getFamily().getId();

            // Check disease panels in clinical analysis
            if (CollectionUtils.isEmpty(ca.getPanels())) {
                throw new IllegalArgumentException("Missing disease panels in clinical analysis " + ca.getId());
            }
            panels = ca.getPanels();
        }
        
        // Check proband
        OpenCGAResult<Individual> indvidualResult = catalogManager.getIndividualManager().get(study, probandId, QueryOptions.empty(),
                token);
        if (indvidualResult.getNumResults() == 0) {
            throw new IllegalArgumentException("Proband '" + probandId + " not found.");
        }
        Individual proband = indvidualResult.first();
        if (CollectionUtils.isEmpty(proband.getDisorders())) {
            throw new IllegalArgumentException("No disorders associated to proband '" + probandId);
        }
        sampleIds = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(proband.getSamples())) {
            sampleIds.add(proband.getSamples().get(0).getId());
        }
        if (proband.getMother() != null && CollectionUtils.isNotEmpty(proband.getMother().getSamples())) {
            sampleIds.add(proband.getMother().getSamples().get(0).getId());
        }
        if (proband.getFather() != null && CollectionUtils.isNotEmpty(proband.getFather().getSamples())) {
            sampleIds.add(proband.getFather().getSamples().get(0).getId());
        }
        if (sampleIds.isEmpty()) {
            throw new IllegalArgumentException("No samples associated to proband '" + probandId + " and his parents.");
        }

        // Check disorder
        if (proband.getDisorders().size() > 1) {
            logger.warn("More than one disorder associated to proband '{}'. Proceeding with the first one: {}", probandId,
                    proband.getDisorders().get(0).getId());
        }
        disorderId = proband.getDisorders().get(0).getId();

        // Check family in clinical analysis
        OpenCGAResult<Family> familyResult = catalogManager.getFamilyManager().get(study, familyId, QueryOptions.empty(), token);
        if (familyResult.getNumResults() == 0) {
            throw new IllegalArgumentException("Family '" + familyId + "' not found.");
        }

        // Check configuration
        if (!Files.exists(configPath)) {
            throw new IllegalArgumentException("RD interpretation configuration file " + configPath + " not found.");
        }
        try {
            config = JacksonUtils.getDefaultObjectMapper().convertValue(new Yaml().load(Files.newInputStream(configPath)),
                    RdInterpretationConfiguration.class);
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
        if (config == null) {
            throw new IllegalArgumentException("RD interpretation configuration is null");
        }
        if (MapUtils.isEmpty(config.getQueries())) {
            throw new IllegalArgumentException("Missing queries in RD interpretation configuration");
        }
        if (config.getTierConfiguration() == null || MapUtils.isEmpty(config.getTierConfiguration().getTiers())) {
            throw new IllegalArgumentException("No tier configuration found in RD interpretation configuration.");
        }

        // Get assembly
        this.assembly = AnalysisUtils.getAssembly(catalogManager, study, token);
    }

    private List<ClinicalVariant> setTierInClinicalVariants(List<ClinicalVariant> inputClinicalVariants) {
        List<ClinicalVariant> outputClinicalVariants = inputClinicalVariants;

        if (config.getTierConfiguration() != null
                && MapUtils.isNotEmpty(config.getTierConfiguration().getTiers())) {

            // Get valid tiers from interpretation configuration
            Set<String> validTiers = config.getTierConfiguration().getTiers().values().stream()
                    .map(tier -> tier.getLabel().toLowerCase()).collect(Collectors.toSet());

            // Set tier for each clinical variant evidence
            List<ClinicalVariant> updatedClinicalVariants = new ArrayList<>(inputClinicalVariants);
            for (ClinicalVariant clinicalVariant : updatedClinicalVariants) {
                setTier(clinicalVariant, config.getTierConfiguration());
            }

            // Filter out clinical variants with no tier assigned in any of their evidences
            outputClinicalVariants = new ArrayList<>();
            for (ClinicalVariant clinicalVariant : updatedClinicalVariants) {
                List<ClinicalVariantEvidence> tieredEvidences = new ArrayList<>(clinicalVariant.getEvidences().size());
                for (ClinicalVariantEvidence evidence : clinicalVariant.getEvidences()) {
                    if (evidence.getClassification() != null
                            && StringUtils.isNotEmpty(evidence.getClassification().getTier())
                            && validTiers.contains(evidence.getClassification().getTier().toLowerCase())) {
                        tieredEvidences.add(evidence);
                    }
                }
                if (!CollectionUtils.isEmpty(tieredEvidences)) {
                    clinicalVariant.setEvidences(tieredEvidences);
                    outputClinicalVariants.add(clinicalVariant);
                }
            }
        }
        return outputClinicalVariants;
    }

    private void setTier(ClinicalVariant clinicalVariant, TierConfiguration tierConfiguration) {
        // Set tier for each evidence
        for (ClinicalVariantEvidence evidence : clinicalVariant.getEvidences()) {
            String tier = determineTier(evidence, clinicalVariant, tierConfiguration);
            if (evidence.getClassification() != null) {
                evidence.setClassification(new VariantClassification());
            }
            evidence.getClassification().setTier(tier);
        }
    }

    public String determineTier(ClinicalVariantEvidence evidence, Variant variant, TierConfiguration tierConfiguration) {
        for (Map.Entry<String, TierConfiguration.Tier> tierEntry : tierConfiguration.getTiers().entrySet()) {
            for (TierConfiguration.Scenario tierScenario : tierEntry.getValue().getScenarios()) {
                if (tierScenario.getConditions().entrySet().stream().allMatch(condition -> evaluateCondition(condition, evidence,
                        variant))) {
                    return tierEntry.getValue().getLabel();
                }
            }
        }
        return "";
    }

    private boolean evaluateCondition(Map.Entry<String, Object> condition, ClinicalVariantEvidence evidence, Variant variant) {
        switch (condition.getKey()) {
            case "in_panel": {
                return evaluateInPanel((String) condition.getValue(), evidence, variant);
            }
            case "moi_match": {
                if (Boolean.TRUE.equals(condition.getValue())) {
                    return evaluateMoi(evidence, variant);
                }
                return true;
            }
            case "consequence_impact": {
                return evaluateConsequenceImpact((List<String>) condition.getValue(), evidence);
            }
            case "region_required_overlap": {
                if (Boolean.TRUE.equals(condition.getValue())) {
                    return evaluateRegionRequiredOverlap(evidence, variant);
                }
                return true;
            }
            default: {
                throw new IllegalArgumentException("Unknown tier condition: " + condition.getKey());
            }
        }
    }

    private boolean evaluateInPanel(String mode, ClinicalVariantEvidence evidence, Variant variant) {
        DiseasePanel panel = checkPanel(evidence.getPanelId());
        if (panel == null) {
            return false;
        }

        if (!mode.equalsIgnoreCase(evidence.getGenomicFeature().getType())) {
            return false;
        }

        switch (evidence.getGenomicFeature().getType()) {
            case VARIANT:
                return getVariantPanel(panel, variant) != null;
            case GENE:
                return getGenePanel(panel, evidence.getGenomicFeature().getGeneName()) != null;
            case REGION:
                return CollectionUtils.isNotEmpty(getRegionPanels(panel, variant));
            default:
                logger.warn("Unknown genomic feature type: {}", evidence.getGenomicFeature().getType());
                return false;
        }
    }

    private boolean evaluateMoi(ClinicalVariantEvidence evidence, Variant variant) {
        DiseasePanel panel = checkPanel(evidence.getPanelId());
        if (panel == null) {
            return false;
        }

        if (CollectionUtils.isEmpty(evidence.getModeOfInheritances())) {
            return false;
        }
        for (ModeOfInheritance moi : evidence.getModeOfInheritances()) {
            if (moi == ModeOfInheritance.UNKNOWN) {
                continue;
            }

            switch (evidence.getGenomicFeature().getType()) {
                case VARIANT: {
                    DiseasePanel.VariantPanel variantPanel = getVariantPanel(panel, variant);
                    if (variantPanel != null && CollectionUtils.isNotEmpty(variantPanel.getCoordinates())) {
                        for (ModeOfInheritance panelMoi : variantPanel.getModesOfInheritance()) {
                            if (isMoICompatible(moi, panelMoi)) {
                                return true;
                            }
                        }
                    }
                    break;
                }
                case GENE: {
                    DiseasePanel.GenePanel genePanel = getGenePanel(panel, evidence.getGenomicFeature().getGeneName());
                    if (genePanel != null && CollectionUtils.isNotEmpty(genePanel.getCoordinates())) {
                        for (ModeOfInheritance panelMoi : genePanel.getModesOfInheritance()) {
                            if (isMoICompatible(moi, panelMoi)) {
                                return true;
                            }
                        }
                    }
                    break;
                }
                case REGION: {
                    List<DiseasePanel.RegionPanel> regionPanels = getRegionPanels(panel, variant);
                    if (CollectionUtils.isNotEmpty(regionPanels)) {
                        for (DiseasePanel.RegionPanel regionPanel : regionPanels) {
                            for (ModeOfInheritance panelMoi : regionPanel.getModesOfInheritance()) {
                                if (isMoICompatible(moi, panelMoi)) {
                                    return true;
                                }
                            }
                        }
                    }
                    break;
                }
                default: {
                    logger.warn("Unknown genomic feature type: {}", evidence.getGenomicFeature().getType());
                    return false;
                }
            }
        }
        return false;
    }

    private static boolean isMoICompatible(ModeOfInheritance evidenceMoi, ModeOfInheritance panelMoi) {
        return moiCompatibility.get(evidenceMoi).contains(panelMoi);
    }

    private boolean evaluateConsequenceImpact(List<String> soTerms, ClinicalVariantEvidence evidence) {
        if (CollectionUtils.isEmpty(evidence.getGenomicFeature().getConsequenceTypes())) {
            return false;
        }

        for (SequenceOntologyTerm soTerm : evidence.getGenomicFeature().getConsequenceTypes()) {
            if (soTerms.contains(soTerm.getName()) || soTerms.contains(soTerm.getAccession())) {
                return true;
            }
        }

        return false;
    }

    private boolean evaluateRegionRequiredOverlap(ClinicalVariantEvidence evidence, Variant variant) {
        DiseasePanel panel = checkPanel(evidence.getPanelId());
        if (panel == null) {
            return false;
        }

        if (!REGION.equalsIgnoreCase(evidence.getGenomicFeature().getType())) {
            return false;
        }

        String location = null;
        if (CollectionUtils.isNotEmpty(evidence.getGenomicFeature().getXrefs())) {
            location = evidence.getGenomicFeature().getXrefs().stream()
                    .filter(xref -> "location".equalsIgnoreCase(xref.getDbName()))
                    .map(Xref::getId)
                    .findFirst()
                    .orElse(null);
        }
        if (location == null) {
            return false;
        }

        DiseasePanel.RegionPanel regionPanel = getRegionPanel(panel, evidence.getGenomicFeature().getId());
        if (regionPanel == null) {
            return false;
        }

        // Compute overlap percentage
        double overlapPercentage = getOverlapPercentage(new Region(location), variant);
        return (overlapPercentage >= regionPanel.getRequiredOverlapPercentage());
    }

    private DiseasePanel checkPanel(String panelId) {
        if (StringUtils.isEmpty(panelId)) {
            return null;
        }
        // Get panel from clinical analysis, otherwise null
        return panels.stream().filter(p -> panelId.equalsIgnoreCase(p.getId())).findFirst().orElse(null);
    }

    private List<DiseasePanel> getDiseasePanels(Variant variant, Map<String, List<DiseasePanel>> variantPanelsMap) {
        List<DiseasePanel> diseasePanels = variantPanelsMap.getOrDefault(variant.toString(), Collections.emptyList());
        if (CollectionUtils.isNotEmpty(panels)) {
            for (Panel panel : panels) {
                if (CollectionUtils.isNotEmpty(panel.getRegions())) {
                    for (DiseasePanel.RegionPanel regionPanel : panel.getRegions()) {
                        if (regionPanel.getCoordinates().stream()
                                .anyMatch(coordinate -> Region.parseRegion(coordinate.getLocation())
                                        .overlaps(variant.getChromosome(), variant.getStart(), variant.getEnd()))
                                && !diseasePanels.contains(panel)) {
                            diseasePanels.add(panel);
                            break;
                        }
                    }
                }
            }
        }
        return diseasePanels;
    }

    private Map<String, List<DiseasePanel>> getVariantPanelsMap(Map<String, List<Variant>> variantsPerQuery) {
        Map<String, List<DiseasePanel>> variantPanelsMap = new HashMap<>();
        for (DiseasePanel panel : panels) {
            for (Map.Entry<String, List<Variant>> entry : variantsPerQuery.entrySet()) {
                for (Variant variant : entry.getValue()) {
                    variantPanelsMap.computeIfAbsent(variant.toString(), k -> new ArrayList<>());
                    // First, panel variants
                    if (CollectionUtils.isNotEmpty(panel.getVariants()) && panel.getVariants().stream()
                            .anyMatch(variantPanel -> variantPanel.getId().equals(variant.toString()))
                            && !variantPanelsMap.get(variant.toString()).contains(panel)) {
                        variantPanelsMap.get(variant.toString()).add(panel);
                    }

                    // Second, panel genes, get the genes from all the consequence types
                    Set<String> variantGenes = new HashSet<>();
                    if (variant.getAnnotation() != null && CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                        for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                            if (StringUtils.isNotEmpty(ct.getGeneName())) {
                                variantGenes.add(ct.getGeneName());
                            }
                        }
                    }
                    if (CollectionUtils.isNotEmpty(panel.getGenes()) && panel.getGenes().stream()
                            .anyMatch(genePanel -> variantGenes.contains(genePanel.getName())
                                    || variantGenes.contains(genePanel.getId()))
                            && !variantPanelsMap.get(variant.toString()).contains(panel)) {
                        variantPanelsMap.get(variant.toString()).add(panel);
                    }
                }
            }
        }
        return variantPanelsMap;
    }

    private Map<String, List<ModeOfInheritance>> getVariantMoisMap(Map<String, List<Variant>> variantsPerQuery) {
        Map<String, List<ModeOfInheritance>> variantMoisMap = new HashMap<>();
        for (Map.Entry<String, List<Variant>> entry : variantsPerQuery.entrySet()) {
            Map<String, Object> map = (Map<String, Object>) config.getQueries().get(entry.getKey());
            ModeOfInheritance moi = getModeOfInheritanceFromQuery(map);
            for (Variant variant : entry.getValue()) {
                variantMoisMap.computeIfAbsent(variant.toString(), k -> new ArrayList<>());
                if (moi != ModeOfInheritance.UNKNOWN && !variantMoisMap.get(variant.toString()).contains(moi)) {
                    variantMoisMap.get(variant.toString()).add(moi);
                }
            }
        }
        return variantMoisMap;
    }

    private void query(Map.Entry<String, Object> entry, Map<String, List<Variant>> variantsPerQuery) throws ToolException {
        Query query = new Query(VariantQueryParam.STUDY.key(), study);
        if (entry.getValue() instanceof Map) {
            query.appendAll((Map<String, Object>) entry.getValue());
        } else {
            throw new ToolException("Unexpected query configuration for key " + entry.getKey());
        }
        ModeOfInheritance moi = getModeOfInheritanceFromQuery(query);
        if (moi != ModeOfInheritance.UNKNOWN) {
            query.put(FAMILY.key(), familyId);
            query.put(FAMILY_PROBAND.key(), probandId);
            query.put(FAMILY_DISORDER.key(), disorderId);
        }
        logger.info("Query label: {} (moi = {}); query = {}", entry.getKey(), moi, query.toJson());

        // Execute query and save the returned variants in the map variantsPerQuery
        try {
            variantsPerQuery.put(entry.getKey(), variantStorageManager.get(query, QueryOptions.empty(), token).getResults());
        } catch (Exception e) {
            logger.warn("Skipping variant query '{}' by error: {}", entry.getKey(), e.getMessage(), e);
        }
    }

    private void queryByPanelRegions(Map.Entry<String, Object> entry, Map<String, List<Variant>> variantsPerQuery) throws ToolException {
        // Prepare disease panels
        List<Region> regions = panels.stream()
                .filter(panel -> panel.getRegions() != null)
                .flatMap(panel -> panel.getRegions().stream())
                .flatMap(regionPanel -> regionPanel.getCoordinates().stream())
                .filter(coordinate -> coordinate.getAssembly().equalsIgnoreCase(assembly))
                .map(coordinate -> Region.parseRegion(coordinate.getLocation()))
                .collect(Collectors.toList());

        if (regions.isEmpty()) {
            logger.info("No regions found in the disease panels for assembly {}, so skipping region queries.", assembly);
        }

        Query query = new Query();
        if (entry.getValue() instanceof Map) {
            query.appendAll((Map<String, Object>) entry.getValue());
        } else {
            throw new ToolException("Unexpected query configuration for key " + entry.getKey());
        }
        query.append(VariantQueryParam.REGION.key(), regions)
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.STUDY.key(), study)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");

        if (CollectionUtils.isNotEmpty(sampleIds)) {
            query.append(VariantQueryParam.SAMPLE.key(), sampleIds);
        }

        try {
            variantsPerQuery.put(entry.getKey(), variantStorageManager.get(query, QueryOptions.empty(), token).getResults());
        } catch (CatalogException | IOException | StorageEngineException e) {
            logger.warn("Error querying region variants: {}", e.getMessage(), e);
        }
    }

    private ClinicalVariant createClinicalVariant(Variant variant, List<ModeOfInheritance> mois, List<DiseasePanel> panels,
                                                  RdInterpretationConfiguration config) throws ToolException {
        ClinicalVariant cv = new ClinicalVariant(variant.getImpl());

        // Sanity check
        if (variant.getAnnotation() == null && CollectionUtils.isEmpty(variant.getAnnotation().getConsequenceTypes())) {
            return cv;
        }
        if (CollectionUtils.isEmpty(mois) && CollectionUtils.isEmpty(panels)) {
            return cv;
        }

        // Compute the clinical variant evidences, one clinical evidence by mode of inheritance, panel and consequence type
        List<ClinicalVariantEvidence> evidences;
        if (CollectionUtils.isEmpty(mois)) {
            evidences = createEvidencesWithPanels(variant, panels);
        } else if (CollectionUtils.isEmpty(panels)) {
            evidences = createEvidencesWithMois(variant, mois);
        } else {
            evidences = createEvidences(variant, mois, panels, config);
        }
        cv.setEvidences(evidences);

        return cv;
    }

    private List<ClinicalVariantEvidence> createEvidencesWithPanels(Variant variant, List<DiseasePanel> panels) throws ToolException {
        List<ClinicalVariantEvidence> evidences = new ArrayList<>();
        for (DiseasePanel panel : panels) {
            DiseasePanel.VariantPanel variantPanel = getVariantPanel(panel, variant);
            List<DiseasePanel.RegionPanel> regionPanels = getRegionPanels(panel, variant);
            for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                createEvidences(variant, panel, ModeOfInheritance.UNKNOWN, UNKNOWN, ct, evidences, variantPanel, regionPanels);
            }
        }
        return evidences;
    }

    private List<ClinicalVariantEvidence> createEvidencesWithMois(Variant variant, List<ModeOfInheritance> mois) throws ToolException {
        List<ClinicalVariantEvidence> evidences = new ArrayList<>();
        for (ModeOfInheritance moi : mois) {
            for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                GenomicFeature genomicFeature = getGenomicFeature(GENE, null, null, ct);
                if (genomicFeature != null) {
                    evidences.add(createClinicalVariantEvidence(genomicFeature, null, moi, UNKNOWN, ct, variant.getAnnotation()));
                }
            }
        }
        return evidences;
    }

    private List<ClinicalVariantEvidence> createEvidences(Variant variant, List<ModeOfInheritance> mois,
                                                          List<DiseasePanel> panels, RdInterpretationConfiguration config) throws ToolException {
        List<ClinicalVariantEvidence> evidences = new ArrayList<>();
        for (DiseasePanel panel : panels) {
            DiseasePanel.VariantPanel variantPanel = getVariantPanel(panel, variant);
            List<DiseasePanel.RegionPanel> regionPanels = getRegionPanels(panel, variant);
            for (ModeOfInheritance moi : mois) {
                for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                    createEvidences(variant, panel, moi, UNKNOWN, ct, evidences, variantPanel, regionPanels);
                }
            }
        }
        return evidences;
    }

    private void createEvidences(Variant variant, DiseasePanel panel, ModeOfInheritance moi, Penetrance penetrance, ConsequenceType ct,
                                 List<ClinicalVariantEvidence> evidences, DiseasePanel.VariantPanel variantPanel,
                                 List<DiseasePanel.RegionPanel> regionPanels) throws ToolException {
        GenomicFeature genomicFeature;

        // Create genomic feature for gene in panel
        DiseasePanel.GenePanel genePanel = getGenePanel(panel, ct.getGeneName());
        if (genePanel != null) {
            genomicFeature = getGenomicFeature(GENE, null, null, ct);
            if (genomicFeature != null) {
                evidences.add(createClinicalVariantEvidence(genomicFeature, panel.getId(), moi, penetrance, ct, variant.getAnnotation()));
            }
        }

        // Create genomic feature for variant in panel
        if (variantPanel != null) {
            genomicFeature = getGenomicFeature(VARIANT, variant.toString(), null, ct);
            if (genomicFeature != null) {
                evidences.add(createClinicalVariantEvidence(genomicFeature, panel.getId(), moi, penetrance, ct, variant.getAnnotation()));
            }
        }

        // Create genomic feature for regions in panel
        if (CollectionUtils.isNotEmpty(regionPanels)) {
            for (DiseasePanel.RegionPanel regionPanel : regionPanels) {
                genomicFeature = getGenomicFeature(REGION, null, regionPanel, ct);
                if (genomicFeature != null) {
                    evidences.add(createClinicalVariantEvidence(genomicFeature, panel.getId(), moi, penetrance, ct,
                            variant.getAnnotation()));
                }
            }
        }
    }

    private DiseasePanel.VariantPanel getVariantPanel(DiseasePanel panel, Variant variant) {
        if (CollectionUtils.isNotEmpty(panel.getVariants())) {
            for (DiseasePanel.VariantPanel panelVariant : panel.getVariants()) {
                if (panelVariant.getId().equals(variant.toString())) {
                    return panelVariant;
                }
            }
        }
        return null;
    }

    private List<DiseasePanel.RegionPanel> getRegionPanels(DiseasePanel panel, Variant variant) {
        Set<DiseasePanel.RegionPanel> regions = new HashSet<>();
        if (CollectionUtils.isNotEmpty(panel.getRegions())) {
            for (DiseasePanel.RegionPanel regionPanel : panel.getRegions()) {
                for (DiseasePanel.Coordinate coordinate : regionPanel.getCoordinates()) {
                    if (assembly.equalsIgnoreCase(coordinate.getAssembly())) {
                        Region region = Region.parseRegion(coordinate.getLocation());
                        if (region.overlaps(variant.getChromosome(), variant.getStart(), variant.getEnd())) {
                            regions.add(regionPanel);
                        }
                    }
                }
            }
        }
        return new ArrayList<>(regions);
    }

    private DiseasePanel.RegionPanel getRegionPanel(DiseasePanel panel, String id) {
        if (CollectionUtils.isNotEmpty(panel.getRegions())) {
            for (DiseasePanel.RegionPanel regionPanel : panel.getRegions()) {
                if (panel.getId().equalsIgnoreCase(id)) {
                    return regionPanel;
                }
            }
        }
        return null;
    }

    private DiseasePanel.GenePanel getGenePanel(DiseasePanel panel, String geneName) {
        if (CollectionUtils.isNotEmpty(panel.getGenes())) {
            for (DiseasePanel.GenePanel panelGene : panel.getGenes()) {
                if (panelGene.getName().equalsIgnoreCase(geneName)) {
                    return panelGene;
                }
            }
        }
        return null;
    }

    private GenomicFeature getGenomicFeature(String type, String variantId, DiseasePanel.RegionPanel regionPanel, ConsequenceType ct)
            throws ToolException {
        String geneId = ct.getEnsemblGeneId();
        String transcriptId = ct.getEnsemblTranscriptId();
        if (StringUtils.isNotEmpty(ct.getSource()) && "refseq".equalsIgnoreCase(ct.getSource())) {
            geneId = ct.getGeneId();
            transcriptId = ct.getTranscriptId();
        }
        if (StringUtils.isEmpty(transcriptId)) {
            logger.warn("Consequence type (gene name: {}) has no transcript ID, so skipping genomic feature creation.", ct.getGeneName());
            return null;
        }

        List<Xref> xrefs = new ArrayList<>();

        String id;
        switch (type) {
            case VARIANT: {
                id = variantId;
                break;
            }
            case REGION: {
                if (regionPanel == null) {
                    throw new ToolException("Region panel cannot be null for genomic feature of type REGION.");
                }
                id = regionPanel.toString();
                regionPanel.getCoordinates().stream().filter(c -> assembly.equalsIgnoreCase(c.getAssembly())).findFirst()
                        .ifPresent(c -> xrefs.add(new Xref(c.getLocation(), "location", "location (" + id + ")")));
                break;
            }
            case GENE:
            default: {
                id = geneId;
                break;
            }
        }
        return new GenomicFeature(id, type, transcriptId, ct.getGeneName(), ct.getSequenceOntologyTerms(), xrefs);
    }

    protected ClinicalVariantEvidence createClinicalVariantEvidence(GenomicFeature genomicFeature, String panelId, ModeOfInheritance moi,
                                                                    Penetrance penetrance, ConsequenceType consequenceType,
                                                                    VariantAnnotation variantAnnotation) {
        ClinicalVariantEvidence clinicalVariantEvidence = new ClinicalVariantEvidence();

        // Genomic feature
        if (genomicFeature != null) {
            clinicalVariantEvidence.setGenomicFeature(genomicFeature);
        }

        // Panel ID
        if (panelId != null) {
            clinicalVariantEvidence.setPanelId(panelId);
        }

        // Mode of inheritance and penetrance
        clinicalVariantEvidence.setModeOfInheritances(Collections.singletonList(moi));
        clinicalVariantEvidence.setPenetrance(penetrance);

        // Variant classification:
        clinicalVariantEvidence.setClassification(new VariantClassification());
        //   - ACMG
        List<ClinicalAcmg> acmgs = calculateAcmgClassification(consequenceType, variantAnnotation, Collections.singletonList(moi));
        clinicalVariantEvidence.getClassification().setAcmg(acmgs);
        //   - Clinical significance
        if (StringUtils.isNotEmpty(panelId)) {
            clinicalVariantEvidence.getClassification().setClinicalSignificance(ClinicalProperty.ClinicalSignificance.PATHOGENIC);
        }

        // Role in cancer
        if (variantAnnotation != null && CollectionUtils.isNotEmpty(variantAnnotation.getGeneCancerAssociations())) {
            Set<ClinicalProperty.RoleInCancer> roles = new HashSet<>();
            for (GeneCancerAssociation geneCancerAssociation : variantAnnotation.getGeneCancerAssociations()) {
                if (CollectionUtils.isNotEmpty(geneCancerAssociation.getRoleInCancer())) {
                    for (String value : geneCancerAssociation.getRoleInCancer()) {
                        try {
                            roles.add(ClinicalProperty.RoleInCancer.valueOf(value.toUpperCase()));
                        } catch (Exception e) {
                            logger.info("Unknown role in cancer value: {}. It will be ignored.", value.toUpperCase());
                        }
                    }
                }
            }
            if (CollectionUtils.isNotEmpty(roles)) {
                List<ClinicalProperty.RoleInCancer> rolesInCancer = new ArrayList<>(roles);
                clinicalVariantEvidence.setRolesInCancer(rolesInCancer);
            }
        }

        return clinicalVariantEvidence;
    }

    private ModeOfInheritance getModeOfInheritanceFromQuery(Map<String, Object> map) {
        return getModeOfInheritanceFromQuery(new Query(map));
    }

    private ModeOfInheritance getModeOfInheritanceFromQuery(Query query) {
        ModeOfInheritance moi = ModeOfInheritance.UNKNOWN;
        if (query.containsKey(FAMILY_SEGREGATION.key())) {
            String familySegregation = query.getString(FAMILY_SEGREGATION.key());
            switch (familySegregation) {
                case "autosomalDominant":
                    return AUTOSOMAL_DOMINANT;
                case "autosomalRecessive":
                    return ModeOfInheritance.AUTOSOMAL_RECESSIVE;
                case "XLinkedDominant":
                    return ModeOfInheritance.X_LINKED_DOMINANT;
                case "XLinkedRecessive":
                    return ModeOfInheritance.X_LINKED_RECESSIVE;
                case "YLinked":
                    return ModeOfInheritance.Y_LINKED;
                case "mitochondrial":
                    return ModeOfInheritance.MITOCHONDRIAL;
                case "deNovo":
                    return ModeOfInheritance.DE_NOVO;
                case "compoundHeterozygous":
                    return ModeOfInheritance.COMPOUND_HETEROZYGOUS;
                default:
                    return ModeOfInheritance.UNKNOWN;
            }
        }
        return moi;
    }

    private double getOverlapPercentage(Region region, Variant variant) {
        int start = Math.max(region.getStart(), variant.getStart());
        int end = Math.min(region.getEnd(), variant.getEnd());
        return 100.0 * (end - start + 1) / region.size();
    }
}
