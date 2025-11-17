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
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.interpretation.ClinicalInterpretationAnalysisParams;
import org.opencb.opencga.core.models.clinical.interpretation.ClinicalInterpretationParams;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.tools.annotations.Tool;
import org.opencb.opencga.core.tools.annotations.ToolParams;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.ClinicalProperty.ModeOfInheritance.*;
import static org.opencb.biodata.models.clinical.ClinicalProperty.Penetrance.COMPLETE;
import static org.opencb.biodata.models.clinical.ClinicalProperty.Penetrance.valueOf;
import static org.opencb.biodata.models.clinical.interpretation.VariantClassification.calculateAcmgClassification;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.*;
import static org.opencb.opencga.core.tools.ResourceManager.ANALYSIS_DIRNAME;

@Tool(id = ClinicalInterpretationAnalysis.ID, resource = Enums.Resource.CLINICAL)
public class ClinicalInterpretationAnalysis extends InterpretationAnalysis {

    public static final String ID = "interpretation-custom-tiering";
    public static final String DESCRIPTION = "Run clinical interpretation analysis based on tiering";

    public static final String PANEL_REGION_QUERY_KEY = "PANEL_REGION";
    public static final String REGION = "REGION";
    public static final String GENE = "GENE";
    public static final String VARIANT = "VARIANT";

    private ClinicalAnalysis ca;
    private ClinicalInterpretationConfiguration interpretationConfig;

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
        moiCompatibility.put(UNKNOWN, Collections.emptyList());
    }

    @ToolParams
    protected final ClinicalInterpretationAnalysisParams analysisParams = new ClinicalInterpretationAnalysisParams();

    @Override
    protected InterpretationMethod getInterpretationMethod() {
        return getInterpretationMethod(ID);
    }

    @Override
    protected void check() throws Exception {
        // IMPORTANT: the first thing to do since it initializes "study" from params.get(STUDY_PARAM)
        super.check();

        setUpStorageEngineExecutor(study);

        // Check clinical analysis
        if (StringUtils.isEmpty(analysisParams.getClinicalAnalysisId())) {
            throw new ToolException("Missing clinical analysis ID");
        }

        // Get clinical analysis to check diseases, proband and family
        try {
            ca = catalogManager.getClinicalAnalysisManager().get(study, analysisParams.getClinicalAnalysisId(),
                    QueryOptions.empty(), token).first();
        } catch (
                CatalogException e) {
            throw new ToolException(e);
        }

        // Check disease panels in clinical analysis
        if (CollectionUtils.isEmpty(ca.getPanels())) {
            throw new ToolException("Missing disease panels in clinical analysis " + ca.getId());
        }

        // Check proband in clinical analysis
        if (ca.getProband() == null || StringUtils.isEmpty(ca.getProband().getId())) {
            throw new ToolException("Missing proband in clinical analysis " + ca.getId());
        }
        OpenCGAResult<Individual> indvidualResult = getCatalogManager().getIndividualManager().get(study, ca.getProband().getId(),
                QueryOptions.empty(), token);
        if (indvidualResult.getNumResults() == 0) {
            throw new ToolException("Proband '" + ca.getProband().getId() + "' in clinical analysis " + ca.getId() + " not found.");
        }
        if (CollectionUtils.isEmpty(ca.getProband().getDisorders())) {
            throw new ToolException("No disorders associated to proband '" + ca.getProband().getId() + "' in clinical analysis "
                    + ca.getId());
        }
        if (ca.getProband().getDisorders().size() > 1) {
            logger.warn("More than one disorder associated to proband '{}' in clinical analysis {}. Proceeding with the first one: {}",
                    ca.getProband().getId(), ca.getId(), ca.getProband().getDisorders().get(0).getId());
        }

        // Check family in clinical analysis
        if (ca.getFamily() != null && StringUtils.isNotEmpty(ca.getFamily().getId())) {
            OpenCGAResult<Family> familyResult = getCatalogManager().getFamilyManager().get(study, ca.getFamily().getId(),
                    QueryOptions.empty(), token);
            if (familyResult.getNumResults() == 0) {
                throw new ToolException("Family '" + ca.getFamily().getId() + "' in clinical analysis " + ca.getId() + " not found.");
            }
        } else {
            logger.warn("Missing family in clinical analysis {}. Proceeding without family information.", ca.getId());
        }

        // Check primary
        this.primary = analysisParams.isPrimary();
        checkPrimaryInterpretation(ca);

        // Check interpretation method in both primary and secondary interpretations (only one interpretation of each method can exist
        // in the clinical analysis)
        checkInterpretationMethod(getInterpretationMethod(ID).getName(), ca);

        // Read clinical interpretation configuration file from the user parameters or default one if not provided
        Path configPath;
        if (StringUtils.isNotEmpty(analysisParams.getConfigFile())) {
            logger.info("Using custom interpretation configuration file: {}", analysisParams.getConfigFile());
            File opencgaFile = getCatalogManager().getFileManager().get(study, analysisParams.getConfigFile(), QueryOptions.empty(), token)
                    .first();
            configPath = Paths.get(opencgaFile.getUri());
        } else {
            logger.info("Using default interpretation configuration file");
            configPath = getOpencgaHome().resolve(ANALYSIS_DIRNAME).resolve("interpretation").resolve("interpretation-configuration.yml");
        }
        if (!Files.exists(configPath)) {
            throw new ToolException("Tiering configuration file not found: " + configPath);
        }
        interpretationConfig = JacksonUtils.getDefaultObjectMapper().convertValue(new Yaml().load(Files.newInputStream(configPath)),
                ClinicalInterpretationConfiguration.class);

        // Finally, update interpretation configuration with the parameters provided by the user and set default values if needed
        updateInterpretationConfiguration();

        // Get assembly
        try {
            assembly = clinicalInterpretationManager.getAssembly(study, token);
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    @Override
    protected void run() throws ToolException {
        step(this::runClinicalInterpretationAnalysis);
    }

    private void runClinicalInterpretationAnalysis() throws ToolException {
        // Create map to store the variants returned by each query
        Map<String, List<Variant>> variantsPerQuery = new HashMap<>();

        // Execute variant queries from the interpretation configuration
        Map<String, Object> queries = interpretationConfig.getQueries();
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
            clinicalVariants.add(createClinicalVariant(variant, mois, panels, interpretationConfig));
        }

        // Finally, set tier in each clinical variant evidence  if tier configuration is provided
        List<ClinicalVariant> updatedClinicalVariants = setTierInClinicalVariants(clinicalVariants);

        // Write clinical variants
        ClinicalUtils.writeClinicalVariants(updatedClinicalVariants, getOutDir().resolve(PRIMARY_FINDINGS_FILENAME));
        logger.info("{} clinical variants written to {}", clinicalVariants.size(), getOutDir().resolve(PRIMARY_FINDINGS_FILENAME));

        // Add interpretation in the clinical analysis and then save in catalog
        ObjectMap additionalAttributes = new ObjectMap();
        additionalAttributes.put("configuration", interpretationConfig);
        saveInterpretation(study, ca, null, additionalAttributes);
    }

    private List<ClinicalVariant> setTierInClinicalVariants(List<ClinicalVariant> inputClinicalVariants) {
        List<ClinicalVariant> outputClinicalVariants = inputClinicalVariants;

        if (interpretationConfig.getTierConfiguration() != null
                && MapUtils.isNotEmpty(interpretationConfig.getTierConfiguration().getTiers())) {

            // Get valid tiers from interpretation configuration
            Set<String> validTiers = interpretationConfig.getTierConfiguration().getTiers().values().stream()
                    .map(tier -> tier.getLabel().toLowerCase()).collect(Collectors.toSet());

            // Set tier for each clinical variant evidence
            List<ClinicalVariant> updatedClinicalVariants = new ArrayList<>(inputClinicalVariants);
            for (ClinicalVariant clinicalVariant : updatedClinicalVariants) {
                setTier(clinicalVariant, interpretationConfig.getTierConfiguration());
            }

            outputClinicalVariants = updatedClinicalVariants;
            if (Boolean.TRUE.equals(interpretationConfig.getTierConfiguration().getDiscardUntieredEvidences())) {
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
        return ca.getPanels().stream().filter(p -> panelId.equalsIgnoreCase(p.getId())).findFirst().orElse(null);
    }

    private List<DiseasePanel> getDiseasePanels(Variant variant, Map<String, List<DiseasePanel>> variantPanelsMap) {
        List<DiseasePanel> panels = variantPanelsMap.getOrDefault(variant.toString(), Collections.emptyList());
        if (CollectionUtils.isNotEmpty(ca.getPanels())) {
            for (Panel panel : ca.getPanels()) {
                if (CollectionUtils.isNotEmpty(panel.getRegions())) {
                    for (DiseasePanel.RegionPanel regionPanel : panel.getRegions()) {
                        if (regionPanel.getCoordinates().stream()
                                .anyMatch(coordinate -> Region.parseRegion(coordinate.getLocation())
                                        .overlaps(variant.getChromosome(), variant.getStart(), variant.getEnd()))
                                && !panels.contains(panel)) {
                            panels.add(panel);
                            break;
                        }
                    }
                }
            }
        }
        return panels;
    }

    private Map<String, List<DiseasePanel>> getVariantPanelsMap(Map<String, List<Variant>> variantsPerQuery) {
        Map<String, List<DiseasePanel>> variantPanelsMap = new HashMap<>();
        for (DiseasePanel panel : ca.getPanels()) {
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
            Map<String, Object> map = (Map<String, Object>) interpretationConfig.getQueries().get(entry.getKey());
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
            query.put(FAMILY.key(), ca.getFamily().getId());
            query.put(FAMILY_PROBAND.key(), ca.getProband().getId());
            query.put(FAMILY_DISORDER.key(), ca.getProband().getDisorders().get(0).getId());
        }
        logger.info("Query label: {} (moi = {}); query = {}", entry.getKey(), moi, query.toJson());

        // Execute query and save the returned variants in the map variantsPerQuery
        try {
            variantsPerQuery.put(entry.getKey(), clinicalInterpretationManager.getVariantStorageManager().get(query, QueryOptions.empty(),
                    token).getResults());
        } catch (Exception e) {
            logger.warn("Skipping variant query '{}' by error: {}", entry.getKey(), e.getMessage(), e);
        }
    }

    private void queryByPanelRegions(Map.Entry<String, Object> entry, Map<String, List<Variant>> variantsPerQuery) throws ToolException {
        // Prepare disease panels from the clinical analysis
        if (CollectionUtils.isEmpty(ca.getPanels())) {
            logger.warn("No disease panels found in the clinical analysis, so skipping region queries.");
            return;
        }
        List<Region> regions = ca.getPanels().stream()
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

        Map<String, String> sampleMap = ClinicalUtils.getSampleMap(ca, ca.getProband());
        if (!sampleMap.isEmpty()) {
            query.append(VariantQueryParam.SAMPLE.key(), sampleMap.values());
        }

        try {
            variantsPerQuery.put(entry.getKey(), clinicalInterpretationManager.getVariantStorageManager().get(query, QueryOptions.empty(),
                    token).getResults());
        } catch (CatalogException | IOException | StorageEngineException e) {
            logger.warn("Error querying region variants: {}", e.getMessage(), e);
        }
    }

    private ClinicalVariant createClinicalVariant(Variant variant, List<ModeOfInheritance> mois, List<DiseasePanel> panels,
                                                  ClinicalInterpretationConfiguration config) throws ToolException {
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
            evidences = createEvidencesWithMois(variant, mois, config);
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
                createEvidences(variant, panel, ModeOfInheritance.UNKNOWN, Penetrance.UNKNOWN, ct, evidences, variantPanel, regionPanels);
            }
        }
        return evidences;
    }

    private List<ClinicalVariantEvidence> createEvidencesWithMois(Variant variant, List<ModeOfInheritance> mois,
                                                                  ClinicalInterpretationConfiguration config) throws ToolException {
        List<ClinicalVariantEvidence> evidences = new ArrayList<>();
        for (ModeOfInheritance moi : mois) {
            for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                GenomicFeature genomicFeature = getGenomicFeature(GENE, null, null, ct);
                if (genomicFeature != null) {
                    Penetrance penetrance = Penetrance.UNKNOWN;
                    if (StringUtils.isNotEmpty(config.getPenetrance())) {
                        penetrance = valueOf(config.getPenetrance());
                    }
                    evidences.add(createClinicalVariantEvidence(genomicFeature, null, moi, penetrance, ct, variant.getAnnotation()));
                }
            }
        }
        return evidences;
    }

    private List<ClinicalVariantEvidence> createEvidences(Variant variant, List<ModeOfInheritance> mois,
                                                          List<DiseasePanel> panels, ClinicalInterpretationConfiguration config) throws ToolException {
        Penetrance penetrance = Penetrance.UNKNOWN;
        if (StringUtils.isNotEmpty(config.getPenetrance())) {
            penetrance = valueOf(config.getPenetrance());
        }

        List<ClinicalVariantEvidence> evidences = new ArrayList<>();
        for (DiseasePanel panel : panels) {
            DiseasePanel.VariantPanel variantPanel = getVariantPanel(panel, variant);
            List<DiseasePanel.RegionPanel> regionPanels = getRegionPanels(panel, variant);
            for (ModeOfInheritance moi : mois) {
                for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                    createEvidences(variant, panel, moi, penetrance, ct, evidences, variantPanel, regionPanels);
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

    private void updateInterpretationConfiguration() {
        // Update interpretation configuration with the parameters provided by the user
        if (analysisParams.getClinicalInterpretationParams() != null) {
            ClinicalInterpretationParams params = analysisParams.getClinicalInterpretationParams();
            if (StringUtils.isNotEmpty(params.getPenetrance())) {
                // Check and update penetrance value
                Penetrance penetrance = valueOf(params.getPenetrance());
                interpretationConfig.setPenetrance(penetrance.name());
            }

            if (params.getOneConsequencePerEvidence() != null) {
                interpretationConfig.setOneConsequencePerEvidence(params.getOneConsequencePerEvidence());
            }

            if (params.getDiscardUntieredEvidences() != null && interpretationConfig.getTierConfiguration() != null) {
                interpretationConfig.getTierConfiguration().setDiscardUntieredEvidences(params.getDiscardUntieredEvidences());
            }
        }

        // Set update values if not present in the configuration
        if (StringUtils.isEmpty(interpretationConfig.getPenetrance())) {
            interpretationConfig.setPenetrance(COMPLETE.name());
        }
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
