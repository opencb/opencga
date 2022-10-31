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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.*;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.GeneCancerAssociation;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.tools.clinical.ClinicalVariantCreator;
import org.opencb.biodata.tools.clinical.DefaultClinicalVariantCreator;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.managers.StudyManager;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantEngine;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantIterator;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;

import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.VariantClassification.calculateAcmgClassification;
import static org.opencb.biodata.models.clinical.interpretation.VariantClassification.computeClinicalSignificance;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.INCLUDE_SAMPLE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.isValidParam;

public class ClinicalInterpretationManager extends StorageManager {

    private String database;

    private InterpretationAnalysisConfiguration config;

    private ClinicalAnalysisManager clinicalAnalysisManager;
    private ClinicalVariantEngine clinicalVariantEngine;
    private VariantStorageManager variantStorageManager;

    protected CellBaseClient cellBaseClient;
    protected AlignmentStorageManager alignmentStorageManager;

    private VariantCatalogQueryUtils catalogQueryUtils;

    private static Query defaultDeNovoQuery;
    private static Query defaultCompoundHeterozigousQuery;

    static {
        defaultDeNovoQuery = new Query()
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(),
                        ParamConstants.POP_FREQ_1000G + ":AFR<0.002;"
                                + ParamConstants.POP_FREQ_1000G + ":AMR<0.002;"
                                + ParamConstants.POP_FREQ_1000G + ":EAS<0.002;"
                                + ParamConstants.POP_FREQ_1000G + ":EUR<0.002;"
                                + ParamConstants.POP_FREQ_1000G + ":SAS<0.002;"
                                + "GNOMAD_EXOMES:AFR<0.001;GNOMAD_EXOMES:AMR<0.001;"
                                + "GNOMAD_EXOMES:EAS<0.001;GNOMAD_EXOMES:FIN<0.001;GNOMAD_EXOMES:NFE<0.001;GNOMAD_EXOMES:ASJ<0.001;"
                                + "GNOMAD_EXOMES:OTH<0.002")
                .append(VariantQueryParam.STATS_MAF.key(), "ALL<0.001")
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), ModeOfInheritance.proteinCoding)
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), ModeOfInheritance.extendedLof)
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");

        defaultCompoundHeterozigousQuery = new Query()
                .append(VariantQueryParam.ANNOT_BIOTYPE.key(), ModeOfInheritance.proteinCoding)
                .append(VariantQueryParam.ANNOT_CONSEQUENCE_TYPE.key(), ModeOfInheritance.extendedLof)
                .append(VariantQueryParam.INCLUDE_GENOTYPE.key(), true)
                .append(VariantQueryParam.FILTER.key(), VCFConstants.PASSES_FILTERS_v4)
                .append(VariantQueryParam.UNKNOWN_GENOTYPE.key(), "./.");
    }

    public ClinicalInterpretationManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory, Path opencgaHome) throws IOException {
        super(catalogManager, storageEngineFactory);

        this.clinicalAnalysisManager = catalogManager.getClinicalAnalysisManager();
        this.variantStorageManager = new VariantStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));

        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.alignmentStorageManager = new AlignmentStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));

        this.catalogQueryUtils = new VariantCatalogQueryUtils(catalogManager);

//        this.opencgaHome = opencgaHome;
        // Load interpretation analysis configuration
        if (opencgaHome != null) {
            ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
            Path configPath = opencgaHome.resolve("analysis/clinical-variant-query.yml");
            if (configPath.toFile().exists()) {
                FileInputStream fis = new FileInputStream(configPath.toFile());
                config = objectMapper.readValue(fis, InterpretationAnalysisConfiguration.class);
            }
        }

//        this.init();
    }


    @Override
    public void testConnection() throws StorageEngineException {
    }

    public DataResult<ClinicalVariant> index(String token) {
        return null;
    }

    public DataResult<ClinicalVariant> index(String study, String token) throws IOException, ClinicalVariantException, CatalogException {
        DBIterator<ClinicalAnalysis> clinicalAnalysisDBIterator =
                clinicalAnalysisManager.iterator(study, new Query(), QueryOptions.empty(), token);

        while (clinicalAnalysisDBIterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = clinicalAnalysisDBIterator.next();
            if (clinicalAnalysis.getInterpretation() != null) {
                clinicalAnalysis.getInterpretation().getAttributes().put("OPENCGA_CLINICAL_ANALYSIS", clinicalAnalysis);

                this.clinicalVariantEngine.insert(clinicalAnalysis.getInterpretation(), database);

            }
        }
        return null;
    }

    public DataResult<ClinicalVariant> query(Query query, QueryOptions options, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        query = checkQueryPermissions(query, token);

        return clinicalVariantEngine.query(query, options, "");
    }

//    public DataResult<Interpretation> interpretationQuery(Query query, QueryOptions options, String token)
//            throws IOException, ClinicalVariantException, CatalogException {
//        // Check permissions
//        query = checkQueryPermissions(query, token);
//
//        return clinicalVariantEngine.interpretationQuery(query, options, "");
//    }

    public DataResult<FacetField> facet(Query query, QueryOptions queryOptions, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        query = checkQueryPermissions(query, token);

        return clinicalVariantEngine.facet(query, queryOptions, "");
    }

    public ClinicalVariantIterator iterator(Query query, QueryOptions options, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        query = checkQueryPermissions(query, token);

        return clinicalVariantEngine.iterator(query, options, "");
    }

    public void addInterpretationComment(String study, long interpretationId, ClinicalComment comment, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        checkInterpretationPermissions(study, interpretationId, token);

        clinicalVariantEngine.addInterpretationComment(interpretationId, comment, "");
    }

    public void addClinicalVariantComment(String study, long interpretationId, String variantId, ClinicalComment comment, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        checkInterpretationPermissions(study, interpretationId, token);

        clinicalVariantEngine.addClinicalVariantComment(interpretationId, variantId, comment, "");
    }

    /*--------------------------------------------------------------------------*/
    /*  Get clinical variants                                                   */
    /*--------------------------------------------------------------------------*/

    public OpenCGAResult<ClinicalVariant> get(Query query, QueryOptions queryOptions, String token)
            throws CatalogException, IOException, StorageEngineException {
        return get(query, queryOptions, config, token);
    }

    public OpenCGAResult<ClinicalVariant> get(Query query, QueryOptions queryOptions, InterpretationAnalysisConfiguration config,
                                              String token) throws CatalogException, IOException, StorageEngineException {
        VariantQueryResult<Variant> variantQueryResult = variantStorageManager.get(query, queryOptions, token);
        List<Variant> variants = variantQueryResult.getResults();

        OpenCGAResult<ClinicalVariant> result = new OpenCGAResult<>(variantQueryResult.getTime(),
                variantQueryResult.getEvents(), variantQueryResult.getNumResults(), Collections.emptyList(),
                variantQueryResult.getNumMatches(), variantQueryResult.getAttributes());

        if (CollectionUtils.isEmpty(variantQueryResult.getResults())) {
            return result;
        }

        StopWatch watch = StopWatch.createStarted();

        // Get study from query
        String studyId = query.getString(STUDY.key());

        // Get assembly
        String assembly = getAssembly(studyId, token);

        // Prepare map<gene name, set of panel names>
        Map<String, Set<String>> genePanelMap = new HashMap<>();

        if (isValidParam(query, PANEL)) {
            List<String> panels = query.getAsStringList(PANEL.key());
            for (String panelId : panels) {
                org.opencb.opencga.core.models.panel.Panel panel = catalogQueryUtils.getPanel(studyId, panelId, token);
                for (DiseasePanel.GenePanel genePanel : panel.getGenes()) {
                    // Check gene name to be inserted in the panel map
                    if (StringUtils.isNotEmpty(genePanel.getName())) {
                        if (!genePanelMap.containsKey(genePanel.getName())) {
                            genePanelMap.put(genePanel.getName(), new HashSet<>());
                        }
                        genePanelMap.get(genePanel.getName()).add(panelId);
                    }
                    // Check gene ID to be inserted in the panel map
                    if (StringUtils.isNotEmpty(genePanel.getId())) {
                        if (!genePanelMap.containsKey(genePanel.getId())) {
                            genePanelMap.put(genePanel.getId(), new HashSet<>());
                        }
                        genePanelMap.get(genePanel.getId()).add(panelId);
                    }
                }
            }
        }

        List<ClinicalVariant> clinicalVariants = new ArrayList<>();

        for (Variant variant : variants) {
            ClinicalVariant clinicalVariant = createClinicalVariant(variant, genePanelMap, config);
            if (clinicalVariant != null) {
                clinicalVariants.add(clinicalVariant);
            }
        }

        // Include interpretation management
        String includeInterpretation = query.getString(ParamConstants.INCLUDE_INTERPRETATION);
        logger.info("Checking the parameter {} with value = {} in query {}", ParamConstants.INCLUDE_INTERPRETATION, includeInterpretation,
                query.toJson());
        if (StringUtils.isNotEmpty(includeInterpretation)) {
            OpenCGAResult<Interpretation> interpretationResult = catalogManager.getInterpretationManager().get(studyId,
                    includeInterpretation, QueryOptions.empty(), token);
            int numResults = interpretationResult.getNumResults();
            logger.info("Checking number of results ({}) found for the interpretation ID {}, it should be 1, otherwise something wrong"
                    + " happened", numResults, includeInterpretation);
            if (numResults == 1) {
                // Interpretation found, check if its primary findings are matching any retrieved variants, in that case set the
                // fields: comments, filters, discussion, status and attributes
                Interpretation interpretation = interpretationResult.first();
                if (CollectionUtils.isNotEmpty(interpretation.getPrimaryFindings())) {
                    logger.info("Checking the primary findings ({}) of the interpretation {}", interpretation.getPrimaryFindings().size(),
                            query.getString(ParamConstants.INCLUDE_INTERPRETATION));
                    for (ClinicalVariant primaryFinding : interpretation.getPrimaryFindings()) {
                        for (ClinicalVariant clinicalVariant : clinicalVariants) {
                            if (clinicalVariant.toStringSimple().equals(primaryFinding.toStringSimple())) {
                                // Only it's updated the following fields
                                // Important to note that the results include the "new" clinical evidences
                                clinicalVariant.setComments(primaryFinding.getComments())
                                        .setFilters(primaryFinding.getFilters())
                                        .setDiscussion(primaryFinding.getDiscussion())
                                        .setStatus(primaryFinding.getStatus())
                                        .setAttributes(primaryFinding.getAttributes());

                                // Update clinical evidence review if it is necessary
                                if (CollectionUtils.isNotEmpty(primaryFinding.getEvidences())
                                        && CollectionUtils.isNotEmpty(clinicalVariant.getEvidences())) {
                                    for (ClinicalVariantEvidence primaryFindingEvidence : primaryFinding.getEvidences()) {
                                        for (ClinicalVariantEvidence clinicalVariantEvidence : clinicalVariant.getEvidences()) {
                                            if (matchEvidence(primaryFindingEvidence, clinicalVariantEvidence)) {
                                                clinicalVariantEvidence.setReview(primaryFindingEvidence.getReview());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } else {
                    logger.warn("Interpretation {} does not have any primary finding", includeInterpretation);
                }
            } else {
                if (interpretationResult.getNumResults() <= 0) {
                    logger.warn("Interpretation {} not found when running the clinical variant query", includeInterpretation);
                } else {
                    logger.warn("Multiple interpretations {} were found for ID {} when running the clinical variant query",
                            interpretationResult.getNumResults(), includeInterpretation);
                }
            }
        }

        int dbTime = (int) watch.getTime(TimeUnit.MILLISECONDS);
        result.setTime(variantQueryResult.getTime() + dbTime);
        result.setResults(clinicalVariants);

        return result;
    }

    private boolean matchEvidence(ClinicalVariantEvidence ev1, ClinicalVariantEvidence ev2) {
        // Check panel ID
        if (ev1.getPanelId() != null && ev2.getPanelId() != null && !ev1.getPanelId().equals(ev2.getPanelId())) {
            return false;
        }
        if (StringUtils.isNotEmpty(ev1.getPanelId()) || StringUtils.isNotEmpty(ev2.getPanelId())) {
            return false;
        }
        // Check genomic feature (gene and transcript)
        if (ev1.getGenomicFeature() != null && ev2.getGenomicFeature() != null) {
            GenomicFeature gf1 = ev1.getGenomicFeature();
            GenomicFeature gf2 = ev2.getGenomicFeature();
            if (StringUtils.isNotEmpty(gf1.getId()) && StringUtils.isNotEmpty(gf2.getId())
                    && StringUtils.isNotEmpty(gf1.getTranscriptId()) && StringUtils.isNotEmpty(gf2.getTranscriptId())) {
                if (gf1.getId().equals(gf2.getId()) && gf1.getTranscriptId().equals(gf2.getTranscriptId())) {
                    return true;
                }
            } else {
                if (StringUtils.isNotEmpty(gf1.getId()) && StringUtils.isNotEmpty(gf2.getId())) {
                    if (gf1.getId().equals(gf2.getId())) {
                        return true;
                    }
                } else if (StringUtils.isNotEmpty(gf1.getTranscriptId()) && StringUtils.isNotEmpty(gf2.getTranscriptId())) {
                    if (gf1.getTranscriptId().equals(gf2.getTranscriptId())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /*--------------------------------------------------------------------------*/

    private ClinicalVariant createClinicalVariant(Variant variant, Map<String, Set<String>> genePanelMap,
                                                  InterpretationAnalysisConfiguration config) {
        List<String> panelIds;
        GenomicFeature gFeature;
        List<ClinicalVariantEvidence> evidences = new ArrayList<>();

        if (variant.getAnnotation() != null && CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
            for (ConsequenceType ct: variant.getAnnotation().getConsequenceTypes()) {
                String geneId = StringUtils.isEmpty(ct.getGeneId()) ? ct.getEnsemblGeneId() : ct.getGeneId();
                String transcriptId = StringUtils.isEmpty(ct.getTranscriptId()) ? ct.getEnsemblTranscriptId() : ct.getTranscriptId();
                String featureType = "GENE";
                if (StringUtils.isEmpty(ct.getGeneName())) {
                    featureType = "INTERGENIC";
                    if (CollectionUtils.isNotEmpty(ct.getSequenceOntologyTerms())) {
                        for (SequenceOntologyTerm soTerm : ct.getSequenceOntologyTerms()) {
                            if ("regulatory_region_variant".equals(soTerm.getName())) {
                                featureType = "REGULATORY";
                                break;
                            }
                        }
                    }
                }

                gFeature = new GenomicFeature(geneId, featureType, transcriptId, ct.getGeneName(), ct.getSequenceOntologyTerms(), null);
                panelIds = new ArrayList();
                if (genePanelMap.containsKey(geneId)) {
                    panelIds.addAll(genePanelMap.get(geneId));
                } else if (genePanelMap.containsKey(ct.getGeneName())) {
                    panelIds.addAll(genePanelMap.get(ct.getGeneName()));
                }

                ClinicalVariantEvidence evidence;
                if (CollectionUtils.isNotEmpty(panelIds)) {
                    for (String panelId : panelIds) {
                        evidence = createEvidence(ct, gFeature, panelId, null, null, variant.getAnnotation(), config);
                        evidences.add(evidence);
                    }
                } else if (genePanelMap.size() == 0) {
                    evidence = createEvidence(ct, gFeature, null, null, null, variant.getAnnotation(), config);
                    evidences.add(evidence);
                }
            }
        }

        if (config != null && config.isSkipUntieredVariants() && CollectionUtils.isEmpty(evidences)) {
            return null;
        }

        ClinicalVariant clinicalVariant = new ClinicalVariant(variant.getImpl());
        clinicalVariant.setEvidences(evidences);
        return clinicalVariant;
    }

    /*--------------------------------------------------------------------------*/

    protected ClinicalVariantEvidence createEvidence(ConsequenceType consequenceType, GenomicFeature genomicFeature, String panelId,
                                                     List<ClinicalProperty.ModeOfInheritance> mois, ClinicalProperty.Penetrance penetrance,
                                                     VariantAnnotation annotation, InterpretationAnalysisConfiguration config) {

        ClinicalVariantEvidence clinicalVariantEvidence = new ClinicalVariantEvidence();

        // Genomic feature
        if (genomicFeature != null) {
            clinicalVariantEvidence.setGenomicFeature(genomicFeature);
        }

        // Set panel
        clinicalVariantEvidence.setPanelId(panelId);

        // Mode of inheritance
        if (mois != null) {
            clinicalVariantEvidence.setModeOfInheritances(mois);
        }

        // Penetrance
        if (penetrance != null) {
            clinicalVariantEvidence.setPenetrance(penetrance);
        }

        // Variant classification:
        clinicalVariantEvidence.setClassification(new VariantClassification());

        // Variant classification: ACMG
        List<ClinicalAcmg> acmgs = calculateAcmgClassification(consequenceType, annotation, mois);
        clinicalVariantEvidence.getClassification().setAcmg(acmgs);

        // Variant classification: clinical significance
        clinicalVariantEvidence.getClassification().setClinicalSignificance(computeClinicalSignificance(acmgs));

        // Role in cancer
        clinicalVariantEvidence.setRolesInCancer(getRolesInCancer(annotation));

        return clinicalVariantEvidence;
    }

    public List<ClinicalProperty.RoleInCancer> getRolesInCancer(VariantAnnotation annotation) {
        if (CollectionUtils.isNotEmpty(annotation.getGeneCancerAssociations())) {
            Set<ClinicalProperty.RoleInCancer> roles = new HashSet<>();
            for (GeneCancerAssociation geneCancerAssociation : annotation.getGeneCancerAssociations()) {
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
                return new ArrayList<>(roles);
            }
        }
        return new ArrayList<>();
    }

    private boolean isTier1(String panelId, List<SequenceOntologyTerm> soTerms, InterpretationAnalysisConfiguration config) {
        if (config.getTier1() != null) {
            if (StringUtils.isEmpty(panelId) && config.getTier1().isPanels()) {
                return false;
            }

            if (CollectionUtils.isNotEmpty(config.getTier1().getConsequenceTypes())) {
                Set<String> tierSO = new HashSet<>(config.getTier1().getConsequenceTypes());
                for (SequenceOntologyTerm soTerm : soTerms) {
                    if (tierSO.contains(soTerm.getName())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private boolean isTier2(String panelId, List<SequenceOntologyTerm> soTerms, InterpretationAnalysisConfiguration config) {
        if (config.getTier2() != null) {
            if (StringUtils.isEmpty(panelId) && config.getTier2().isPanels()) {
                return false;
            }

            if (CollectionUtils.isNotEmpty(config.getTier2().getConsequenceTypes())) {
                Set<String> tierSO = new HashSet<>(config.getTier1().getConsequenceTypes());
                for (SequenceOntologyTerm soTerm : soTerms) {
                    if (tierSO.contains(soTerm.getName())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /*--------------------------------------------------------------------------*/
    /*                 Clinical interpretation analysis utils                   */
    /*--------------------------------------------------------------------------*/

    public List<Variant> getDeNovoVariants(String clinicalAnalysisId, String studyId, Query query, QueryOptions queryOptions,
                                           String sessionId)
            throws ToolException, CatalogException, StorageEngineException, IOException {
        logger.debug("Getting DeNovo variants");

        Query currentQuery = new Query(defaultDeNovoQuery).append(STUDY.key(), studyId);

        if (MapUtils.isNotEmpty(query)) {
            currentQuery.putAll(query);
        }

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis(studyId, clinicalAnalysisId, sessionId);
        Individual proband = ClinicalUtils.getProband(clinicalAnalysis);

        OpenCGAResult<Study> studyQueryResult = catalogManager.getStudyManager().get(studyId,
                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key()), sessionId);
        if (studyQueryResult.getNumResults() == 0) {
            throw new ToolException("Study " + studyId + " not found");
        }

        List<Variant> variants;

        String sampleId = proband.getSamples().get(0).getId();
        SampleMetadata sampleMetadata = variantStorageManager.getSampleMetadata(studyQueryResult.first().getFqn(), sampleId, sessionId);
        if (TaskMetadata.Status.READY.equals(sampleMetadata.getMendelianErrorStatus())) {
            logger.debug("Getting precomputed DE NOVO variants");

            // Mendelian errors are pre-calculated
            currentQuery.put(FAMILY.key(), clinicalAnalysis.getFamily().getId());
            currentQuery.put(FAMILY_SEGREGATION.key(), "DeNovo");
            currentQuery.put(INCLUDE_SAMPLE.key(), sampleId);

            logger.debug("Query: {}", currentQuery.safeToString());

            variants = variantStorageManager.get(currentQuery, queryOptions, sessionId).getResults();
        } else {
            // Get pedigree
            Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily(), proband.getId());

            // Discard members from the pedigree that do not have any samples. If we don't do this, we will always assume
            ClinicalUtils.removeMembersWithoutSamples(pedigree, clinicalAnalysis.getFamily());

            // Get the map of <individual ID, sample ID> and update proband information (to be able to navigate to the parents and their
            // samples easily)
            Map<String, String> sampleMap = ClinicalUtils.getSampleMap(clinicalAnalysis, proband);
            Map<String, List<String>> genotypeMap = ModeOfInheritance.deNovo(pedigree);
            List<String> samples = new ArrayList<>();
            List<String> genotypeList = new ArrayList<>();
            for (Map.Entry<String, List<String>> entry : genotypeMap.entrySet()) {
                if (sampleMap.containsKey(entry.getKey())) {
                    genotypeList.add(sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR));
                }
            }
            if (genotypeList.isEmpty()) {
                logger.error("No genotypes found");
                return null;
            }
            currentQuery.put(VariantQueryParam.GENOTYPE.key(), StringUtils.join(genotypeList, ";"));
            samples.add(sampleMap.get(proband.getId()));

            int motherSampleIdx = 1;
            int fatherSampleIdx = 2;

            if (proband.getMother() != null && StringUtils.isNotEmpty(proband.getMother().getId())
                    && sampleMap.containsKey(proband.getMother().getId())) {
                samples.add(sampleMap.get(proband.getMother().getId()));
            } else {
                motherSampleIdx = -1;
                fatherSampleIdx = 1;
            }
            if (proband.getFather() != null && StringUtils.isNotEmpty(proband.getFather().getId())
                    && sampleMap.containsKey(proband.getFather().getId())) {
                samples.add(sampleMap.get(proband.getFather().getId()));
            } else {
                fatherSampleIdx = -1;
            }

            currentQuery.put(INCLUDE_SAMPLE.key(), samples);
            cleanQuery(currentQuery);

            VariantDBIterator iterator = variantStorageManager.iterator(currentQuery, queryOptions, sessionId);
            variants = ModeOfInheritance.deNovo(iterator, 0, motherSampleIdx, fatherSampleIdx);
        }
        logger.debug("DeNovo variants obtained: {}", variants.size());

        return variants;
    }

    public Map<String, List<Variant>> getCompoundHeterozigousVariants(String clinicalAnalysisId, String studyId, Query query,
                                                                      QueryOptions queryOptions, String sessionId)
            throws ToolException, CatalogException, StorageEngineException, IOException {
        logger.debug("Getting Compound Heterozigous variants");

        Query currentQuery = new Query(defaultCompoundHeterozigousQuery).append(STUDY.key(), studyId);

        if (MapUtils.isNotEmpty(query)) {
            currentQuery.putAll(query);
        }

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis(studyId, clinicalAnalysisId, sessionId);
        Individual proband = ClinicalUtils.getProband(clinicalAnalysis);

        // Get pedigree
        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily(), proband.getId());

        // Discard members from the pedigree that do not have any samples. If we don't do this, we will always assume
        ClinicalUtils.removeMembersWithoutSamples(pedigree, clinicalAnalysis.getFamily());

        // Get the map of individual - sample id and update proband information (to be able to navigate to the parents and their
        // samples easily)
        Map<String, String> sampleMap = ClinicalUtils.getSampleMap(clinicalAnalysis, proband);

        Map<String, List<String>> genotypeMap = ModeOfInheritance.compoundHeterozygous(pedigree);

        List<String> samples = new ArrayList<>();
        List<String> genotypeList = new ArrayList<>();

        for (Map.Entry<String, List<String>> entry : genotypeMap.entrySet()) {
            if (sampleMap.containsKey(entry.getKey())) {
                samples.add(sampleMap.get(entry.getKey()));
                genotypeList.add(sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR));
            }
        }

        int probandSampleIdx = samples.indexOf(proband.getSamples().get(0).getId());
        int fatherSampleIdx = -1;
        int motherSampleIdx = -1;
        if (proband.getFather() != null && ListUtils.isNotEmpty(proband.getFather().getSamples())) {
            fatherSampleIdx = samples.indexOf(proband.getFather().getSamples().get(0).getId());
        }
        if (proband.getMother() != null && ListUtils.isNotEmpty(proband.getMother().getSamples())) {
            motherSampleIdx = samples.indexOf(proband.getMother().getSamples().get(0).getId());
        }

        if (genotypeList.isEmpty()) {
            logger.error("No genotypes found");
            return null;
        }

        currentQuery.put(VariantQueryParam.GENOTYPE.key(), StringUtils.join(genotypeList, ";"));
        cleanQuery(currentQuery);

        logger.debug("CH Samples: {}", StringUtils.join(samples, ","));
        logger.debug("CH Proband idx: {}, mother idx: {}, father idx: {}", probandSampleIdx, motherSampleIdx, fatherSampleIdx);
        logger.debug("CH Query: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(currentQuery));
        VariantDBIterator iterator = variantStorageManager.iterator(currentQuery, queryOptions, sessionId);

        return ModeOfInheritance.compoundHeterozygous(iterator, probandSampleIdx, motherSampleIdx, fatherSampleIdx);
    }

    public DefaultClinicalVariantCreator createClinicalVariantCreator(Query query, String assembly, boolean skipUntieredVariants,
                                                                      String sessionId) throws ToolException {
        // Clinical variant creator
        ClinicalProperty.ModeOfInheritance moi = ClinicalProperty.ModeOfInheritance.valueOf(query.getString(FAMILY_SEGREGATION.key(),
                ClinicalProperty.ModeOfInheritance.UNKNOWN.name()));
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

        Disorder disorder = new Disorder().setId(query.getString(FAMILY_DISORDER.key()));
        List<DiseasePanel> diseasePanels = getDiseasePanels(query, sessionId);
        return new DefaultClinicalVariantCreator(disorder, Collections.singletonList(moi), ClinicalProperty.Penetrance.COMPLETE,
                diseasePanels, biotypes, soNames, !skipUntieredVariants);
    }

    public ClinicalAnalysis getClinicalAnalysis(String studyId, String clinicalAnalysisId, String sessionId)
            throws ToolException, CatalogException {
        OpenCGAResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager()
                .get(studyId, clinicalAnalysisId, QueryOptions.empty(), sessionId);

        if (clinicalAnalysisQueryResult.getNumResults() == 0) {
            throw new ToolException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyId);
        }

        ClinicalAnalysis clinicalAnalysis = clinicalAnalysisQueryResult.first();

        if (clinicalAnalysis.getProband() == null || StringUtils.isEmpty(clinicalAnalysis.getProband().getId())) {
            throw new ToolException("Missing proband in clinical analysis " + clinicalAnalysisId);
        }

        return clinicalAnalysis;
    }

    public List<DiseasePanel> getDiseasePanels(Query query, String sessionId) throws ToolException {
        if (!query.containsKey(STUDY.key())) {
            throw new ToolException("Missing study in query");
        }
        List<DiseasePanel> diseasePanels = new ArrayList<>();
        if (!query.containsKey(PANEL.key())) {
            return diseasePanels;
        }
        return getDiseasePanels(query.getString(STUDY.key()), query.getAsStringList(query.getString(PANEL.key())), sessionId);
    }

    public List<DiseasePanel> getDiseasePanels(String studyId, List<String> diseasePanelIds, String sessionId)
            throws ToolException {
        List<DiseasePanel> diseasePanels = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(diseasePanelIds)) {
            OpenCGAResult<org.opencb.opencga.core.models.panel.Panel> queryResults;
            try {
                queryResults = catalogManager.getPanelManager().get(studyId, diseasePanelIds, QueryOptions.empty(),
                        sessionId);
            } catch (CatalogException e) {
                throw new ToolException("Error accessing panel manager", e);
            }

            if (queryResults.getNumResults() != diseasePanelIds.size()) {
                throw new ToolException("The number of disease panels retrieved doesn't match the number of disease panels queried");
            }

            for (org.opencb.opencga.core.models.panel.Panel panel : queryResults.getResults()) {
                diseasePanels.add(panel);
            }
        }
        return diseasePanels;
    }

    public List<ClinicalVariant> getPrimaryFindings(Query query, QueryOptions queryOptions, ClinicalVariantCreator clinicalVariantCreator,
                                                    String sessionId) throws ToolException {
        try {
            VariantQueryResult<Variant> variantQueryResult = getVariantStorageManager().get(query, queryOptions, sessionId);
            List<Variant> variants = variantQueryResult.getResults();

            System.out.println("Number of variants = " + variants.size());
            for (Variant variant : variants) {
                System.out.println(variant.getId());
            }


            return clinicalVariantCreator.create(variants);
        } catch (InterpretationAnalysisException | CatalogException | StorageEngineException | IOException e) {
            throw new ToolException(e);
        }
    }

    public List<ClinicalVariant> getPrimaryFindings(String clinicalAnalysisId, Query query, QueryOptions queryOptions,
                                                    ClinicalVariantCreator clinicalVariantCreator, String sessionId)
            throws ToolException {
        List<Variant> variants;
        List<ClinicalVariant> clinicalVariants;

        String studyId = query.getString(VariantQueryParam.STUDY.key());

        ClinicalProperty.ModeOfInheritance moi = ClinicalProperty.ModeOfInheritance.UNKNOWN;
        if (query.containsKey(FAMILY_SEGREGATION.key())) {
            moi = ClinicalProperty.ModeOfInheritance.valueOf(query.getString(FAMILY_SEGREGATION.key()));
        }
        try {
            switch (moi) {
                case DE_NOVO:
                    variants = getDeNovoVariants(clinicalAnalysisId, studyId, query, queryOptions, sessionId);
                    clinicalVariants = clinicalVariantCreator.create(variants);
                    break;
                case COMPOUND_HETEROZYGOUS:
                    Map<String, List<Variant>> chVariants;
                    chVariants = getCompoundHeterozigousVariants(clinicalAnalysisId, studyId, query,
                            queryOptions, sessionId);
                    clinicalVariants = ClinicalUtils.getCompoundHeterozygousClinicalVariants(chVariants, clinicalVariantCreator);
                    break;
                default:
                    VariantQueryResult<Variant> variantQueryResult = getVariantStorageManager().get(query, queryOptions, sessionId);
                    variants = variantQueryResult.getResults();
                    clinicalVariants = clinicalVariantCreator.create(variants);
                    break;
            }
        } catch (Exception e) {
            throw new ToolException("Error retrieving primary findings variants", e);
        }

        return clinicalVariants;
    }

    public ClinicalAnalyst getAnalyst(String token) throws ToolException {
        try {
            String userId = catalogManager.getUserManager().getUserId(token);
            OpenCGAResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);
            User user = userQueryResult.first();
            return new ClinicalAnalyst(userId, user.getName(), user.getEmail(), "", "");
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    public String getAssembly(String studyId, String sessionId) throws CatalogException {
        String assembly = "";
        OpenCGAResult<Project> projectQueryResult;
        projectQueryResult = catalogManager.getProjectManager().search(new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), studyId),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()), sessionId);
        if (CollectionUtils.isNotEmpty(projectQueryResult.getResults())) {
            assembly = projectQueryResult.first().getOrganism().getAssembly();
        }
        if (StringUtils.isNotEmpty(assembly)) {
            assembly = assembly.toLowerCase();
        }
        return assembly;
    }

    public VariantStorageManager getVariantStorageManager() {
        return variantStorageManager;
    }

    /*--------------------------------------------------------------------------*/
    /*                    P R I V A T E     M E T H O D S                       */
    /*--------------------------------------------------------------------------*/

    // FIXME Class path to a new section in storage-configuration.yml file
    private void init() {
        try {
            this.database = catalogManager.getConfiguration().getDatabasePrefix() + "_clinical";

            this.clinicalVariantEngine = getClinicalStorageEngine();
        } catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            e.printStackTrace();
        }

    }

    private ClinicalVariantEngine getClinicalStorageEngine() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        String clazz = this.storageConfiguration.getClinical().getManager();
        ClinicalVariantEngine storageEngine = (ClinicalVariantEngine) Class.forName(clazz).newInstance();
        storageEngine.setStorageConfiguration(this.storageConfiguration);
        return storageEngine;
    }

    private Query checkQueryPermissions(Query query, String token) throws ClinicalVariantException, CatalogException {
        if (query == null) {
            throw new ClinicalVariantException("Query object is null");
        }

        // Get userId from token and Study numeric IDs from the query
        String userId = catalogManager.getUserManager().getUserId(token);
        List<String> studyIds = getStudyIds(userId, query);

        // If one specific clinical analysis, sample or individual is provided we expect a single valid study as well
        if (isCaseProvided(query)) {
            if (studyIds.size() == 1) {
                // This checks that the user has permission to the clinical analysis, family, sample or individual
                DataResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager()
                        .search(studyIds.get(0), query, QueryOptions.empty(), token);

                if (clinicalAnalysisQueryResult.getResults().isEmpty()) {
                    throw new ClinicalVariantException("Either the ID does not exist or the user does not have permissions to view it");
                } else {
                    if (!query.containsKey(ClinicalVariantEngine.QueryParams.CLINICAL_ANALYSIS_ID.key())) {
                        query.remove(ClinicalVariantEngine.QueryParams.FAMILY.key());
                        query.remove(ClinicalVariantEngine.QueryParams.SAMPLE.key());
                        query.remove(ClinicalVariantEngine.QueryParams.SUBJECT.key());
                        String clinicalAnalysisList = StringUtils.join(
                                clinicalAnalysisQueryResult.getResults().stream().map(ClinicalAnalysis::getId).collect(Collectors.toList()),
                                ",");
                        query.put("clinicalAnalysisId", clinicalAnalysisList);
                    }
                }
            } else {
                throw new ClinicalVariantException("No single valid study provided: "
                        + query.getString(ClinicalVariantEngine.QueryParams.STUDY.key()));
            }
        } else {
            // Get the owner of all the studies
            Set<String> users = new HashSet<>();
            for (String studyFqn : studyIds) {
                users.add(StringUtils.split(studyFqn, "@")[0]);
            }

            // There must be one single owner for all the studies, we do nt allow to query multiple databases
            if (users.size() == 1) {
                Query studyQuery = new Query(StudyDBAdaptor.QueryParams.ID.key(), StringUtils.join(studyIds, ","));
                DataResult<Study> studyQueryResult = catalogManager.getStudyManager().search(studyQuery, QueryOptions.empty(), token);

                // If the user is the owner we do not have to check anything else
                List<String> studyAliases = new ArrayList<>(studyIds.size());
                if (users.contains(userId)) {
                    for (Study study : studyQueryResult.getResults()) {
                        studyAliases.add(study.getAlias());
                    }
                } else {
                    for (Study study : studyQueryResult.getResults()) {
                        for (Group group : study.getGroups()) {
                            if (group.getId().equalsIgnoreCase("@admins") && group.getUserIds().contains(userId)) {
                                studyAliases.add(study.getAlias());
                                break;
                            }
                        }
                    }
                }

                if (studyAliases.isEmpty()) {
                    throw new ClinicalVariantException("This user is not owner or admins for the provided studies");
                } else {
                    query.put(ClinicalVariantEngine.QueryParams.STUDY.key(), StringUtils.join(studyAliases, ","));
                }
            } else {
                throw new ClinicalVariantException("");
            }
        }
        return query;
    }

    private void checkInterpretationPermissions(String study, long interpretationId, String token)
            throws CatalogException, ClinicalVariantException {
        // Get user ID from token and study numeric ID
        String studyId = catalogManager.getStudyManager().get(study, StudyManager.INCLUDE_STUDY_IDS, token).first().getFqn();

        // This checks that the user has permission to this interpretation
        Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATION_ID.key(), interpretationId);
        DataResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager()
                .search(studyId, query, QueryOptions.empty(), token);

        if (clinicalAnalysisQueryResult.getResults().isEmpty()) {
            throw new ClinicalVariantException("Either the interpretation ID (" + interpretationId + ") does not exist or the user does"
                    + " not have access permissions");
        }
    }

    private List<String> getStudyIds(String userId, Query query) throws CatalogException {
        List<String> studyIds = new ArrayList<>();

        if (query != null && query.containsKey(ClinicalVariantEngine.QueryParams.STUDY.key())) {
            String study = query.getString(ClinicalVariantEngine.QueryParams.STUDY.key());
            List<String> studies = Arrays.asList(study.split(","));
            studyIds = catalogManager.getStudyManager().get(studies, StudyManager.INCLUDE_STUDY_IDS, false, userId)
                    .getResults()
                    .stream()
                    .map(Study::getFqn)
                    .collect(Collectors.toList());
        }
        return studyIds;
    }

    private boolean isCaseProvided(Query query) {
        if (query != null) {
            return query.containsKey(ClinicalVariantEngine.QueryParams.CLINICAL_ANALYSIS_ID.key())
                    || query.containsKey(ClinicalVariantEngine.QueryParams.FAMILY.key())
                    || query.containsKey(ClinicalVariantEngine.QueryParams.SUBJECT.key())
                    || query.containsKey(ClinicalVariantEngine.QueryParams.SAMPLE.key());
        }
        return false;
    }

    private void cleanQuery(Query query) {
        if (query.containsKey(VariantQueryParam.GENOTYPE.key())) {
            query.remove(VariantQueryParam.SAMPLE.key());
            query.remove(FAMILY.key());
            query.remove(FAMILY_PHENOTYPE.key());
            query.remove(MODE_OF_INHERITANCE.key());
        }
    }
}
