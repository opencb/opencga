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
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Disorder;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.core.Exon;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.ConsequenceTypeMappings;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.biodata.models.variant.avro.SequenceOntologyTerm;
import org.opencb.biodata.tools.clinical.DefaultReportedVariantCreator;
import org.opencb.biodata.tools.clinical.ReportedVariantCreator;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.alignment.AlignmentStorageManager;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.analysis.variant.manager.VariantStorageManager;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.clinical.ClinicalConsent;
import org.opencb.opencga.core.models.clinical.Interpretation;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.project.Project;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.Group;
import org.opencb.opencga.core.models.study.Study;
import org.opencb.opencga.core.models.user.User;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.opencb.opencga.core.response.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantEngine;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.clinical.ReportedVariantIterator;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.analysis.StorageManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.VariantClassification.*;
import static org.opencb.commons.datastore.core.QueryOptions.INCLUDE;
import static org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils.*;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.INCLUDE_SAMPLE;
import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.STUDY;
import static org.opencb.opencga.storage.core.variant.query.VariantQueryUtils.isValidParam;

public class ClinicalInterpretationManager extends StorageManager {

    public static final int LOW_COVERAGE_DEFAULT = 20;
    public static final int DEFAULT_COVERAGE_THRESHOLD = 20;

    private String database;

    private ClinicalAnalysisManager clinicalAnalysisManager;
    private ClinicalVariantEngine clinicalVariantEngine;
    private VariantStorageManager variantStorageManager;

    protected CellBaseClient cellBaseClient;
    protected AlignmentStorageManager alignmentStorageManager;

    private RoleInCancerManager roleInCancerManager;
    private ActionableVariantManager actionableVariantManager;

    private VariantCatalogQueryUtils catalogQueryUtils;

    private static Query defaultDeNovoQuery;
    private static Query defaultCompoundHeterozigousQuery;

    static {
        defaultDeNovoQuery = new Query()
                .append(VariantQueryParam.ANNOT_POPULATION_ALTERNATE_FREQUENCY.key(), "1kG_phase3:AFR<0.002;1kG_phase3:AMR<0.002;"
                        + "1kG_phase3:EAS<0.002;1kG_phase3:EUR<0.002;1kG_phase3:SAS<0.002;GNOMAD_EXOMES:AFR<0.001;GNOMAD_EXOMES:AMR<0.001;"
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

    public ClinicalInterpretationManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        this(catalogManager, storageEngineFactory, null, null);
    }

    public ClinicalInterpretationManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory, Path roleInCancerPath,
                                         Path actionableVariantPath) {
        super(catalogManager, storageEngineFactory);

        this.clinicalAnalysisManager = catalogManager.getClinicalAnalysisManager();
        this.variantStorageManager = new VariantStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));

        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.alignmentStorageManager = new AlignmentStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));

        this.roleInCancerManager = new RoleInCancerManager(roleInCancerPath);
        this.actionableVariantManager = new ActionableVariantManager(actionableVariantPath);

        this.catalogQueryUtils = new VariantCatalogQueryUtils(catalogManager);

//        this.init();
    }


    @Override
    public void testConnection() throws StorageEngineException {
    }

    public DataResult<ReportedVariant> index(String token) throws IOException, ClinicalVariantException {
        return null;
    }

    public DataResult<ReportedVariant> index(String study, String token) throws IOException, ClinicalVariantException, CatalogException {
        DBIterator<ClinicalAnalysis> clinicalAnalysisDBIterator =
                clinicalAnalysisManager.iterator(study, new Query(), QueryOptions.empty(), token);

        while (clinicalAnalysisDBIterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = clinicalAnalysisDBIterator.next();
            for (Interpretation interpretation : clinicalAnalysis.getInterpretations()) {
                interpretation.getAttributes().put("OPENCGA_CLINICAL_ANALYSIS", clinicalAnalysis);

                this.clinicalVariantEngine.insert(interpretation, database);
            }
        }
        return null;
    }

    public DataResult<ReportedVariant> query(Query query, QueryOptions options, String token)
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

    public ReportedVariantIterator iterator(Query query, QueryOptions options, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        query = checkQueryPermissions(query, token);

        return clinicalVariantEngine.iterator(query, options, "");
    }

    public void addInterpretationComment(String study, long interpretationId, Comment comment, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        checkInterpretationPermissions(study, interpretationId, token);

        clinicalVariantEngine.addInterpretationComment(interpretationId, comment, "");
    }

    public void addReportedVariantComment(String study, long interpretationId, String variantId, Comment comment, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        checkInterpretationPermissions(study, interpretationId, token);

        clinicalVariantEngine.addReportedVariantComment(interpretationId, variantId, comment, "");
    }

    /*--------------------------------------------------------------------------*/
    /*  Get clinical variants                                                   */
    /*--------------------------------------------------------------------------*/

    public List<ReportedVariant> get(Query query, QueryOptions queryOptions, String token)
            throws CatalogException, IOException, StorageEngineException {

        List<Variant> variants = variantStorageManager.get(query, queryOptions, token).getResults();
        if (CollectionUtils.isEmpty(variants)) {
            return Collections.emptyList();
        }

        // Get study from query
        String studyId = query.getString(STUDY.key());

        // Get assembly
        String assembly = getAssembly(studyId, token);

        // Prepare map<gene name, set of panel names>
        Map<String, Set<String>> genePanelMap = new HashMap<>();

        if (isValidParam(query, PANEL)) {
            List<String> panels = query.getAsStringList(PANEL.key());
            for (String panelId : panels) {
                Panel panel = catalogQueryUtils.getPanel(studyId, panelId, token);
                for (DiseasePanel.GenePanel genePanel : panel.getGenes()) {
                    if (!genePanelMap.containsKey(genePanel.getName())) {
                        genePanelMap.put(genePanel.getName(), new HashSet<>());
                    }
                    genePanelMap.get(genePanel.getName()).add(panelId);
                }
            }
        }

        Map<String, ClinicalProperty.RoleInCancer> roleInCancer = roleInCancerManager.getRoleInCancer();
        Map<String, List<String>> actionableVariants = actionableVariantManager.getActionableVariants(assembly);

        List<ReportedVariant> clinicalVariants = new ArrayList<>();
        for (Variant variant : variants) {
            clinicalVariants.add(createClinicalVariant(variant, genePanelMap, roleInCancer, actionableVariants));
        }
        return clinicalVariants;
    }

    /*--------------------------------------------------------------------------*/
    /*  Get actionable variants                                                 */
    /*--------------------------------------------------------------------------*/

    public List<ReportedVariant> getActionableVariants(String studyId, String sampleId, String token)
            throws CatalogException, IOException, StorageEngineException {

        List<Variant> variants = new ArrayList<>();

        // Prepare query object
        Query query = new Query();
        query.put(STUDY.key(), studyId);
        query.put(VariantQueryParam.SAMPLE.key(), sampleId);

        // Get the correct actionable variants for the assembly
        String assembly = getAssembly(studyId, token);
        Map<String, List<String>> actionableVariants = actionableVariantManager.getActionableVariants(assembly);
        if (actionableVariants != null) {
            Iterator<String> iterator = actionableVariants.keySet().iterator();
            List<String> variantIds = new ArrayList<>();
            while (iterator.hasNext()) {
                String id = iterator.next();
                variantIds.add(id);
                if (variantIds.size() >= 1000) {
                    query.put(VariantQueryParam.ID.key(), variantIds);
                    VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), token);
                    variants.addAll(result.getResults());
                    variantIds.clear();
                }
            }

            if (variantIds.size() > 0) {
                query.put(VariantQueryParam.ID.key(), variantIds);
                VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), token);
                variants.addAll(result.getResults());
            }
        }

        List<ReportedVariant> clinicalVariants = new ArrayList<>();
        for (Variant variant : variants) {
            clinicalVariants.add(createClinicalVariant(variant, null, roleInCancerManager.getRoleInCancer(), actionableVariants));
        }
        return clinicalVariants;
    }

    /*--------------------------------------------------------------------------*/

    private ReportedVariant createClinicalVariant(Variant variant, Map<String, Set<String>> genePanelMap,
                                                  Map<String, ClinicalProperty.RoleInCancer> roleInCancer,
                                                  Map<String, List<String>> actionableVariants) {
        ReportedVariant reportedVariant = new ReportedVariant(variant.getImpl());

        if (variant.getAnnotation() ==  null) {
            return reportedVariant;
        }

        if (CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {

            List<String> panelIds;
            GenomicFeature gFeature;
            List<ReportedEvent> evidences = new ArrayList<>();

            for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                gFeature = new GenomicFeature(ct.getEnsemblGeneId(), "GENE", ct.getEnsemblTranscriptId(), ct.getGeneName(), null);
                panelIds = null;
                if (genePanelMap.containsKey(ct.getEnsemblGeneId())) {
                    panelIds = new ArrayList<>(genePanelMap.get(ct.getEnsemblGeneId()));
                } else if (genePanelMap.containsKey(ct.getGeneName())) {
                    panelIds = new ArrayList<>(genePanelMap.get(ct.getGeneName()));
                }

                if (CollectionUtils.isNotEmpty(panelIds)) {
                    for (String panelId : panelIds) {
                        evidences.add(createEvidence(ct.getSequenceOntologyTerms(), gFeature, panelId, null, null, null, variant,
                                roleInCancer, actionableVariants));
                    }
                } else {
                    evidences.add(createEvidence(ct.getSequenceOntologyTerms(), gFeature, null, null, null, null, variant, roleInCancer,
                            actionableVariants));
                }
            }
            reportedVariant.setEvidences(evidences);
        }

        return reportedVariant;
    }

    /*--------------------------------------------------------------------------*/

    protected ReportedEvent createEvidence(List<SequenceOntologyTerm> soTerms, GenomicFeature genomicFeature, String panelId,
                                           ClinicalProperty.ModeOfInheritance moi, ClinicalProperty.Penetrance penetrance, String tier,
                                           Variant variant, Map<String, ClinicalProperty.RoleInCancer> roleInCancer,
                                           Map<String, List<String>> actionableVariants) {
        ReportedEvent reportedEvent = new ReportedEvent().setId("OPENCB-" + UUID.randomUUID());

        // Consequence types
        if (CollectionUtils.isNotEmpty(soTerms)) {
            // Set consequence type
            reportedEvent.setConsequenceTypes(soTerms);
        }

        // Genomic feature
        if (genomicFeature != null) {
            reportedEvent.setGenomicFeature(genomicFeature);
        }

        // Panel ID
        if (panelId != null) {
            reportedEvent.setPanelId(panelId);
        }

        // Mode of inheritance
        if (moi != null) {
            reportedEvent.setModeOfInheritance(moi);
        }

        // Penetrance
        if (penetrance != null) {
            reportedEvent.setPenetrance(penetrance);
        }

        // Variant classification:
        reportedEvent.setClassification(new VariantClassification());

        // Variant classification: ACMG
        List<String> acmgs = calculateAcmgClassification(variant, moi);
        reportedEvent.getClassification().setAcmg(acmgs);

        // Variant classification: clinical significance
        reportedEvent.getClassification().setClinicalSignificance(computeClinicalSignificance(acmgs));

        // Role in cancer
        if (variant.getAnnotation() != null && roleInCancer != null) {
            if (MapUtils.isNotEmpty(roleInCancer) && CollectionUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                for (ConsequenceType ct : variant.getAnnotation().getConsequenceTypes()) {
                    if (StringUtils.isNotEmpty(ct.getGeneName()) && roleInCancer.containsKey(ct.getGeneName())) {
                        reportedEvent.setRoleInCancer(roleInCancer.get(ct.getGeneName()));
                        break;
                    }
                }
            }
        }

        // Actionable management
        if (MapUtils.isNotEmpty(actionableVariants) & actionableVariants.containsKey(variant.getId())) {
            reportedEvent.setActionable(true);

            // Set tier 3 only if it is null or untiered
            if (tier == null || UNTIERED.equals(tier)) {
                reportedEvent.getClassification().setTier(TIER_3);
            } else {
                reportedEvent.getClassification().setTier(tier);
            }

            // Add 'actionable' phenotypes
            if (CollectionUtils.isNotEmpty(actionableVariants.get(variant.getId()))) {
                List<Phenotype> phenotypes = new ArrayList<>();
                for (String phenotypeId : actionableVariants.get(variant.getId())) {
                    phenotypes.add(new Phenotype(phenotypeId, phenotypeId, ""));
                }
                if (CollectionUtils.isNotEmpty(phenotypes)) {
                    reportedEvent.setPhenotypes(phenotypes);
                }
            }
        }

        return reportedEvent;
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

    public DefaultReportedVariantCreator createReportedVariantCreator(Query query, String assembly, boolean skipUntieredVariants,
                                                                      String sessionId) throws ToolException {
        // Reported variant creator
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
        try {
            Disorder disorder = new Disorder().setId(query.getString(FAMILY_DISORDER.key()));
            List<DiseasePanel> diseasePanels = getDiseasePanels(query, sessionId);
            return new DefaultReportedVariantCreator(getRoleInCancerManager().getRoleInCancer(),
                    getActionableVariantManager().getActionableVariants(assembly), disorder, moi, ClinicalProperty.Penetrance.COMPLETE,
                    diseasePanels, biotypes, soNames, !skipUntieredVariants);
        } catch (IOException e) {
            throw new ToolException("Error creating reported variant creator", e);
        }
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
            OpenCGAResult<Panel> queryResults;
            try {
                queryResults = catalogManager.getPanelManager().get(studyId, diseasePanelIds, QueryOptions.empty(),
                        sessionId);
            } catch (CatalogException e) {
                throw new ToolException("Error accessing panel manager", e);
            }

            if (queryResults.getNumResults() != diseasePanelIds.size()) {
                throw new ToolException("The number of disease panels retrieved doesn't match the number of disease panels queried");
            }

            for (Panel panel : queryResults.getResults()) {
                diseasePanels.add(panel);
            }
        }
        return diseasePanels;
    }

    public List<ReportedVariant> getPrimaryFindings(Query query, QueryOptions queryOptions, ReportedVariantCreator reportedVariantCreator,
                                                    String sessionId) throws ToolException {
        try {
            VariantQueryResult<Variant> variantQueryResult = getVariantStorageManager().get(query, queryOptions, sessionId);
            List<Variant> variants = variantQueryResult.getResults();

            System.out.println("Number of variants = " + variants.size());
            for (Variant variant : variants) {
                System.out.println(variant.getId());
            }


            return reportedVariantCreator.create(variants);
        } catch (InterpretationAnalysisException | CatalogException | StorageEngineException | IOException e) {
            throw new ToolException(e);
        }
    }

    public List<ReportedVariant> getPrimaryFindings(String clinicalAnalysisId, Query query, QueryOptions queryOptions,
                                                    ReportedVariantCreator reportedVariantCreator, String sessionId)
            throws ToolException {
        List<Variant> variants;
        List<ReportedVariant> reportedVariants;

        String studyId = query.getString(VariantQueryParam.STUDY.key());

        ClinicalProperty.ModeOfInheritance moi = ClinicalProperty.ModeOfInheritance.UNKNOWN;
        if (query.containsKey(FAMILY_SEGREGATION.key())) {
            moi = ClinicalProperty.ModeOfInheritance.valueOf(query.getString(FAMILY_SEGREGATION.key()));
        }
        try {
            switch (moi) {
                case DE_NOVO:
                    variants = getDeNovoVariants(clinicalAnalysisId, studyId, query, queryOptions, sessionId);
                    reportedVariants = reportedVariantCreator.create(variants);
                    break;
                case COMPOUND_HETEROZYGOUS:
                    Map<String, List<Variant>> chVariants;
                    chVariants = getCompoundHeterozigousVariants(clinicalAnalysisId, studyId, query,
                            queryOptions, sessionId);
                    reportedVariants = ClinicalUtils.getCompoundHeterozygousReportedVariants(chVariants, reportedVariantCreator);
                    break;
                default:
                    VariantQueryResult<Variant> variantQueryResult = getVariantStorageManager().get(query, queryOptions, sessionId);
                    variants = variantQueryResult.getResults();
                    reportedVariants = reportedVariantCreator.create(variants);
                    break;
            }
        } catch (Exception e) {
            throw new ToolException("Error retrieving primary findings variants", e);
        }

        return reportedVariants;
    }

    public List<ReportedVariant> getSecondaryFindings(String studyId, String sampleId, ReportedVariantCreator reportedVariantCreator,
                                                      String sessionId) throws ToolException {
        try {
            List<Variant> variants = new ArrayList<>();

            // Prepare query object
            Query query = new Query();
            query.put(STUDY.key(), studyId);
            query.put(VariantQueryParam.SAMPLE.key(), sampleId);

            // Get the correct actionable variants for the assembly
            String assembly = getAssembly(studyId, sessionId);
            Map<String, List<String>> actionableVariants = actionableVariantManager.getActionableVariants(assembly);
            if (actionableVariants != null) {
                Iterator<String> iterator = actionableVariants.keySet().iterator();
                List<String> variantIds = new ArrayList<>();
                while (iterator.hasNext()) {
                    String id = iterator.next();
                    variantIds.add(id);
                    if (variantIds.size() >= 1000) {
                        query.put(VariantQueryParam.ID.key(), variantIds);
                        VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), sessionId);
                        variants.addAll(result.getResults());
                        variantIds.clear();
                    }
                }

                if (variantIds.size() > 0) {
                    query.put(VariantQueryParam.ID.key(), variantIds);
                    VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), sessionId);
                    variants.addAll(result.getResults());
                }
            }

            return reportedVariantCreator.create(variants);
        } catch (InterpretationAnalysisException | IOException | CatalogException | StorageEngineException e) {
            throw new ToolException(e);
        }
    }

    public List<Variant> getSecondaryFindings(String inputSampleId, String clinicalAnalysisId, String studyId, String sessionId)
            throws CatalogException, ToolException, IOException, StorageEngineException {
        String sampleId = inputSampleId;
        // sampleId has preference over clinicalAnalysisId
        if (StringUtils.isEmpty(sampleId)) {
            // Throws an Exception if it cannot fetch analysis ID or proband is null
            ClinicalAnalysis clinicalAnalysis = getClinicalAnalysis(studyId, clinicalAnalysisId, sessionId);
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getProband().getSamples())) {
                for (Sample sample : clinicalAnalysis.getProband().getSamples()) {
                    if (!sample.isSomatic()) {
                        sampleId = clinicalAnalysis.getProband().getSamples().get(0).getId();
                        break;
                    }
                }
            } else {
                throw new ToolException("Missing germline sample");
            }
        }

        List<Variant> variants = new ArrayList<>();

        // Prepare query object
        Query query = new Query();
        query.put(STUDY.key(), studyId);
        query.put(VariantQueryParam.SAMPLE.key(), sampleId);

        // Get the correct actionable variants for the assembly
        String assembly = getAssembly(studyId, sessionId);
        Map<String, List<String>> actionableVariants = actionableVariantManager.getActionableVariants(assembly);
        if (actionableVariants != null) {
            Iterator<String> iterator = actionableVariants.keySet().iterator();
            List<String> variantIds = new ArrayList<>();
            while (iterator.hasNext()) {
                String id = iterator.next();
                variantIds.add(id);
                if (variantIds.size() >= 1000) {
                    query.put(VariantQueryParam.ID.key(), variantIds);
                    VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), sessionId);
                    variants.addAll(result.getResults());
                    variantIds.clear();
                }
            }

            if (variantIds.size() > 0) {
                query.put(VariantQueryParam.ID.key(), variantIds);
                VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), sessionId);
                variants.addAll(result.getResults());
            }
        }

        return variants;
    }

    public List<ReportedVariant> getSecondaryFindings(ClinicalAnalysis clinicalAnalysis,  List<String> sampleNames,
                                                      String studyId, ReportedVariantCreator creator, String sessionId)
            throws StorageEngineException, ToolException, CatalogException, IOException {
        List<ReportedVariant> secondaryFindings = null;
        if (clinicalAnalysis.getConsent() != null
                && clinicalAnalysis.getConsent().getSecondaryFindings() == ClinicalConsent.ConsentStatus.YES) {
            List<Variant> variants = getSecondaryFindings(sampleNames.get(0), clinicalAnalysis.getId(), studyId,
                    sessionId);
            if (CollectionUtils.isNotEmpty(variants)) {
                secondaryFindings = creator.createSecondaryFindings(variants);
            }
        }
        return secondaryFindings;
    }

    public List<ReportedLowCoverage> getReportedLowCoverage(int maxCoverage, ClinicalAnalysis clinicalAnalysis,
                                                            List<DiseasePanel> diseasePanels, String studyId, String sessionId)
            throws ToolException {
        String clinicalAnalysisId = clinicalAnalysis.getId();

        // Sanity check
        if (clinicalAnalysis.getProband() == null || CollectionUtils.isEmpty(clinicalAnalysis.getProband().getSamples())) {
            throw new ToolException("Missing proband when computing reported low coverage");
        }
        String probandId;
        try {
            probandId = clinicalAnalysis.getProband().getSamples().get(0).getId();
        } catch (Exception e) {
            throw new ToolException("Missing proband when computing reported low coverage", e);
        }

        // Reported low coverage map
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();

        Set<String> lowCoverageByGeneDone = new HashSet<>();

        // Look for the bam file of the proband
        OpenCGAResult<File> fileQueryResult;
        try {
            fileQueryResult = catalogManager.getFileManager().get(studyId, null, new Query()
                            .append(FileDBAdaptor.QueryParams.SAMPLES.key(), probandId)
                            .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM),
                    new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key()), false, sessionId);
        } catch (CatalogException e) {
            throw new ToolException(e.getMessage(), e);
        }
        if (fileQueryResult.getNumResults() > 1) {
            throw new ToolException("More than one BAM file found for proband " + probandId + " in clinical analysis "
                    + clinicalAnalysisId);
        }

        String bamFileId = fileQueryResult.getNumResults() == 1 ? fileQueryResult.first().getUuid() : null;

        if (bamFileId != null) {
            for (DiseasePanel diseasePanel : diseasePanels) {
                for (DiseasePanel.GenePanel genePanel : diseasePanel.getGenes()) {
                    String geneName = genePanel.getId();
                    if (!lowCoverageByGeneDone.contains(geneName)) {
                        reportedLowCoverages.addAll(getReportedLowCoverages(geneName, bamFileId, maxCoverage, studyId,
                                sessionId));
                        lowCoverageByGeneDone.add(geneName);
                    }
                }
            }
        }

        return reportedLowCoverages;
    }

    public List<ReportedLowCoverage> getReportedLowCoverages(String geneName, String bamFileId, int maxCoverage, String studyId,
                                                             String sessionId) {
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();
        try {
            // Get gene exons from CellBase
            QueryResponse<Gene> geneQueryResponse = cellBaseClient.getGeneClient().get(Collections.singletonList(geneName),
                    QueryOptions.empty());
            List<RegionCoverage> regionCoverages;
            for (Transcript transcript: geneQueryResponse.getResponse().get(0).first().getTranscripts()) {
                for (Exon exon: transcript.getExons()) {
                    Region region = new Region(exon.getChromosome(), exon.getStart(), exon.getEnd());
                    regionCoverages = alignmentStorageManager.coverageQuery(studyId, bamFileId, region, 0, maxCoverage, 1,
                            sessionId).getResults();
                    for (RegionCoverage regionCoverage: regionCoverages) {
                        ReportedLowCoverage reportedLowCoverage = new ReportedLowCoverage(regionCoverage)
                                .setGeneName(geneName)
                                .setId(exon.getId());
                        reportedLowCoverages.add(reportedLowCoverage);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error getting low coverage regions for panel genes.", e.getMessage());
        }
        // And for that exon regions, get low coverage regions
        return reportedLowCoverages;
    }

    public Analyst getAnalyst(String token) throws ToolException {
        try {
            String userId = catalogManager.getUserManager().getUserId(token);
            OpenCGAResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);

            return new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization());
        } catch (CatalogException e) {
            throw new ToolException(e);
        }
    }

    public String getAssembly(String studyId, String sessionId) throws CatalogException {
        String assembly = "";
        OpenCGAResult<Project> projectQueryResult;
        projectQueryResult = catalogManager.getProjectManager().get(new Query(ProjectDBAdaptor.QueryParams.STUDY.key(), studyId),
                new QueryOptions(QueryOptions.INCLUDE, ProjectDBAdaptor.QueryParams.ORGANISM.key()), sessionId);
        if (CollectionUtils.isNotEmpty(projectQueryResult.getResults())) {
            assembly = projectQueryResult.first().getOrganism().getAssembly();
        }
        if (StringUtils.isNotEmpty(assembly)) {
            assembly = assembly.toLowerCase();
        }
        return assembly;
    }

    public Interpretation generateInterpretation(String analysisName, String clinicalAnalysisId, Query query,
                                                 List<ReportedVariant> primaryFindings, List<ReportedVariant> secondaryFindings,
                                                 List<DiseasePanel> diseasePanels, List<ReportedLowCoverage> reportedLowCoverages,
                                                 String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        OpenCGAResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), sessionId);

        // Create Interpretation
        return new Interpretation()
                .setId(analysisName + "__" + TimeUtils.getTimeMillis())
                .setPrimaryFindings(primaryFindings)
                .setSecondaryFindings(secondaryFindings)
                .setLowCoverageRegions(reportedLowCoverages)
                .setAnalyst(new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization()))
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(diseasePanels)
                .setFilters(query)
                .setSoftware(new Software().setName(analysisName));
    }

    public void calculateLowCoverageRegions(String studyId, List<String> inputGenes, List<DiseasePanel> diseasePanels,
                                            int maxCoverage, String probandSampleId, Map<String, List<File>> files,
                                            List<ReportedLowCoverage> reportedLowCoverages, String sessionId) {
        String bamFileId = null;
        if (files != null) {
            for (String sampleId : files.keySet()) {
                if (sampleId.equals(probandSampleId)) {
                    for (File file : files.get(sampleId)) {
                        if (File.Format.BAM.equals(file.getFormat())) {
                            bamFileId = file.getUuid();
                        }
                    }
                }
            }
        }

        if (bamFileId != null) {
            // We need the genes from Query.gene and Query.panel
            Set<String> genes = new HashSet<>(inputGenes);
            if (CollectionUtils.isNotEmpty(diseasePanels)) {
                for (DiseasePanel diseasePanel : diseasePanels) {
                    if (CollectionUtils.isNotEmpty(diseasePanel.getGenes())) {
                        genes.addAll(diseasePanel.getGenes().stream().map(DiseasePanel.GenePanel::getId).collect(Collectors.toList()));
                    }
                }
            }

            // Compute low coverage for genes found
            Iterator<String> iterator = genes.iterator();
            while (iterator.hasNext()) {
                String geneName = iterator.next();
                List<ReportedLowCoverage> lowCoverages = getReportedLowCoverages(geneName, bamFileId, maxCoverage, studyId, sessionId);
                if (ListUtils.isNotEmpty(lowCoverages)) {
                    reportedLowCoverages.addAll(lowCoverages);
                }
            }
        }
    }

    public RoleInCancerManager getRoleInCancerManager() {
        return roleInCancerManager;
    }

    public ActionableVariantManager getActionableVariantManager() {
        return actionableVariantManager;
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
                DataResult<Study> studyQueryResult = catalogManager.getStudyManager().get(studyQuery, QueryOptions.empty(), token);

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
        String userId = catalogManager.getUserManager().getUserId(token);
        String studyId = catalogManager.getStudyManager().resolveId(study, userId).getFqn();

        // This checks that the user has permission to this interpretation
        Query query = new Query(ClinicalAnalysisDBAdaptor.QueryParams.INTERPRETATIONS_ID.key(), interpretationId);
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
            studyIds = catalogManager.getStudyManager().resolveIds(studies, userId)
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
