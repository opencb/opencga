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
package org.opencb.opencga.storage.core.manager.clinical;

import htsjdk.variant.vcf.VCFConstants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.core.Exon;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.ReportedVariantCreator;
import org.opencb.biodata.tools.pedigree.ModeOfInheritance;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.FacetQueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantEngine;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.clinical.ReportedVariantIterator;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.AlignmentStorageManager;
import org.opencb.opencga.storage.core.manager.StorageManager;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.manager.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.metadata.models.SampleMetadata;
import org.opencb.opencga.storage.core.metadata.models.TaskMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.oskar.analysis.exceptions.AnalysisException;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam.INCLUDE_SAMPLE;

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

        this.init();
    }


    @Override
    public void testConnection() throws StorageEngineException {
    }

    public QueryResult<ReportedVariant> index(String token) throws IOException, ClinicalVariantException {
        return null;
    }

    public QueryResult<ReportedVariant> index(String study, String token) throws IOException, ClinicalVariantException, CatalogException {
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

    public QueryResult<ReportedVariant> query(Query query, QueryOptions options, String token)
            throws IOException, ClinicalVariantException, CatalogException {
        // Check permissions
        query = checkQueryPermissions(query, token);

        return clinicalVariantEngine.query(query, options, "");
    }

//    public QueryResult<Interpretation> interpretationQuery(Query query, QueryOptions options, String token)
//            throws IOException, ClinicalVariantException, CatalogException {
//        // Check permissions
//        query = checkQueryPermissions(query, token);
//
//        return clinicalVariantEngine.interpretationQuery(query, options, "");
//    }

    public FacetQueryResult facet(Query query, QueryOptions queryOptions, String token)
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
    /*                 Clinical interpretation analysis utils                   */
    /*--------------------------------------------------------------------------*/

    public List<Variant> getDeNovoVariants(String clinicalAnalysisId, String studyId, Query inputQuery, String sessionId)
            throws AnalysisException, CatalogException, StorageEngineException, IOException {
        logger.debug("Getting DeNovo variants");

        Query query = new Query(defaultDeNovoQuery).append(VariantQueryParam.STUDY.key(), studyId);

        if (MapUtils.isNotEmpty(inputQuery)) {
            query.putAll(inputQuery);
        }

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = ClinicalUtils.getClinicalAnalysis(studyId, clinicalAnalysisId, catalogManager, sessionId);
        Individual proband = ClinicalUtils.getProband(clinicalAnalysis);

        QueryResult<Study> studyQueryResult = catalogManager.getStudyManager().get(studyId,
                new QueryOptions(QueryOptions.INCLUDE, StudyDBAdaptor.QueryParams.FQN.key()), sessionId);
        if (studyQueryResult.getNumResults() == 0) {
            throw new AnalysisException("Study " + studyId + " not found");
        }

        List<Variant> variants;

        String sampleId = proband.getSamples().get(0).getId();
        SampleMetadata sampleMetadata = variantStorageManager.getSampleMetadata(studyQueryResult.first().getFqn(), sampleId, sessionId);
        if (TaskMetadata.Status.READY.equals(sampleMetadata.getMendelianErrorStatus())) {
            logger.debug("Getting precomputed DE NOVO variants");

            // Mendelian errors are pre-calculated
            query.put(VariantCatalogQueryUtils.FAMILY.key(), clinicalAnalysis.getFamily().getId());
            query.put(VariantCatalogQueryUtils.FAMILY_SEGREGATION.key(), "DeNovo");
            query.put(INCLUDE_SAMPLE.key(), sampleId);

            logger.debug("Query: {}", query.safeToString());

            variants = variantStorageManager.get(query, QueryOptions.empty(), sessionId).getResult();
        } else {
            // Get pedigree
            Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily(), proband.getId());

            // Discard members from the pedigree that do not have any samples. If we don't do this, we will always assume
            ClinicalUtils.removeMembersWithoutSamples(pedigree, clinicalAnalysis.getFamily());

            // Get the map of individual - sample id and update proband information (to be able to navigate to the parents and their
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
            query.put(VariantQueryParam.GENOTYPE.key(), StringUtils.join(genotypeList, ";"));
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

            query.put(INCLUDE_SAMPLE.key(), samples);
            cleanQuery(query);

            VariantDBIterator iterator = variantStorageManager.iterator(query, QueryOptions.empty(), sessionId);
            variants = ModeOfInheritance.deNovo(iterator, 0, motherSampleIdx, fatherSampleIdx);
        }
        logger.debug("DeNovo variants obtained: {}", variants.size());

        return variants;
    }

    public Map<String, List<Variant>> getCompoundHeterozigousVariants(String clinicalAnalysisId, String studyId, Query inputQuery,
                                                                      String sessionId)
            throws AnalysisException, CatalogException, StorageEngineException, IOException {
        logger.debug("Getting Compound Heterozigous variants");

        Query query = new Query(defaultCompoundHeterozigousQuery).append(VariantQueryParam.STUDY.key(), studyId);

        if (MapUtils.isNotEmpty(inputQuery)) {
            query.putAll(inputQuery);
        }

        // Get and check clinical analysis and proband
        ClinicalAnalysis clinicalAnalysis = ClinicalUtils.getClinicalAnalysis(studyId, clinicalAnalysisId, catalogManager, sessionId);
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

        query.put(VariantQueryParam.GENOTYPE.key(), StringUtils.join(genotypeList, ";"));
        cleanQuery(query);

        logger.debug("CH Samples: {}", StringUtils.join(samples, ","));
        logger.debug("CH Proband idx: {}, mother idx: {}, father idx: {}", probandSampleIdx, motherSampleIdx, fatherSampleIdx);
        logger.debug("CH Query: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(query));
        VariantDBIterator iterator = variantStorageManager.iterator(query, QueryOptions.empty(), sessionId);

        return ModeOfInheritance.compoundHeterozygous(iterator, probandSampleIdx, motherSampleIdx, fatherSampleIdx);
    }

    public List<Variant> getSecondaryFindings(String inputSampleId, String clinicalAnalysisId, String studyId, String sessionId)
            throws CatalogException, AnalysisException, IOException, StorageEngineException {
        String sampleId = inputSampleId;
        // sampleId has preference over clinicalAnalysisId
        if (StringUtils.isEmpty(sampleId)) {
            // Throws an Exception if it cannot fetch analysis ID or proband is null
            ClinicalAnalysis clinicalAnalysis = ClinicalUtils.getClinicalAnalysis(studyId, clinicalAnalysisId, catalogManager, sessionId);
            if (CollectionUtils.isNotEmpty(clinicalAnalysis.getProband().getSamples())) {
                for (Sample sample : clinicalAnalysis.getProband().getSamples()) {
                    if (!sample.isSomatic()) {
                        sampleId = clinicalAnalysis.getProband().getSamples().get(0).getId();
                        break;
                    }
                }
            } else {
                throw new AnalysisException("Missing germline sample");
            }
        }

        List<Variant> variants = new ArrayList<>();

        // Prepare query object
        Query query = new Query();
        query.put(VariantQueryParam.STUDY.key(), studyId);
        query.put(VariantQueryParam.SAMPLE.key(), sampleId);

        // Get the correct actionable variants for the assembly
        String assembly = ClinicalUtils.getAssembly(catalogManager, studyId, sessionId);
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
                    variants.addAll(result.getResult());
                    variantIds.clear();
                }
            }

            if (variantIds.size() > 0) {
                query.put(VariantQueryParam.ID.key(), variantIds);
                VariantQueryResult<Variant> result = variantStorageManager.get(query, QueryOptions.empty(), sessionId);
                variants.addAll(result.getResult());
            }
        }

        return variants;
    }

    public List<ReportedVariant> getSecondaryFindings(ClinicalAnalysis clinicalAnalysis,  List<String> sampleNames,
                                                      String studyId, ReportedVariantCreator creator, String sessionId) throws Exception {
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

    public List<ReportedLowCoverage> getReportedLowCoverage(ClinicalAnalysis clinicalAnalysis, List<DiseasePanel> diseasePanels,
                                                            String studyId, String sessionId)
            throws AnalysisException {
        String clinicalAnalysisId = clinicalAnalysis.getId();

        // Sanity check
        if (clinicalAnalysis.getProband() == null || CollectionUtils.isEmpty(clinicalAnalysis.getProband().getSamples())) {
            throw new AnalysisException("Missing proband when computing reported low coverage");
        }
        String probandId;
        try {
            probandId = clinicalAnalysis.getProband().getSamples().get(0).getId();
        } catch (Exception e) {
            throw new AnalysisException("Missing proband when computing reported low coverage", e);
        }

        // Reported low coverage map
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();

        Set<String> lowCoverageByGeneDone = new HashSet<>();

        // Look for the bam file of the proband
        QueryResult<File> fileQueryResult;
        try {
            fileQueryResult = catalogManager.getFileManager().get(studyId, new Query()
                            .append(FileDBAdaptor.QueryParams.SAMPLES.key(), probandId)
                            .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM),
                    new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key()), sessionId);
        } catch (CatalogException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (fileQueryResult.getNumResults() > 1) {
            throw new AnalysisException("More than one BAM file found for proband " + probandId + " in clinical analysis "
                    + clinicalAnalysisId);
        }

        String bamFileId = fileQueryResult.getNumResults() == 1 ? fileQueryResult.first().getUuid() : null;

        if (bamFileId != null) {
            for (DiseasePanel diseasePanel : diseasePanels) {
                for (DiseasePanel.GenePanel genePanel : diseasePanel.getGenes()) {
                    String geneName = genePanel.getId();
                    if (!lowCoverageByGeneDone.contains(geneName)) {
                        reportedLowCoverages.addAll(getReportedLowCoverages(geneName, bamFileId, DEFAULT_COVERAGE_THRESHOLD, studyId,
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
                    regionCoverages = alignmentStorageManager.getLowCoverageRegions(studyId, bamFileId,
                            new Region(exon.getChromosome(), exon.getStart(), exon.getEnd()), maxCoverage, sessionId).getResult();
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

    public Interpretation generateInterpretation(String analysisName, String clinicalAnalysisId, Query query,
                                                 List<ReportedVariant> primaryFindings, List<ReportedVariant> secondaryFindings,
                                                 List<DiseasePanel> diseasePanels, List<ReportedLowCoverage> reportedLowCoverages,
                                                 String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        QueryResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
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
                QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager()
                        .get(studyIds.get(0), query, QueryOptions.empty(), token);

                if (clinicalAnalysisQueryResult.getResult().isEmpty()) {
                    throw new ClinicalVariantException("Either the ID does not exist or the user does not have permissions to view it");
                } else {
                    if (!query.containsKey(ClinicalVariantEngine.QueryParams.CLINICAL_ANALYSIS_ID.key())) {
                        query.remove(ClinicalVariantEngine.QueryParams.FAMILY.key());
                        query.remove(ClinicalVariantEngine.QueryParams.SAMPLE.key());
                        query.remove(ClinicalVariantEngine.QueryParams.SUBJECT.key());
                        String clinicalAnalysisList = StringUtils.join(
                                clinicalAnalysisQueryResult.getResult().stream().map(ClinicalAnalysis::getId).collect(Collectors.toList()),
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
                QueryResult<Study> studyQueryResult = catalogManager.getStudyManager().get(studyQuery, QueryOptions.empty(), token);

                // If the user is the owner we do not have to check anything else
                List<String> studyAliases = new ArrayList<>(studyIds.size());
                if (users.contains(userId)) {
                    for (Study study : studyQueryResult.getResult()) {
                        studyAliases.add(study.getAlias());
                    }
                } else {
                    for (Study study : studyQueryResult.getResult()) {
                        for (Group group : study.getGroups()) {
                            if (group.getName().equalsIgnoreCase("admins") && group.getUserIds().contains(userId)) {
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
        QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager()
                .get(studyId, query, QueryOptions.empty(), token);

        if (clinicalAnalysisQueryResult.getResult().isEmpty()) {
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
            query.remove(VariantCatalogQueryUtils.FAMILY.key());
            query.remove(VariantCatalogQueryUtils.FAMILY_PHENOTYPE.key());
            query.remove(VariantCatalogQueryUtils.MODE_OF_INHERITANCE.key());
        }
    }
}
