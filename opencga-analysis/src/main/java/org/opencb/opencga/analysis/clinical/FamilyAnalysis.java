package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.RoleInCancer;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.ReportedLowCoverage;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.clinical.interpretation.exceptions.InterpretationAnalysisException;
import org.opencb.biodata.models.clinical.pedigree.Member;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.core.Exon;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.tools.clinical.ReportedVariantCreator;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.models.*;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.AlignmentStorageManager;
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.ModeOfInheritance.COMPOUND_HETEROZYGOUS;

public abstract class FamilyAnalysis<T> extends OpenCgaAnalysis<T> {

    protected String clinicalAnalysisId;
    protected List<String> diseasePanelIds;

    protected Map<String, RoleInCancer> roleInCancer;
    protected Map<String, List<String>> actionableVariants;

    protected ClinicalProperty.Penetrance penetrance;

    protected ObjectMap config;
    @Deprecated
    protected int maxCoverage;

    protected final static String SEPARATOR = "__";

    public final static int LOW_COVERAGE_DEFAULT = 20;

    public final static String INCLUDE_LOW_COVERAGE_PARAM = "includeLowCoverage";
    public final static String MAX_LOW_COVERAGE_PARAM = "maxLowCoverage";
    public final static String SKIP_DIAGNOSTIC_VARIANTS_PARAM = "skipDiagnosticVariants";
    public final static String SKIP_UNTIERED_VARIANTS_PARAM = "skipUntieredVariants";

    protected CellBaseClient cellBaseClient;
    protected AlignmentStorageManager alignmentStorageManager;

//    protected static Set<String> extendedLof;
//    protected static Set<String> proteinCoding;
//
//    static {
//        proteinCoding = new HashSet<>(Arrays.asList("protein_coding", "IG_C_gene", "IG_D_gene", "IG_J_gene", "IG_V_gene",
//                "nonsense_mediated_decay", "non_stop_decay", "TR_C_gene", "TR_D_gene", "TR_J_gene", "TR_V_gene"));
//
//        extendedLof = new HashSet<>(Arrays.asList("SO:0001893", "SO:0001574", "SO:0001575", "SO:0001587", "SO:0001589", "SO:0001578",
//                "SO:0001582", "SO:0001889", "SO:0001821", "SO:0001822", "SO:0001583", "SO:0001630", "SO:0001626"));
//    }

    public FamilyAnalysis(String clinicalAnalysisId, List<String> diseasePanelIds, Map<String, RoleInCancer> roleInCancer,
                          Map<String, List<String>> actionableVariants, ClinicalProperty.Penetrance penetrance, ObjectMap config,
                          String studyStr, String opencgaHome, String token) {
        super(opencgaHome, studyStr, token);

        this.clinicalAnalysisId = clinicalAnalysisId;
        this.diseasePanelIds = diseasePanelIds;

        this.actionableVariants = actionableVariants;
        this.roleInCancer = roleInCancer;

        this.penetrance = penetrance;

        this.config = config != null ? config : new ObjectMap();
        this.maxCoverage = 20;

        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.alignmentStorageManager = new AlignmentStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));

    }

    @Override
    public abstract AnalysisResult<T> execute() throws Exception;


    protected ClinicalAnalysis getClinicalAnalysis() throws AnalysisException {
        QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult;
        try {
            clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager()
                    .get(studyStr, clinicalAnalysisId, QueryOptions.empty(), token);
        } catch (CatalogException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
        if (clinicalAnalysisQueryResult.getNumResults() == 0) {
            throw new AnalysisException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyStr);
        }

        ClinicalAnalysis clinicalAnalysis = clinicalAnalysisQueryResult.first();

        // Sanity checks
        if (clinicalAnalysis.getFamily() == null || StringUtils.isEmpty(clinicalAnalysis.getFamily().getId())) {
            throw new AnalysisException("Missing family in clinical analysis " + clinicalAnalysisId);
        }

        if (clinicalAnalysis.getProband() == null || StringUtils.isEmpty(clinicalAnalysis.getProband().getId())) {
            throw new AnalysisException("Missing proband in clinical analysis " + clinicalAnalysisId);
        }

        return clinicalAnalysis;
    }

    protected Individual getProband(ClinicalAnalysis clinicalAnalysis) throws AnalysisException {
        Individual proband = clinicalAnalysis.getProband();

        String clinicalAnalysisId = clinicalAnalysis.getId();
        // Sanity checks
        if (proband == null) {
            throw new AnalysisException("Missing proband in clinical analysis " + clinicalAnalysisId);
        }

        if (ListUtils.isEmpty(proband.getSamples())) {
            throw new AnalysisException("Missing samples in proband " + proband.getId() + " in clinical analysis " + clinicalAnalysisId);
        }

        if (proband.getSamples().size() > 1) {
            throw new AnalysisException("Found more than one sample for proband " + proband.getId() + " in clinical analysis "
                    + clinicalAnalysisId);
        }

        // Fill with parent information
        String fatherId = null;
        String motherId = null;
        if (proband.getFather() != null && StringUtils.isNotEmpty(proband.getFather().getId())) {
            fatherId = proband.getFather().getId();
        }
        if (proband.getMother() != null && StringUtils.isNotEmpty(proband.getMother().getId())) {
            motherId = proband.getMother().getId();
        }
        if (fatherId != null && motherId != null && clinicalAnalysis.getFamily() != null
                && ListUtils.isNotEmpty(clinicalAnalysis.getFamily().getMembers())) {
            for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                if (member.getId().equals(fatherId)) {
                    proband.setFather(member);
                } else if (member.getId().equals(motherId)) {
                    proband.setMother(member);
                }
            }
        }

        return proband;
    }


    protected List<String> getSampleNames(ClinicalAnalysis clinicalAnalysis) throws AnalysisException {
        return getSampleNames(clinicalAnalysis, null);
    }

    protected List<String> getSampleNames(ClinicalAnalysis clinicalAnalysis, Individual proband) throws AnalysisException {
        List<String> sampleList = new ArrayList<>();
        // Sanity check
        if (clinicalAnalysis != null && clinicalAnalysis.getFamily() != null
                && CollectionUtils.isNotEmpty(clinicalAnalysis.getFamily().getMembers())) {

            Map<String, Individual> individualMap = new HashMap<>();
            for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                if (ListUtils.isEmpty(member.getSamples())) {
//                    throw new AnalysisException("No samples found for member " + member.getId());
                    continue;
                }
                if (member.getSamples().size() > 1) {
                    throw new AnalysisException("More than one sample found for member " + member.getId());
                }
                sampleList.add(member.getSamples().get(0).getId());
                individualMap.put(member.getId(), member);
            }

            if (proband != null) {
                // Fill proband information to be able to navigate to the parents and their samples easily
                // Sanity check
                if (proband.getFather() != null && StringUtils.isNotEmpty(proband.getFather().getId())
                        && individualMap.containsKey(proband.getFather().getId())) {
                    proband.setFather(individualMap.get(proband.getFather().getId()));
                }
                if (proband.getMother() != null && StringUtils.isNotEmpty(proband.getMother().getId())
                        && individualMap.containsKey(proband.getMother().getId())) {
                    proband.setMother(individualMap.get(proband.getMother().getId()));
                }
            }
        }
        return sampleList;
    }

    protected Map<String, String> getSampleMap(ClinicalAnalysis clinicalAnalysis, Individual proband) throws AnalysisException {
        Map<String, String> individualSampleMap = new HashMap<>();
        // Sanity check
        if (clinicalAnalysis != null && clinicalAnalysis.getFamily() != null
                && CollectionUtils.isNotEmpty(clinicalAnalysis.getFamily().getMembers())) {

            Map<String, Individual> individualMap = new HashMap<>();
            for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                if (ListUtils.isEmpty(member.getSamples())) {
//                    throw new AnalysisException("No samples found for member " + member.getId());
                    continue;
                }
                if (member.getSamples().size() > 1) {
                    throw new AnalysisException("More than one sample found for member " + member.getId());
                }
                individualSampleMap.put(member.getId(), member.getSamples().get(0).getId());
                individualMap.put(member.getId(), member);
            }

            if (proband != null) {
                // Fill proband information to be able to navigate to the parents and their samples easily
                // Sanity check
                if (proband.getFather() != null && StringUtils.isNotEmpty(proband.getFather().getId())
                        && individualMap.containsKey(proband.getFather().getId())) {
                    proband.setFather(individualMap.get(proband.getFather().getId()));
                }
                if (proband.getMother() != null && StringUtils.isNotEmpty(proband.getMother().getId())
                        && individualMap.containsKey(proband.getMother().getId())) {
                    proband.setMother(individualMap.get(proband.getMother().getId()));
                }
            }
        }
        return individualSampleMap;
    }


    protected List<ReportedLowCoverage> getReportedLowCoverage(ClinicalAnalysis clinicalAnalysis, List<DiseasePanel> diseasePanels)
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
            fileQueryResult = catalogManager.getFileManager().get(studyStr, new Query()
                            .append(FileDBAdaptor.QueryParams.SAMPLES.key(), probandId)
                            .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM),
                    new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key()), token);
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
                        reportedLowCoverages.addAll(getReportedLowCoverages(geneName, bamFileId, maxCoverage));
                        lowCoverageByGeneDone.add(geneName);
                    }
                }
            }
        }

        return reportedLowCoverages;
    }

    protected List<ReportedLowCoverage> getReportedLowCoverages(String geneName, String bamFileId, int maxCoverage) {
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();
        try {
            // Get gene exons from CellBase
            QueryResponse<Gene> geneQueryResponse = cellBaseClient.getGeneClient().get(Collections.singletonList(geneName),
                    QueryOptions.empty());
            List<RegionCoverage> regionCoverages;
            for (Transcript transcript: geneQueryResponse.getResponse().get(0).first().getTranscripts()) {
                for (Exon exon: transcript.getExons()) {
                    regionCoverages = alignmentStorageManager.getLowCoverageRegions(studyStr, bamFileId,
                            new Region(exon.getChromosome(), exon.getStart(), exon.getEnd()), maxCoverage, token).getResult();
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

    protected List<DiseasePanel> getDiseasePanelsFromIds(List<String> diseasePanelIds) throws AnalysisException {
        List<DiseasePanel> diseasePanels = new ArrayList<>();
        if (diseasePanelIds != null && !diseasePanelIds.isEmpty()) {
            List<QueryResult<Panel>> queryResults;
            try {
                queryResults = catalogManager.getPanelManager()
                        .get(studyStr, diseasePanelIds, QueryOptions.empty(), token);
            } catch (CatalogException e) {
                throw new AnalysisException(e.getMessage(), e);
            }

            if (queryResults.size() != diseasePanelIds.size()) {
                throw new AnalysisException("The number of disease panels retrieved doesn't match the number of disease panels queried");
            }

            for (QueryResult<Panel> queryResult : queryResults) {
                if (queryResult.getNumResults() != 1) {
                    throw new AnalysisException("The number of disease panels retrieved doesn't match the number of disease panels " +
                            "queried");
                }
                diseasePanels.add(queryResult.first());
            }
        } else {
            throw new AnalysisException("Missing disease panels");
        }

        return diseasePanels;
    }

    protected Analyst getAnalyst(String token) throws AnalysisException {
        try {
            String userId = catalogManager.getUserManager().getUserId(token);
            QueryResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                    Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);

            return new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization());
        } catch (CatalogException e) {
            throw new AnalysisException(e.getMessage(), e);
        }
    }

    protected List<ReportedVariant> getSecondaryFindings(ClinicalAnalysis clinicalAnalysis, List<ReportedVariant> primaryFindings,
                                                         List<String> sampleNames, ReportedVariantCreator creator)
            throws StorageEngineException, InterpretationAnalysisException, CatalogException, IOException {
        List<ReportedVariant> secondaryFindings = null;
        if (clinicalAnalysis.getConsent() != null
                && clinicalAnalysis.getConsent().getSecondaryFindings() == ClinicalConsent.ConsentStatus.YES) {
            List<String> excludeIds = null;
            if (CollectionUtils.isNotEmpty(primaryFindings)) {
                excludeIds = primaryFindings.stream().map(ReportedVariant::getId).collect(Collectors.toList());
            }

            List<Variant> findings = InterpretationAnalysisUtils.secondaryFindings(studyStr, sampleNames, actionableVariants.keySet(),
                    excludeIds, variantStorageManager, token);

            if (CollectionUtils.isNotEmpty(findings)) {
                secondaryFindings = creator.createSecondaryFindings(findings);
            }
        }
        return secondaryFindings;
    }

    protected List<ReportedVariant> getCompoundHeterozygousReportedVariants(Map<String, List<Variant>> chVariantMap,
                                                                            ReportedVariantCreator creator)
            throws InterpretationAnalysisException {
        // Compound heterozygous management
        // Create transcript - reported variant map from transcript - variant
        Map<String, List<ReportedVariant>> reportedVariantMap = new HashMap<>();
        for (Map.Entry<String, List<Variant>> entry : chVariantMap.entrySet()) {
            reportedVariantMap.put(entry.getKey(), creator.create(entry.getValue(), COMPOUND_HETEROZYGOUS));
        }
        return creator.groupCHVariants(reportedVariantMap);
    }


    protected void putGenotypes(Map<String, List<String>> genotypes, Map<String, String> sampleMap, Query query) {
        String genotypeString = StringUtils.join(genotypes.entrySet().stream()
                .filter(entry -> sampleMap.containsKey(entry.getKey()))
                .filter(entry -> ListUtils.isNotEmpty(entry.getValue()))
                .map(entry -> sampleMap.get(entry.getKey()) + ":" + StringUtils.join(entry.getValue(), VariantQueryUtils.OR))
                .collect(Collectors.toList()), ";");
        if (StringUtils.isNotEmpty(genotypeString)) {
            query.put(VariantQueryParam.GENOTYPE.key(), genotypeString);
        }
        try {
            logger.debug("Query: {}", JacksonUtils.getDefaultObjectMapper().writer().writeValueAsString(query));
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    protected void removeMembersWithoutSamples(Pedigree pedigree, Family family) {
        Set<String> membersWithoutSamples = new HashSet<>();
        for (Individual member : family.getMembers()) {
            if (ListUtils.isEmpty(member.getSamples())) {
                membersWithoutSamples.add(member.getId());
            }
        }

        Iterator<Member> iterator = pedigree.getMembers().iterator();
        while (iterator.hasNext()) {
            Member member = iterator.next();
            if (membersWithoutSamples.contains(member.getId())) {
                iterator.remove();
            } else {
                if (member.getFather() != null && membersWithoutSamples.contains(member.getFather().getId())) {
                    member.setFather(null);
                }
                if (member.getMother() != null && membersWithoutSamples.contains(member.getMother().getId())) {
                    member.setMother(null);
                }
            }
        }

        if (pedigree.getProband().getFather() != null && membersWithoutSamples.contains(pedigree.getProband().getFather().getId())) {
            pedigree.getProband().setFather(null);
        }
        if (pedigree.getProband().getMother() != null && membersWithoutSamples.contains(pedigree.getProband().getMother().getId())) {
            pedigree.getProband().setMother(null);
        }

        logger.debug("Pedigree: {}", pedigree);
    }


    protected void cleanQuery(Query query) {
        if (query.containsKey(VariantQueryParam.GENOTYPE.key())) {
            query.remove(VariantQueryParam.SAMPLE.key());
            query.remove(VariantCatalogQueryUtils.FAMILY.key());
            query.remove(VariantCatalogQueryUtils.FAMILY_PHENOTYPE.key());
            query.remove(VariantCatalogQueryUtils.MODE_OF_INHERITANCE.key());
        }
    }

}
