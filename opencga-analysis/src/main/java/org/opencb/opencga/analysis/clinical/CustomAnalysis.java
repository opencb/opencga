package org.opencb.opencga.analysis.clinical;

import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.OntologyTerm;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.core.Exon;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResponse;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.File;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.User;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.manager.AlignmentStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.*;
import java.util.stream.Collectors;

public class CustomAnalysis extends OpenCgaAnalysis<Interpretation> {

    private Query query;
    private String clinicalAnalysisId;
    private String probandSampleId;
    private String diseaseName;
    private List<String> diseasePanelIds;

    private int maxCoverage;

    private CellBaseClient cellBaseClient;
    private AlignmentStorageManager alignmentStorageManager;

    private final static String SEPARATOR = "__";

    public CustomAnalysis(Query query, String opencgaHome, String studyStr, String token) {
        super(opencgaHome, studyStr, token);

        this.query = query;
//        this.clinicalAnalysisId = clinicalAnalysisId;
//        this.probandSampleId = probandSampleId;
//        this.diseaseName = diseaseName;
//        this.diseasePanelIds = diseasePanelIds;

        this.maxCoverage = 20;

        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.alignmentStorageManager = new AlignmentStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
    }

    @Override
    public AnalysisResult<Interpretation> execute() throws Exception {
        Phenotype phenotype;
        Map<String, ReportedVariant> reportedVariantMap = new HashMap<>();
        ClinicalProperty.ModeOfInheritance moi = null;

        // Check clinical analysis (only when sample proband ID is not provided)
        if (clinicalAnalysisId != null) {
            QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager().get(studyStr,
                    clinicalAnalysisId, QueryOptions.empty(), token);
            if (clinicalAnalysisQueryResult.getNumResults() == 0) {
                throw new AnalysisException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyStr);
            }

            ClinicalAnalysis clinicalAnalysis = clinicalAnalysisQueryResult.first();

            if (clinicalAnalysis.getFamily() == null || StringUtils.isEmpty(clinicalAnalysis.getFamily().getId())) {
                throw new AnalysisException("Missing family in clinical analysis " + clinicalAnalysisId);
            }

            if (clinicalAnalysis.getProband() == null || StringUtils.isEmpty(clinicalAnalysis.getProband().getId())) {
                throw new AnalysisException("Missing proband in clinical analysis " + clinicalAnalysisId);
            }

            org.opencb.opencga.core.models.Individual proband = clinicalAnalysis.getProband();
            if (ListUtils.isEmpty(proband.getSamples())) {
                throw new AnalysisException("Missing samples in proband " + proband.getId() + " in clinical analysis " + clinicalAnalysisId);
            }

            if (proband.getSamples().size() > 1) {
                throw new AnalysisException("Found more than one sample for proband " + proband.getId() + " in clinical analysis "
                        + clinicalAnalysisId);
            }

            // If proband sample ID is not provided, then take it from clinical analysis
            if (probandSampleId == null) {
                probandSampleId = clinicalAnalysis.getProband().getSamples().get(0).getId();
            }

            // If disease is not provided, then take it from clinical analysis
            if (diseaseName == null) {
                OntologyTerm disease = clinicalAnalysis.getDisorder();
                phenotype = new Phenotype(disease.getId(), disease.getName(), disease.getSource(), Phenotype.Status.UNKNOWN);
            }
        }

        // Check disease
        if (diseaseName == null) {
            throw new AnalysisException("Missing disease");
        } else {
            phenotype = new Phenotype("", diseaseName, "", Phenotype.Status.UNKNOWN);
        }

        // Check proband sample ID
        String bamFileId;
        Set<String> lowCoverageByGeneDone;
        List<ReportedLowCoverage> reportedLowCoverages;
        if (probandSampleId == null) {
            throw new AnalysisException("Missing proband sample ID");
        } else {
            // Reported low coverage map
            lowCoverageByGeneDone = new HashSet<>();
            reportedLowCoverages = new ArrayList<>();

            // Look for the bam file of the proband
            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(studyStr, new Query()
                            .append(FileDBAdaptor.QueryParams.SAMPLES.key(), this.probandSampleId)
                            .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM),
                    new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key()), token);
            if (fileQueryResult.getNumResults() > 1) {
                throw new AnalysisException("More than one BAM file found for proband sample ID " + this.probandSampleId
                        + " in clinical analysis " + clinicalAnalysisId);
            }

            bamFileId = fileQueryResult.getNumResults() == 1 ? fileQueryResult.first().getUuid() : null;
        }

        // Get and check panels
        List<Panel> diseasePanels = new ArrayList<>();
        if (diseasePanelIds != null && !diseasePanelIds.isEmpty()) {
            List<QueryResult<Panel>> queryResults = catalogManager.getPanelManager()
                    .get(studyStr, diseasePanelIds, new Query(), QueryOptions.empty(), token);

            if (queryResults.size() != diseasePanelIds.size()) {
                throw new AnalysisException("The number of disease panels retrieved doesn't match the number of " +
                        "disease panels queried");
            }

            for (QueryResult<Panel> queryResult : queryResults) {
                if (queryResult.getNumResults() != 1) {
                    throw new AnalysisException("The number of disease panels retrieved doesn't match the number of " +
                            "disease panels queried");
                }
                diseasePanels.add(queryResult.first());
            }
        } else {
            throw new AnalysisException("Missing disease panels");
        }

        // Get and check moi
        // TODO: get and check mode of inheritance from query or disease panel?

        for (Panel diseasePanel: diseasePanels) {
            Map<String, List<String>> genePenetranceMap = new HashMap<>();

            for (DiseasePanel.GenePanel genePanel : diseasePanel.getDiseasePanel().getGenes()) {
                String key;
                if (StringUtils.isEmpty(genePanel.getModeOfInheritance())) {
                    key = "all";
                } else {
                    if (genePanel.getPenetrance() == null) {
                        key = genePanel.getModeOfInheritance() + SEPARATOR + ClinicalProperty.Penetrance.COMPLETE;
                    } else {
                        key = genePanel.getModeOfInheritance() + SEPARATOR + genePanel.getPenetrance().name();
                    }
                }

                if (!genePenetranceMap.containsKey(key)) {
                    genePenetranceMap.put(key, new ArrayList<>());
                }

                // Add gene id to the list
                genePenetranceMap.get(key).add(genePanel.getId());
            }

            if (bamFileId != null) {
                for (DiseasePanel.GenePanel genePanel : diseasePanel.getDiseasePanel().getGenes()) {
                    String geneName = genePanel.getId();
                    if (!lowCoverageByGeneDone.contains(geneName)) {
                        reportedLowCoverages.addAll(getReportedLowCoverages(geneName, bamFileId, maxCoverage));
                        lowCoverageByGeneDone.add(geneName);
                    }
                }
            }

            ClinicalProperty.Penetrance penetrance;

            for (String key : genePenetranceMap.keySet()) {
                if (key.equals("all")) {
                    penetrance = ClinicalProperty.Penetrance.COMPLETE;
                } else {
                    String[] splitString = key.split(SEPARATOR);

                    // TODO: splitString[0] is a free string, it will never match a valid ClinicalProperty.ModeOfInheritance
                    moi = ClinicalProperty.ModeOfInheritance.valueOf(splitString[0]);
                    penetrance = ClinicalProperty.Penetrance.valueOf(splitString[1]);

                    // Genes
                    query.put(VariantQueryParam.ANNOT_XREF.key(), genePenetranceMap.get(key));
                }

                // Execute query
                VariantQueryResult<Variant> variantQueryResult = variantStorageManager.get(query, QueryOptions.empty(), token);

                // Create reported variants and evetns
                for (Variant variant: variantQueryResult.getResult()) {
                    if (!reportedVariantMap.containsKey(variant.getId())) {
                        reportedVariantMap.put(variant.getId(), new ReportedVariant(variant.getImpl(), 0, new ArrayList<>(),
                                Collections.emptyList(), Collections.emptyMap()));
                    }
                    ReportedVariant reportedVariant = reportedVariantMap.get(variant.getId());

                    // Sanity check
                    if (variant.getAnnotation() != null && ListUtils.isNotEmpty(variant.getAnnotation().getConsequenceTypes())) {
                        for (ConsequenceType ct: variant.getAnnotation().getConsequenceTypes()) {
                            // Create the reported event
                            ReportedEvent reportedEvent = new ReportedEvent()
                                    .setId("JT-PF-" + reportedVariant.getReportedEvents().size())
                                    .setPhenotypes(Collections.singletonList(phenotype))
                                    .setConsequenceTypeIds(Collections.singletonList(ct.getBiotype()))
                                    .setGenomicFeature(new GenomicFeature(ct.getEnsemblGeneId(), ct.getEnsemblTranscriptId(), ct.getGeneName(),
                                            null, null))
                                    .setPanelId(diseasePanel.getDiseasePanel().getId())
                                    .setPenetrance(penetrance);

                            if (moi != null) {
                                     reportedEvent.setModeOfInheritance(moi);
                            }

                            // TODO: add additional reported event fields

                            // Add reported event to the reported variant
                            reportedVariant.getReportedEvents().add(reportedEvent);
                        }
                    }
                }
            }
        }


        String userId = catalogManager.getUserManager().getUserId(token);
        QueryResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);

        List<DiseasePanel> biodataDiseasePanels =
                diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());

        // Create Interpretation
        Interpretation interpretation = new Interpretation()
                .setId("Custom-" + TimeUtils.getTimeMillis())
                .setAnalyst(new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization()))
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(biodataDiseasePanels)
                .setFilters(null) //TODO
                .setSoftware(new Software().setName("Custom"))
                .setReportedVariants(new ArrayList<>(reportedVariantMap.values()))
                .setReportedLowCoverages(reportedLowCoverages);

        // Return interpretation result
        return new AnalysisResult<>(interpretation);
    }

    private List<ReportedLowCoverage> getReportedLowCoverages(String geneName, String bamFileId, int maxCoverage) {
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

    public Query getQuery() {
        return query;
    }

    public CustomAnalysis setQuery(Query query) {
        this.query = query;
        return this;
    }

    public String getClinicalAnalysisId() {
        return clinicalAnalysisId;
    }

    public CustomAnalysis setClinicalAnalysisId(String clinicalAnalysisId) {
        this.clinicalAnalysisId = clinicalAnalysisId;
        return this;
    }

    public String getProbandSampleId() {
        return probandSampleId;
    }

    public CustomAnalysis setProbandSampleId(String probandSampleId) {
        this.probandSampleId = probandSampleId;
        return this;
    }

    public String getDiseaseName() {
        return diseaseName;
    }

    public CustomAnalysis setDiseaseName(String diseaseName) {
        this.diseaseName = diseaseName;
        return this;
    }

    public List<String> getDiseasePanelIds() {
        return diseasePanelIds;
    }

    public CustomAnalysis setDiseasePanelIds(List<String> diseasePanelIds) {
        this.diseasePanelIds = diseasePanelIds;
        return this;
    }

    public int getMaxCoverage() {
        return maxCoverage;
    }

    public CustomAnalysis setMaxCoverage(int maxCoverage) {
        this.maxCoverage = maxCoverage;
        return this;
    }
}
