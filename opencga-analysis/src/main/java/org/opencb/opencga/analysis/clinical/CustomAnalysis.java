package org.opencb.opencga.analysis.clinical;

import org.opencb.biodata.models.alignment.RegionCoverage;
import org.opencb.biodata.models.clinical.interpretation.*;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.models.core.Exon;
import org.opencb.biodata.models.core.Gene;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.core.Transcript;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.OpenCgaAnalysis;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
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

    private String clinicalAnalysisId;
    private Query query;
    private ObjectMap options;

    private int maxCoverage;

    private CellBaseClient cellBaseClient;
    private AlignmentStorageManager alignmentStorageManager;

    private final static String SEPARATOR = "__";

    public CustomAnalysis(String clinicalAnalysisId, Query query, String studyStr, String opencgaHome, ObjectMap options, String token) {
        this(query, opencgaHome, studyStr, options, token);

        this.clinicalAnalysisId = clinicalAnalysisId;
    }

    public CustomAnalysis(Query query, String studyStr, String opencgaHome, ObjectMap options, String token) {
        super(opencgaHome, studyStr, token);

        this.query = query;
        this.options = options;

        this.maxCoverage = 20;

        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.alignmentStorageManager = new AlignmentStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
    }

    @Override
    public AnalysisResult<Interpretation> execute() throws Exception {
        String probandSampleId = null;
        Phenotype phenotype = null;
        Map<String, ReportedVariant> reportedVariantMap = new HashMap<>();
        ClinicalProperty.ModeOfInheritance moi = null;

        Map<String, List<File>> files = null;

        // We allow query to be empty, it is likely that we will add some filters from CA
        if (query == null) {
            query = new Query(VariantQueryParam.STUDY.key(), studyStr);
        }

        if (!query.containsKey(VariantQueryParam.STUDY.key())) {
            query.put(VariantQueryParam.STUDY.key(), studyStr);
        }


        // Check clinical analysis (only when sample proband ID is not provided)
        if (clinicalAnalysisId != null) {
            QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager().get(studyStr,
                    clinicalAnalysisId, QueryOptions.empty(), token);
            if (clinicalAnalysisQueryResult.getNumResults() != 1) {
                throw new AnalysisException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyStr);
            }

            ClinicalAnalysis clinicalAnalysis = clinicalAnalysisQueryResult.first();

            // Proband ID
            if (clinicalAnalysis.getProband() != null) {
                probandSampleId = clinicalAnalysis.getProband().getId();
            }

            // Family parameter
            if (clinicalAnalysis.getFamily() != null) {
                // Query contains a different family than ClinicAnalysis
                if (query.containsKey("family") && !clinicalAnalysis.getFamily().getId().equals(query.get("family"))) {
                    logger.warn("Two families passed");
                } else {
                    query.put("family", clinicalAnalysis.getFamily().getId());
                }
            } else {
                // Individual parameter
                if (clinicalAnalysis.getProband() != null) {
                    // Query contains a different sample than ClinicAnalysis
                    if (query.containsKey("sample") && !clinicalAnalysis.getProband().getId().equals(query.get("sample"))) {
                        logger.warn("Two samples passed");
                    } else {
                        query.put("sample", clinicalAnalysis.getProband().getId());
                    }
                }
            }

            if (clinicalAnalysis.getFiles() != null && clinicalAnalysis.getFiles().size() > 0) {
                files = clinicalAnalysis.getFiles();
            }

//            if (clinicalAnalysis.getFamily() == null || StringUtils.isEmpty(clinicalAnalysis.getFamily().getId())) {
//                throw new AnalysisException("Missing family in clinical analysis " + clinicalAnalysisId);
//            }

//            if (clinicalAnalysis.getProband() == null || StringUtils.isEmpty(clinicalAnalysis.getProband().getId())) {
//                throw new AnalysisException("Missing proband in clinical analysis " + clinicalAnalysisId);
//            }

//            org.opencb.opencga.core.models.Individual proband = clinicalAnalysis.getProband();
//            if (ListUtils.isEmpty(proband.getSamples())) {
//                throw new AnalysisException("Missing samples in proband " + proband.getId() + " in clinical analysis " + clinicalAnalysisId);
//            }

//            if (proband.getSamples().size() > 1) {
//                throw new AnalysisException("Found more than one sample for proband " + proband.getId() + " in clinical analysis "
//                        + clinicalAnalysisId);
//            }

//            // If proband sample ID is not provided, then take it from clinical analysis
//            if (probandSampleId == null) {
//                probandSampleId = clinicalAnalysis.getProband().getSamples().get(0).getId();
//            }

            // If disease is not provided, then take it from clinical analysis
            if (clinicalAnalysis.getDisorder() != null) {
//                disorder = clinicalAnalysis.getDisorder().getId();
                query.put("familyPhenotype", clinicalAnalysis.getDisorder().getId());
//                OntologyTerm disease = clinicalAnalysis.getDisorder();
//                phenotype = new Phenotype(disease.getId(), disease.getName(), disease.getSource(), Phenotype.Status.UNKNOWN);
            }
        }

        // Check Query looks fine for Interpretation
        if (!query.containsKey(VariantQueryParam.GENOTYPE.key()) && !query.containsKey(VariantQueryParam.SAMPLE.key())) {
            // TODO check query is correct
        }

        // Check disease
//        if (diseaseName == null) {
//            throw new AnalysisException("Missing disease");
//        } else {
//            phenotype = new Phenotype("", diseaseName, "", Phenotype.Status.UNKNOWN);
//        }



        // Check proband sample ID
//        String bamFileId;
//        Set<String> lowCoverageByGeneDone;
//        List<ReportedLowCoverage> reportedLowCoverages;
//        if (probandSampleId == null) {
//            throw new AnalysisException("Missing proband sample ID");
//        } else {
//            // Reported low coverage map
//            lowCoverageByGeneDone = new HashSet<>();
//            reportedLowCoverages = new ArrayList<>();
//
//            // Look for the bam file of the proband
//            QueryResult<File> fileQueryResult = catalogManager.getFileManager().get(studyStr, new Query()
//                            .append(FileDBAdaptor.QueryParams.SAMPLES.key(), probandSampleId)
//                            .append(FileDBAdaptor.QueryParams.FORMAT.key(), File.Format.BAM),
//                    new QueryOptions(QueryOptions.INCLUDE, FileDBAdaptor.QueryParams.UUID.key()), token);
//            if (fileQueryResult.getNumResults() > 1) {
//                throw new AnalysisException("More than one BAM file found for proband sample ID " + probandSampleId
//                        + " in clinical analysis " + clinicalAnalysisId);
//            }
//
//            bamFileId = fileQueryResult.getNumResults() == 1 ? fileQueryResult.first().getUuid() : null;
//        }

        // Get and check panels
        List<Panel> diseasePanels = new ArrayList<>();
        if (query.get("panel") != null) {
            List<String> diseasePanelIds = Arrays.asList(query.getString("panel").split(","));
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
//            throw new AnalysisException("Missing disease panels");
        }
//
//        // Get and check moi
//        // TODO: get and check mode of inheritance from query or disease panel?
//
//        for (Panel diseasePanel: diseasePanels) {
//            Map<String, List<String>> genePenetranceMap = new HashMap<>();
//
//            for (DiseasePanel.GenePanel genePanel : diseasePanel.getDiseasePanel().getGenes()) {
//                String key;
//                if (StringUtils.isEmpty(genePanel.getModeOfInheritance())) {
//                    key = "all";
//                } else {
//                    if (genePanel.getPenetrance() == null) {
//                        key = genePanel.getModeOfInheritance() + SEPARATOR + ClinicalProperty.Penetrance.COMPLETE;
//                    } else {
//                        key = genePanel.getModeOfInheritance() + SEPARATOR + genePanel.getPenetrance().name();
//                    }
//                }
//
//                if (!genePenetranceMap.containsKey(key)) {
//                    genePenetranceMap.put(key, new ArrayList<>());
//                }
//
//                // Add gene id to the list
//                genePenetranceMap.get(key).add(genePanel.getId());
//            }
//
//            if (bamFileId != null) {
//                for (DiseasePanel.GenePanel genePanel : diseasePanel.getDiseasePanel().getGenes()) {
//                    String geneName = genePanel.getId();
//                    if (!lowCoverageByGeneDone.contains(geneName)) {
//                        reportedLowCoverages.addAll(getReportedLowCoverages(geneName, bamFileId, maxCoverage));
//                        lowCoverageByGeneDone.add(geneName);
//                    }
//                }
//            }
//
//            ClinicalProperty.Penetrance penetrance;
//
//            for (String key : genePenetranceMap.keySet()) {
//                if (key.equals("all")) {
//                    penetrance = ClinicalProperty.Penetrance.COMPLETE;
//                } else {
//                    String[] splitString = key.split(SEPARATOR);
//
//                    // TODO: splitString[0] is a free string, it will never match a valid ClinicalProperty.ModeOfInheritance
//                    moi = ClinicalProperty.ModeOfInheritance.valueOf(splitString[0]);
//                    penetrance = ClinicalProperty.Penetrance.valueOf(splitString[1]);
//
//                    // Genes
//                    query.put(VariantQueryParam.ANNOT_XREF.key(), genePenetranceMap.get(key));
//                }
//

//            }
//        }


        QueryOptions queryOptions = new QueryOptions();
        queryOptions.add(QueryOptions.LIMIT, 10);

        // Execute query
        VariantQueryResult<Variant> variantQueryResult = variantStorageManager.get(query, queryOptions, token);

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
                    if (diseasePanels.size() > 0) {
                        for (Panel diseasePanel : diseasePanels) {

                            // Create the reported event
                            ReportedEvent reportedEvent = new ReportedEvent()
                                    .setId("JT-PF-" + reportedVariant.getReportedEvents().size())
                                    .setPhenotypes(Collections.singletonList(phenotype))
                                    .setConsequenceTypeIds(Collections.singletonList(ct.getBiotype()))
                                    .setGenomicFeature(new GenomicFeature(ct.getEnsemblGeneId(), ct.getEnsemblTranscriptId(), ct.getGeneName(),
                                            null, null))
                                    .setPanelId(diseasePanel.getDiseasePanel().getId());
//                                .setPenetrance(penetrance);

                            if (moi != null) {
                                reportedEvent.setModeOfInheritance(moi);
                            }

                            // TODO: add additional reported event fields

                            // Add reported event to the reported variant
                            reportedVariant.getReportedEvents().add(reportedEvent);
                        }
                    } else {
                        // Create the reported event
                        ReportedEvent reportedEvent = new ReportedEvent()
                                .setId("JT-PF-" + reportedVariant.getReportedEvents().size())
                                .setPhenotypes(Collections.singletonList(phenotype))
                                .setConsequenceTypeIds(Collections.singletonList(ct.getBiotype()))
                                .setGenomicFeature(new GenomicFeature(ct.getEnsemblGeneId(), ct.getEnsemblTranscriptId(), ct.getGeneName(),
                                        null, null));
//                                .setPanelId(diseasePanel.getDiseasePanel().getId());
//                                .setPenetrance(penetrance);

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





        if (options.getBoolean("INCLUDE_LOW_COVERAGE_REGION", false)) {
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

            // We need the genes from Query.gene and Query.panel

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
                .setFilters(query) //TODO
                .setSoftware(new Software().setName("Custom"))
                .setReportedVariants(new ArrayList<>(reportedVariantMap.values()));
//                .setReportedLowCoverages(reportedLowCoverages);

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

//    public String getProbandSampleId() {
//        return probandSampleId;
//    }
//
//    public CustomAnalysis setProbandSampleId(String probandSampleId) {
//        this.probandSampleId = probandSampleId;
//        return this;
//    }
//
//    public String getDisorder() {
//        return disorder;
//    }
//
//    public CustomAnalysis setDisorder(String disorder) {
//        this.disorder = disorder;
//        return this;
//    }
//
//    public List<String> getDiseasePanelIds() {
//        return diseasePanelIds;
//    }
//
//    public CustomAnalysis setDiseasePanelIds(List<String> diseasePanelIds) {
//        this.diseasePanelIds = diseasePanelIds;
//        return this;
//    }

    public int getMaxCoverage() {
        return maxCoverage;
    }

    public CustomAnalysis setMaxCoverage(int maxCoverage) {
        this.maxCoverage = maxCoverage;
        return this;
    }
}
