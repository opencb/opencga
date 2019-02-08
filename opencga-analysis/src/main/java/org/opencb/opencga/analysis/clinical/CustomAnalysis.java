package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.StopWatch;
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
import org.opencb.biodata.tools.clinical.DefaultReportedVariantCreator;
import org.opencb.biodata.tools.clinical.ReportedVariantCreator;
import org.opencb.biodata.tools.clinical.TeamReportedVariantCreator;
import org.opencb.cellbase.client.rest.CellBaseClient;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.utils.ListUtils;
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
import org.opencb.opencga.storage.core.manager.variant.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.*;
import java.util.stream.Collectors;

public class CustomAnalysis extends FamilyAnalysis {

    private String clinicalAnalysisId;
    private Query query;
    private Set<String> findings;
    private ObjectMap options;

    private CellBaseClient cellBaseClient;
    private AlignmentStorageManager alignmentStorageManager;

    private final static int LOW_COVERAGE_DEFAULT = 20;
    private final static String CUSTOM_ANALYSIS_NAME = "Custom";

    public CustomAnalysis(String clinicalAnalysisId, Query query, Set<String> findings, String studyStr, String opencgaHome, ObjectMap options, String token) {
        this(query, findings, opencgaHome, studyStr, options, token);

        this.clinicalAnalysisId = clinicalAnalysisId;
    }

    public CustomAnalysis(Query query, Set<String> findings, String studyStr, String opencgaHome, ObjectMap options, String token) {
        super(opencgaHome, studyStr, token);

        this.query = query;
        this.findings = findings;
        this.options = (options != null) ? options : new ObjectMap();

        this.cellBaseClient = new CellBaseClient(storageConfiguration.getCellbase().toClientConfiguration());
        this.alignmentStorageManager = new AlignmentStorageManager(catalogManager, StorageEngineFactory.get(storageConfiguration));
    }

    @Override
    public InterpretationResult execute() throws Exception {
        StopWatch watcher = StopWatch.createStarted();

        String probandSampleId = null;
        Phenotype phenotype = null;
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

            if (clinicalAnalysis.getDisorder() != null) {
                query.put("familyPhenotype", clinicalAnalysis.getDisorder().getId());
            }
        }

        // Check Query looks fine for Interpretation
        if (!query.containsKey(VariantQueryParam.GENOTYPE.key()) && !query.containsKey(VariantQueryParam.SAMPLE.key())) {
            // TODO check query is correct
        }

        // Get and check panels
        List<Panel> diseasePanels = new ArrayList<>();
        if (query.get(VariantCatalogQueryUtils.PANEL.key()) != null) {
            List<String> diseasePanelIds = Arrays.asList(query.getString(VariantCatalogQueryUtils.PANEL.key()).split(","));
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
        }

        QueryOptions queryOptions = new QueryOptions(options);
//        queryOptions.add(QueryOptions.LIMIT, 20);

        // Execute query
        VariantQueryResult<Variant> variantQueryResult = variantStorageManager.get(query, queryOptions, token);

        // Create reported variants and events
        List<ReportedVariant> reportedVariants = null;
        List<DiseasePanel> biodataDiseasePanels = null;
        // Sanity check
        if(CollectionUtils.isNotEmpty(variantQueryResult.getResult())) {
            ReportedVariantCreator creator;
            if (CollectionUtils.isNotEmpty(diseasePanels)) {
                // Team reported variant creator
                biodataDiseasePanels = diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());
                creator = new TeamReportedVariantCreator(biodataDiseasePanels, findings, phenotype, moi, null);
            } else {
                // Default reported variant creator
                creator = new DefaultReportedVariantCreator(phenotype, moi, null);
            }
            creator.setIncludeNoTier(config.getBoolean("includeNoTier", true));
            reportedVariants = creator.create(variantQueryResult.getResult());
        }


        // Low coverage support
        List<ReportedLowCoverage> reportedLowCoverages = new ArrayList<>();
        if (options.getBoolean("includeLowCoverage", false)) {
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
                Set<String> genes = new HashSet<>();
                if (query.get(VariantQueryParam.GENE.key()) != null) {
                    genes.addAll(Arrays.asList(query.getString(VariantQueryParam.GENE.key()).split(",")));
                }
                for (Panel diseasePanel : diseasePanels) {
                    for (DiseasePanel.GenePanel genePanel : diseasePanel.getDiseasePanel().getGenes()) {
                        genes.add(genePanel.getId());
                    }
                }

                // Compute low coverage for genes found
                int maxCoverage = options.getInt("maxLowCoverage", LOW_COVERAGE_DEFAULT);
                Iterator<String> iterator = genes.iterator();
                while (iterator.hasNext()) {
                    String geneName = iterator.next();
                    List<ReportedLowCoverage> lowCoverages = getReportedLowCoverages(geneName, bamFileId, maxCoverage);
                    if (ListUtils.isNotEmpty(lowCoverages)) {
                        reportedLowCoverages.addAll(lowCoverages);
                    }
                }
            }
        }

        String userId = catalogManager.getUserManager().getUserId(token);
        QueryResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);

        // Create Interpretation
        Interpretation interpretation = new Interpretation()
                .setId(CUSTOM_ANALYSIS_NAME + SEPARATOR + TimeUtils.getTimeMillis())
                .setReportedVariants(reportedVariants)
                .setReportedLowCoverages(reportedLowCoverages)
                .setAnalyst(new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization()))
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(biodataDiseasePanels)
                .setFilters(query)
                .setSoftware(new Software().setName(CUSTOM_ANALYSIS_NAME));

        // Return interpretation result
        return new InterpretationResult(
                interpretation,
                Math.toIntExact(watcher.getTime()),
                new HashMap<>(),
                variantQueryResult.getDbTime(),
                variantQueryResult.getNumResults(),
                variantQueryResult.getNumTotalResults(),
                variantQueryResult.getWarningMsg(),
                variantQueryResult.getErrorMsg());
    }

    /**
     * Compute low coverages from a BAM file for a given gene.
     *
     * @param geneName Gene name
     * @param bamFileId BAM file
     * @param maxCoverage Max. coverage to be reported (default 20)
     * @return  List of reported low coverage
     */
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

    private ReportedEvent createReportedEvent(String id, Phenotype phenotype, Variant variant, ConsequenceType ct,
                                              String panelId, ClinicalProperty.ModeOfInheritance moi) {
        // Create the reported event
        ReportedEvent reportedEvent = new ReportedEvent()
                .setId(id)
                .setPhenotypes(Collections.singletonList(phenotype))
                .setConsequenceTypeIds(Collections.singletonList(ct.getBiotype()))
                .setGenomicFeature(new GenomicFeature(ct.getEnsemblGeneId(), ct.getEnsemblTranscriptId(), ct.getGeneName(),
                        null, null));

        if (panelId != null) {
            reportedEvent.setPanelId(panelId);
        }

        if (moi != null) {
            reportedEvent.setModeOfInheritance(moi);
        }

        // TODO: add additional reported event fields

        VariantClassification variantClassification = new VariantClassification();
        variantClassification.setAcmg(VariantClassification.calculateAcmgClassification(variant, reportedEvent));
        reportedEvent.setClassification(variantClassification);

        return reportedEvent;
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
}
