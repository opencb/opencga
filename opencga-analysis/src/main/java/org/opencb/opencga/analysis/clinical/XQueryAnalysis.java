package org.opencb.opencga.analysis.clinical;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.biodata.models.clinical.interpretation.ClinicalProperty.RoleInCancer;
import org.opencb.biodata.models.clinical.interpretation.DiseasePanel;
import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.biodata.models.clinical.pedigree.Pedigree;
import org.opencb.biodata.models.commons.Analyst;
import org.opencb.biodata.models.commons.OntologyTerm;
import org.opencb.biodata.models.commons.Phenotype;
import org.opencb.biodata.models.commons.Software;
import org.opencb.biodata.tools.clinical.TeamReportedVariantCreator;
import org.opencb.bionetdb.core.BioNetDbManager;
import org.opencb.bionetdb.core.config.BioNetDBConfiguration;
import org.opencb.bionetdb.core.exceptions.BioNetDBException;
import org.opencb.bionetdb.core.neo4j.interpretation.FamilyFilter;
import org.opencb.bionetdb.core.neo4j.interpretation.GeneFilter;
import org.opencb.bionetdb.core.neo4j.interpretation.VariantContainer;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.utils.ListUtils;
import org.opencb.opencga.analysis.AnalysisResult;
import org.opencb.opencga.analysis.exceptions.AnalysisException;
import org.opencb.opencga.catalog.db.api.UserDBAdaptor;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Panel;
import org.opencb.opencga.core.models.User;

import java.util.*;
import java.util.stream.Collectors;

public class XQueryAnalysis extends FamilyAnalysis {

    private BioNetDbManager bioNetDbManager;

    private final static String XQUERY_ANALYSIS_NAME = "BioNetInterpretation";

    public XQueryAnalysis(String clinicalAnalysisId, List<String> diseasePanelIds, String studyStr, Map<String, RoleInCancer> roleInCancer,
                          Map<String, List<String>> actionableVariants, ObjectMap options, BioNetDBConfiguration configuration,
                          String opencgaHome, String token) throws BioNetDBException {
        super(clinicalAnalysisId, diseasePanelIds, roleInCancer, actionableVariants, options, studyStr, opencgaHome, token);

        this.bioNetDbManager = new BioNetDbManager(configuration);
    }

    @Override
    public AnalysisResult<Interpretation> execute() throws Exception {
        StopWatch watcher = StopWatch.createStarted();

        // Sanity check

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

        List<Panel> diseasePanels = new ArrayList<>();
        if (diseasePanelIds != null && !diseasePanelIds.isEmpty()) {
            List<QueryResult<Panel>> queryResults = catalogManager.getPanelManager()
                    .get(studyStr, diseasePanelIds, new Query(), QueryOptions.empty(), token);

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

        // Check sample and proband exists
        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily());
        OntologyTerm disorder = clinicalAnalysis.getDisorder();
        Phenotype phenotype = new Phenotype(disorder.getId(), disorder.getName(), disorder.getSource(),
                Phenotype.Status.UNKNOWN);

        FamilyFilter familyFilter = new FamilyFilter(pedigree, phenotype);
        List<DiseasePanel> biodataDiseasePanels = diseasePanels.stream().map(Panel::getDiseasePanel).collect(Collectors.toList());
        GeneFilter geneFilter = new GeneFilter();
        geneFilter.setPanels(biodataDiseasePanels);

        // Execute query
        StopWatch dbWatcher = StopWatch.createStarted();
        VariantContainer variantContainer = this.bioNetDbManager.xQuery(familyFilter, geneFilter);
        long dbTime = dbWatcher.getTime();

        // Create reported variants and events
        List<ReportedVariant> reportedVariants = null;
        int numResults = 0;
        if (CollectionUtils.isNotEmpty(variantContainer.getComplexVariantList())) {
            TeamReportedVariantCreator creator = new TeamReportedVariantCreator(biodataDiseasePanels, roleInCancer, actionableVariants,
                    clinicalAnalysis.getDisorder(), null, null);
            reportedVariants = creator.create(variantContainer.getComplexVariantList());
        }
        if (CollectionUtils.isNotEmpty(variantContainer.getReactionVariantList())) {
            TeamReportedVariantCreator creator = new TeamReportedVariantCreator(biodataDiseasePanels, roleInCancer, actionableVariants,
                    clinicalAnalysis.getDisorder(), null, null);
            reportedVariants.addAll(creator.create(variantContainer.getReactionVariantList()));
        }

        // Create user information
        String userId = catalogManager.getUserManager().getUserId(token);
        QueryResult<User> userQueryResult = catalogManager.getUserManager().get(userId, new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(UserDBAdaptor.QueryParams.EMAIL.key(), UserDBAdaptor.QueryParams.ORGANIZATION.key())), token);

        // Create Interpretation
        Interpretation interpretation = new Interpretation()
                .setId(XQUERY_ANALYSIS_NAME + SEPARATOR + TimeUtils.getTimeMillis())
                .setAnalyst(new Analyst(userId, userQueryResult.first().getEmail(), userQueryResult.first().getOrganization()))
                .setClinicalAnalysisId(clinicalAnalysisId)
                .setCreationDate(TimeUtils.getTime())
                .setPanels(biodataDiseasePanels)
                .setFilters(getFilters(familyFilter, geneFilter))
                .setSoftware(new Software().setName(XQUERY_ANALYSIS_NAME));

        // Set reported variants
        if (ListUtils.isNotEmpty(reportedVariants)) {
            interpretation.setReportedVariants(reportedVariants);
            numResults = reportedVariants.size();
        }

        // Set low coverage
//        if (ListUtils.isNotEmpty(reportedLowCoverages)) {
//            interpretation.setReportedLowCoverages(reportedLowCoverages);
//        }

        // Return interpretation result
        return new InterpretationResult(
                interpretation,
                Math.toIntExact(watcher.getTime()),
                new HashMap<>(),
                (int) dbTime,
                numResults,
                numResults,
                "", // warning message
                ""); // error message
    }

    private Map<String, Object> getFilters(FamilyFilter familyFilter, GeneFilter geneFilter) {
        ObjectMap filters = new ObjectMap();
        if (familyFilter != null) {
            filters.put("familyFilter", familyFilter);
        }
        if (geneFilter != null) {
            filters.put("geneFilter", geneFilter);
        }
        return filters;
    }
}
