package org.opencb.opencga.analysis.clinical;

import org.opencb.biodata.models.clinical.interpretation.Interpretation;
import org.opencb.opencga.analysis.AnalysisResult;

public abstract class XQueryAnalysis /*extends FamilyAnalysis<Interpretation>*/ {

//    private BioNetDbManager bioNetDbManager;
//
//    private final static String XQUERY_ANALYSIS_NAME = "BioNetInterpretation";
//
//   public XQueryAnalysis(String clinicalAnalysisId, List<String> diseasePanelIds, String studyStr, Map<String, RoleInCancer> roleInCancer,
//                          Map<String, List<String>> actionableVariants, ClinicalProperty.Penetrance penetrance, ObjectMap options,
//                          BioNetDBConfiguration configuration, String opencgaHome, String token) throws BioNetDBException {
//        super(clinicalAnalysisId, diseasePanelIds, roleInCancer, actionableVariants, penetrance, options, studyStr, opencgaHome, token);
//
//        this.bioNetDbManager = new BioNetDbManager(configuration);
//    }

//    @Override
    public AnalysisResult<Interpretation> execute() throws Exception {
/*
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
        Pedigree pedigree = FamilyManager.getPedigreeFromFamily(clinicalAnalysis.getFamily(), proband.getId());
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
        List<ReportedVariant> primaryFindings = null;
        List<ReportedVariant> secondaryFindings = null;

        if (CollectionUtils.isNotEmpty(variantContainer.getComplexVariantList())) {
            TeamReportedVariantCreator creator = new TeamReportedVariantCreator(biodataDiseasePanels, roleInCancer, actionableVariants,
                    clinicalAnalysis.getDisorder(), null, null);
            primaryFindings = creator.create(variantContainer.getComplexVariantList());
        }
        if (CollectionUtils.isNotEmpty(variantContainer.getReactionVariantList())) {
            TeamReportedVariantCreator creator = new TeamReportedVariantCreator(biodataDiseasePanels, roleInCancer, actionableVariants,
                    clinicalAnalysis.getDisorder(), null, null);
            primaryFindings.addAll(creator.create(variantContainer.getReactionVariantList()));
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
                .setSoftware(new Software().setName(XQUERY_ANALYSIS_NAME))
                .setPrimaryFindings(primaryFindings)
                .setSecondaryFindings(secondaryFindings);

        // Compute number of results (primary and secondary findings)
        int numResults = 0;
        if (ListUtils.isNotEmpty(primaryFindings)) {
            numResults += primaryFindings.size();
        }
        if (ListUtils.isNotEmpty(secondaryFindings)) {
            numResults += secondaryFindings.size();
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
                */

        throw new UnsupportedOperationException("XQuery not yet supported");
    }

/*
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
    */
}
