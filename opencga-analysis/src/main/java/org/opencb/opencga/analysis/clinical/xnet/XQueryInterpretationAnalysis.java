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

package org.opencb.opencga.analysis.clinical.xnet;

import org.opencb.biodata.models.clinical.interpretation.InterpretationMethod;
import org.opencb.opencga.analysis.clinical.InterpretationAnalysis;
import org.opencb.opencga.core.exceptions.ToolException;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.tools.annotations.Tool;

import java.util.List;

@Tool(id = XQueryInterpretationAnalysis.ID, resource = Enums.Resource.CLINICAL)
public class XQueryInterpretationAnalysis extends InterpretationAnalysis {

    public final static String ID = "bionet-interpretation";

    private List<String> diseasePanelIds;
//    private BioNetDbManager bioNetDbManager;
//    private BionetInterpretationConfiguration config;

//    private List<DiseasePanel> diseasePanels;
//    private ClinicalAnalysis clinicalAnalysis;

    @Override
    protected InterpretationMethod getInterpretationMethod() {
        return getInterpretationMethod(ID);
    }

    @Override
    protected void run() throws ToolException {
        throw new ToolException("Not yet implemented!");
/*
        StopWatch watcher = StopWatch.createStarted();
        // Sanity check

        QueryResult<ClinicalAnalysis> clinicalAnalysisQueryResult = catalogManager.getClinicalAnalysisManager().get(studyId,
                clinicalAnalysisId, QueryOptions.empty(), token);
        if (clinicalAnalysisQueryResult.getNumResults() == 0) {
            throw new AnalysisException("Clinical analysis " + clinicalAnalysisId + " not found in study " + studyId);
        }

        ClinicalAnalysis clinicalAnalysis = clinicalAnalysisQueryResult.first();

        if (clinicalAnalysis.getFamily() == null || StringUtils.isEmpty(clinicalAnalysis.getFamily().getId())) {
            throw new AnalysisException("Missing family in clinical analysis " + clinicalAnalysisId);
        }

        if (clinicalAnalysis.getProband() == null || StringUtils.isEmpty(clinicalAnalysis.getProband().getId())) {
            throw new AnalysisException("Missing proband in clinical analysis " + clinicalAnalysisId);
        }

        org.opencb.opencga.core.models.individual.Individual proband = clinicalAnalysis.getProband();
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
                    .get(studyId, diseasePanelIds, new Query(), QueryOptions.empty(), token);

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
        List<ClinicalVariant> primaryFindings = null;
        List<ClinicalVariant> secondaryFindings = null;

        if (CollectionUtils.isNotEmpty(variantContainer.getComplexVariantList())) {
            TeamClinicalVariantCreator creator = new TeamClinicalVariantCreator(biodataDiseasePanels, roleInCancer, actionableVariants,
                    clinicalAnalysis.getDisorder(), null, null);
            primaryFindings = creator.create(variantContainer.getComplexVariantList());
        }
        if (CollectionUtils.isNotEmpty(variantContainer.getReactionVariantList())) {
            TeamClinicalVariantCreator creator = new TeamClinicalVariantCreator(biodataDiseasePanels, roleInCancer, actionableVariants,
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
                .setClinicalAnalysis(clinicalAnalysisId)
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
*/
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
