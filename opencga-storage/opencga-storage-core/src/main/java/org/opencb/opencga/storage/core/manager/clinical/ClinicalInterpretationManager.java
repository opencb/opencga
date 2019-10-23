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

import org.apache.commons.lang.StringUtils;
import org.opencb.biodata.models.clinical.interpretation.Comment;
import org.opencb.biodata.models.clinical.interpretation.ReportedVariant;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.FacetField;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.StudyDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.core.models.ClinicalAnalysis;
import org.opencb.opencga.core.models.Group;
import org.opencb.opencga.core.models.Interpretation;
import org.opencb.opencga.core.models.Study;
import org.opencb.opencga.storage.core.StorageEngineFactory;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantEngine;
import org.opencb.opencga.storage.core.clinical.ClinicalVariantException;
import org.opencb.opencga.storage.core.clinical.ReportedVariantIterator;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.manager.StorageManager;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class ClinicalInterpretationManager extends StorageManager {

    private String database;

    private ClinicalAnalysisManager clinicalAnalysisManager;
    private ClinicalVariantEngine clinicalVariantEngine;


    public ClinicalInterpretationManager(CatalogManager catalogManager, StorageEngineFactory storageEngineFactory) {
        super(catalogManager, storageEngineFactory);

        clinicalAnalysisManager = catalogManager.getClinicalAnalysisManager();

        this.init();
    }

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
    /*                    P R I V A T E     M E T H O D S                       */
    /*--------------------------------------------------------------------------*/

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
}
