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

package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.clinical.ClinicalAnalyst;
import org.opencb.biodata.models.clinical.ClinicalAudit;
import org.opencb.biodata.models.clinical.ClinicalComment;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.db.api.InterpretationDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.ClinicalAnalysisConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.ClinicalAnalysisCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.JacksonUtils;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.clinical.*;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.FlagAnnotation;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.file.File;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.panel.Panel;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.StudyPermissions;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.*;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.filterAnnotationSets;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by pfurio on 05/06/17.
 */
public class ClinicalAnalysisMongoDBAdaptor extends AnnotationMongoDBAdaptor<ClinicalAnalysis> implements ClinicalAnalysisDBAdaptor {

    private static final String PRIVATE_DUE_DATE = "_dueDate";
    private final MongoDBCollection clinicalCollection;
    private final MongoDBCollection archiveClinicalCollection;
    private final MongoDBCollection deletedClinicalCollection;
    private final ClinicalAnalysisConverter clinicalConverter;

    private final SnapshotVersionedMongoDBAdaptor versionedMongoDBAdaptor;

    public ClinicalAnalysisMongoDBAdaptor(MongoDBCollection clinicalCollection, MongoDBCollection archiveClinicalCollection,
                                          MongoDBCollection deletedClinicalCollection, Configuration configuration,
                                          OrganizationMongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(ClinicalAnalysisMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.clinicalCollection = clinicalCollection;
        this.archiveClinicalCollection = archiveClinicalCollection;
        this.deletedClinicalCollection = deletedClinicalCollection;
        this.clinicalConverter = new ClinicalAnalysisConverter();
        this.versionedMongoDBAdaptor = new SnapshotVersionedMongoDBAdaptor(clinicalCollection, archiveClinicalCollection,
                deletedClinicalCollection);
    }

    @Override
    protected MongoDBCollection getCollection() {
        return clinicalCollection;
    }

    static void fixCommentsForRemoval(ObjectMap parameters) {
        if (parameters.get(COMMENTS.key()) == null) {
            return;
        }

        List<Document> commentParamList = new LinkedList<>();
        for (Object comment : parameters.getAsList(COMMENTS.key())) {
            if (comment instanceof ClinicalComment) {
                commentParamList.add(new Document(COMMENTS_DATE.key().replace(COMMENTS.key() + ".", ""),
                        ((ClinicalComment) comment).getDate()));
            }
        }
        parameters.put(COMMENTS.key(), commentParamList);
    }

    static void fixFlagsForRemoval(ObjectMap parameters) {
        if (parameters.get(FLAGS.key()) == null) {
            return;
        }

        List<FlagValueParam> flagParamList = new LinkedList<>();
        for (Object comment : parameters.getAsList(FLAGS.key())) {
            if (comment instanceof FlagAnnotation) {
                flagParamList.add(FlagValueParam.of((FlagAnnotation) comment));
            }
        }
        parameters.put(FLAGS.key(), flagParamList);
    }

    static void fixPanelsForRemoval(ObjectMap parameters) {
        if (parameters.get(PANELS.key()) == null) {
            return;
        }

        List<Document> panelParamList = new LinkedList<>();
        for (Object panel : parameters.getAsList(PANELS.key())) {
            if (panel instanceof Panel) {
                panelParamList.add(new Document("id", ((Panel) panel).getId()));
            }
        }
        parameters.put(PANELS.key(), panelParamList);
    }

    static void fixFilesForRemoval(ObjectMap parameters, String key) {
        if (parameters.get(key) == null) {
            return;
        }

        List<Document> fileParamList = new LinkedList<>();
        for (Object file : parameters.getAsList(key)) {
            if (file instanceof File) {
                fileParamList.add(new Document("uid", ((File) file).getUid()));
            } else if (file instanceof Document) {
                fileParamList.add(new Document("uid", ((Document) file).get("uid")));
            } else {
                throw new IllegalArgumentException("Expected a File or Document object");
            }
        }
        parameters.put(key, fileParamList);
    }

    static void fixAnalystsForRemoval(ObjectMap parameters) {
        if (parameters.get(ANALYSTS.key()) == null) {
            return;
        }

        List<Document> analystParamList = new LinkedList<>();
        for (Object analyst : parameters.getAsList(ANALYSTS.key())) {
            if (analyst instanceof ClinicalAnalyst) {
                analystParamList.add(new Document("id", ((ClinicalAnalyst) analyst).getId()));
            }
        }
        parameters.put(ANALYSTS.key(), analystParamList);
    }

    @Override
    public OpenCGAResult<AnnotationSet> getAnnotationSet(long id, @Nullable String annotationSetName)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = new QueryOptions();
        List<String> includeList = new ArrayList<>();

        if (StringUtils.isNotEmpty(annotationSetName)) {
            includeList.add(Constants.ANNOTATION_SET_NAME + "." + annotationSetName);
        } else {
            includeList.add(QueryParams.ANNOTATION_SETS.key());
        }
        queryOptions.put(QueryOptions.INCLUDE, includeList);

        OpenCGAResult<ClinicalAnalysis> clinicalDataResult = get(id, queryOptions);
        if (CollectionUtils.isEmpty(clinicalDataResult.first().getAnnotationSets())) {
            return new OpenCGAResult<>(clinicalDataResult.getTime(), clinicalDataResult.getEvents(), 0, Collections.emptyList(), 0);
        } else {
            List<AnnotationSet> annotationSets = clinicalDataResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new OpenCGAResult<>(clinicalDataResult.getTime(), clinicalDataResult.getEvents(), size, annotationSets, size);
        }
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(clinicalCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult<Long> count(final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query, user);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, final Query query, final String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        logger.debug("Clinical count: query : {}", bson.toBsonDocument());
        return new OpenCGAResult<>(clinicalCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Use other update method passing ClinicalAudit parameter");
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Use other update method passing ClinicalAudit parameter");
    }

    @Override
    public OpenCGAResult update(long uid, ObjectMap parameters, List<VariableSet> variableSetList, List<ClinicalAudit> clinicalAuditList,
                                QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query(QueryParams.UID.key(), uid);
        QueryOptions options = new QueryOptions()
                .append(QueryOptions.INCLUDE, Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.STUDY_UID.key()))
                .append(NATIVE_QUERY, true);
        OpenCGAResult<ClinicalAnalysis> result = get(query, options);
        if (result.getNumResults() == 0) {
            throw new CatalogDBException("Could not update clinical analysis. Clinical Analysis uid '" + uid + "' not found.");
        }
        String clinicalAnalysisId = result.first().getId();

        try {
            return runTransaction(clientSession -> transactionalUpdate(clientSession, result.first(), parameters, variableSetList,
                    clinicalAuditList, queryOptions));
        } catch (CatalogException e) {
            logger.error("Could not update clinical analysis {}: {}", clinicalAnalysisId, e.getMessage(), e);
            throw new CatalogDBException("Could not update clinical analysis " + clinicalAnalysisId + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, List<ClinicalAudit> clinicalAuditList,
                                QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not possible updating Clinical Analyses based on a query");
    }

    @Override
    public OpenCGAResult update(long uid, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Use other update method passing the ClinicalAuditList");
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Use other update method passing the ClinicalAuditList");
    }

    OpenCGAResult transactionalUpdate(ClientSession clientSession, ClinicalAnalysis clinical, ObjectMap parameters,
                                      List<VariableSet> variableSetList, List<ClinicalAudit> clinicalAuditList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        String clinicalAnalysisId = clinical.getId();
        long clinicalAnalysisUid = clinical.getUid();

        Query tmpQuery = new Query()
                .append(STUDY_UID.key(), clinical.getStudyUid())
                .append(QueryParams.UID.key(), clinicalAnalysisUid);
        Bson bsonQuery = parseQuery(tmpQuery);
        return versionedMongoDBAdaptor.update(clientSession, bsonQuery, (entryList) -> {
            DataResult<?> result = updateAnnotationSets(clientSession, clinical.getStudyUid(), clinicalAnalysisUid, parameters,
                    variableSetList, queryOptions, false);

            // Perform the update
            UpdateDocument updateDocument = parseAndValidateUpdateParams(parameters, clinicalAuditList, tmpQuery, queryOptions);

            Document updateOperation = updateDocument.toFinalUpdateDocument();
            List<Event> events = new ArrayList<>();
            if (!updateOperation.isEmpty() || !updateDocument.getNestedUpdateList().isEmpty()) {
                DataResult<?> update;

                if (!updateOperation.isEmpty()) {
                    logger.debug("Update clinical analysis. Query: {}, Update: {}", bsonQuery.toBsonDocument(), updateDocument);
                    update = clinicalCollection.update(clientSession, bsonQuery, updateOperation, null);

                    if (update.getNumMatches() == 0) {
                        throw CatalogDBException.uidNotFound("Clinical Analysis", clinicalAnalysisUid);
                    }
                    if (update.getNumUpdated() == 0) {
                        events.add(new Event(Event.Type.WARNING, clinicalAnalysisId, "Clinical Analysis was already updated"));
                    }

                    if (parameters.getBoolean(LOCKED.key())) {
                        // Propagate locked value to Interpretations
                        logger.debug("Propagating case lock to all the Interpretations");
                        dbAdaptorFactory.getInterpretationDBAdaptor().propagateLockedFromClinicalAnalysis(clientSession, clinical,
                                parameters.getBoolean(LOCKED.key()));
                    }

                    logger.debug("Clinical Analysis {} successfully updated", clinicalAnalysisId);
                }

                if (!updateDocument.getNestedUpdateList().isEmpty()) {
                    // Nested documents are used to update reports
                    for (NestedArrayUpdateDocument nestedDocument : updateDocument.getNestedUpdateList()) {
                        Bson tmpBsonQuery = parseQuery(nestedDocument.getQuery().append(QueryParams.UID.key(), clinicalAnalysisUid));
                        logger.debug("Update nested element from Clinical Analysis. Query: {}, Update: {}",
                                tmpBsonQuery.toBsonDocument(), nestedDocument.getSet());
                        update = clinicalCollection.update(clientSession, tmpBsonQuery, nestedDocument.getSet(), null);

                        if (update.getNumMatches() == 0) {
                            throw CatalogDBException.uidNotFound("Clinical Analysis", clinicalAnalysisUid);
                        }
                    }
                }

            } else if (result.getNumUpdated() == 0) {
                throw new CatalogDBException("Nothing to update");
            }

            return endWrite(tmpStartTime, 1, 1, events);
        }, null, null);
    }

    @Override
    OpenCGAResult<ClinicalAnalysis> transactionalUpdate(ClientSession clientSession, ClinicalAnalysis entry, ObjectMap parameters,
                                                        List<VariableSet> variableSetList, QueryOptions queryOptions,
                                                        boolean incrementVersion)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        throw new NotImplementedException("Please call to the other transactionalUpdate method passing the ClinicalAudit list");
    }

    @Override
    OpenCGAResult<ClinicalAnalysis> transactionalUpdate(ClientSession clientSession, long studyUid, Bson query,
                                                        UpdateDocument updateDocument, boolean incrementVersion)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        Document updateOperation = updateDocument.toFinalUpdateDocument();
        if (!updateOperation.isEmpty()) {
            SnapshotVersionedMongoDBAdaptor.FunctionWithException<ClinicalAnalysis> updateClinicalReferences = (clinicalList) -> {
                logger.debug("Update clinical analysis. Query: {}, Update: {}", query.toBsonDocument(), updateDocument);
                DataResult<?> update = clinicalCollection.update(clientSession, query, updateOperation, null);

                if (updateDocument.getSet().getBoolean(LOCKED.key(), false)) {
                    // Propagate locked value to Interpretations
                    logger.debug("Propagating case lock to all the Interpretations");
                    MongoDBIterator<ClinicalAnalysis> iterator = clinicalCollection.iterator(clientSession, query, null, clinicalConverter,
                            ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS);
                    while (iterator.hasNext()) {
                        ClinicalAnalysis clinical = iterator.next();
                        dbAdaptorFactory.getInterpretationDBAdaptor().propagateLockedFromClinicalAnalysis(clientSession, clinical, true);
                    }
                }

                logger.debug("{} clinical analyses successfully updated", update.getNumUpdated());
                return endWrite(tmpStartTime, update.getNumMatches(), update.getNumUpdated(), Collections.emptyList());
            };
            if (incrementVersion) {
                return versionedMongoDBAdaptor.update(clientSession, query, null, updateClinicalReferences, null, null);
            } else {
                return versionedMongoDBAdaptor.updateWithoutVersionIncrement(clientSession, query, null, updateClinicalReferences);
            }
        } else {
            throw new CatalogDBException("Nothing to update");
        }
    }

    UpdateDocument parseAndValidateUpdateParams(ObjectMap parameters, List<ClinicalAudit> clinicalAuditList, Query query,
                                                QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one clinical analysis...
            Query tmpQuery = new Query(query);

            OpenCGAResult<ClinicalAnalysis> clinicalAnalysisDataResult = get(tmpQuery, new QueryOptions());
            if (clinicalAnalysisDataResult.getNumResults() == 0) {
                throw new CatalogDBException("Update clinical analysis: No clinical analysis found to be updated");
            }
            if (clinicalAnalysisDataResult.getNumResults() > 1) {
                throw new CatalogDBException("Update clinical analysis: Cannot set the same id parameter for different clinical analyses");
            }

            // Check that the new clinical analysis id will be unique
            long studyId = getStudyId(clinicalAnalysisDataResult.first().getUid());

            tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            OpenCGAResult<Long> count = count(tmpQuery);
            if (count.getNumMatches() > 0) {
                throw new CatalogDBException("Cannot set id for clinical analysis. A clinical analysis with { id: '"
                        + parameters.get(QueryParams.ID.key()) + "'} already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        String[] acceptedBooleanParams = {LOCKED.key(), PANEL_LOCKED.key()};
        filterBooleanParams(parameters, document.getSet(), acceptedBooleanParams);

        String[] acceptedParams = {QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, document.getSet(), acceptedParams);

        if (StringUtils.isNotEmpty(parameters.getString(QueryParams.CREATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.CREATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.CREATION_DATE.key(), time);
            document.getSet().put(PRIVATE_CREATION_DATE, date);
        }
        if (StringUtils.isNotEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
            String time = parameters.getString(QueryParams.MODIFICATION_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }
        if (StringUtils.isNotEmpty(parameters.getString(DUE_DATE.key()))) {
            String time = parameters.getString(QueryParams.DUE_DATE.key());
            Date date = TimeUtils.toDate(time);
            document.getSet().put(DUE_DATE.key(), time);
            document.getSet().put(PRIVATE_DUE_DATE, date);
        }

        String[] acceptedObjectParams = {QueryParams.FAMILY.key(), QueryParams.DISORDER.key(), QUALITY_CONTROL.key(),
                QueryParams.PROBAND.key(), QueryParams.ALERTS.key(), QueryParams.INTERNAL_STATUS.key(), QueryParams.PRIORITY.key(),
                QueryParams.CONSENT.key(), QueryParams.STATUS.key(), QueryParams.INTERPRETATION.key(), REPORT.key(),
                REQUEST.key(), RESPONSIBLE.key(), ATTRIBUTES.key(), };
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);

        if (parameters.containsKey(INTERPRETATION.key()) && parameters.get(INTERPRETATION.key()) == null) {
            // User wants to remove current interpretation
            document.getSet().put(INTERPRETATION.key(), null);
        }

        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());

        if (parameters.containsKey(REPORT_UPDATE.key())) {
            ObjectMap reportParameters = parameters.getNestedMap(REPORT_UPDATE.key());
            reportParameters.put(ReportQueryParams.DATE.key(), TimeUtils.getTime());
            String[] stringParams = {ReportQueryParams.TITLE.key(), ReportQueryParams.OVERVIEW.key(), ReportQueryParams.LOGO.key(),
                    ReportQueryParams.SIGNED_BY.key(), ReportQueryParams.SIGNATURE.key(), ReportQueryParams.DATE.key(), };
            filterStringParams(reportParameters, document.getSet(), stringParams, REPORT.key() + ".");

            String[] objectParams = {ReportQueryParams.DISCUSSION.key()};
            filterObjectParams(reportParameters, document.getSet(), objectParams, REPORT.key() + ".");

            String[] commentParams = {ReportQueryParams.COMMENTS.key()};
            ParamUtils.AddRemoveReplaceAction basicOperation = ParamUtils.AddRemoveReplaceAction
                    .from(actionMap, ReportQueryParams.COMMENTS.key(), ParamUtils.AddRemoveReplaceAction.ADD);
            switch (basicOperation) {
                case REMOVE:
                    fixCommentsForRemoval(reportParameters);
                    filterObjectParams(reportParameters, document.getPull(), commentParams, REPORT.key() + ".");
                    break;
                case ADD:
                    filterObjectParams(reportParameters, document.getAddToSet(), commentParams, REPORT.key() + ".");
                    break;
                case REPLACE:
                    filterReplaceParams(reportParameters.getAsList(ReportQueryParams.COMMENTS.key(), ClinicalComment.class), document,
                            ClinicalComment::getDate, QueryParams.REPORT.key() + "." + QueryParams.COMMENTS_DATE.key());
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + basicOperation);
            }

            String[] filesParams = new String[]{ReportQueryParams.FILES.key()};
            ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, ReportQueryParams.FILES.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            switch (operation) {
                case SET:
                    filterObjectParams(reportParameters, document.getSet(), filesParams, QueryParams.REPORT.key() + ".");
                    clinicalConverter.validateFilesToUpdate(document.getSet(),  QueryParams.REPORT.key() + "."
                            + ReportQueryParams.FILES.key());
                    break;
                case REMOVE:
                    fixFilesForRemoval(reportParameters, ReportQueryParams.FILES.key());
                    filterObjectParams(reportParameters, document.getPull(), filesParams, QueryParams.REPORT.key() + ".");
                    break;
                case ADD:
                    filterObjectParams(reportParameters, document.getAddToSet(), filesParams, QueryParams.REPORT.key() + ".");
                    clinicalConverter.validateFilesToUpdate(document.getAddToSet(), QueryParams.REPORT.key() + "."
                            + ReportQueryParams.FILES.key());
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + basicOperation);
            }

            String[] supportingEvidencesParams = new String[]{ReportQueryParams.SUPPORTING_EVIDENCES.key()};
            operation = ParamUtils.BasicUpdateAction.from(actionMap, ReportQueryParams.SUPPORTING_EVIDENCES.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            switch (operation) {
                case SET:
                    filterObjectParams(reportParameters, document.getSet(), supportingEvidencesParams, QueryParams.REPORT.key() + ".");
                    clinicalConverter.validateFilesToUpdate(document.getSet(), QueryParams.REPORT.key() + "."
                            + ReportQueryParams.SUPPORTING_EVIDENCES.key());
                    break;
                case REMOVE:
                    fixFilesForRemoval(reportParameters, ReportQueryParams.SUPPORTING_EVIDENCES.key());
                    filterObjectParams(reportParameters, document.getPull(), supportingEvidencesParams, QueryParams.REPORT.key() + ".");
                    break;
                case ADD:
                    filterObjectParams(reportParameters, document.getAddToSet(), supportingEvidencesParams, QueryParams.REPORT.key() + ".");
                    clinicalConverter.validateFilesToUpdate(document.getAddToSet(), QueryParams.REPORT.key() + "."
                            + ReportQueryParams.SUPPORTING_EVIDENCES.key());
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + basicOperation);
            }
        }

        clinicalConverter.validateInterpretationToUpdate(document.getSet());
        clinicalConverter.validateFamilyToUpdate(document.getSet());
        clinicalConverter.validateProbandToUpdate(document.getSet());
        clinicalConverter.validateReportToUpdate(document.getSet());

        String[] objectAcceptedParams = new String[]{QueryParams.COMMENTS.key()};
        ParamUtils.AddRemoveReplaceAction basicOperation = ParamUtils.AddRemoveReplaceAction.from(actionMap, QueryParams.COMMENTS.key(),
                ParamUtils.AddRemoveReplaceAction.ADD);
        switch (basicOperation) {
            case REMOVE:
                fixCommentsForRemoval(parameters);
                filterObjectParams(parameters, document.getPull(), objectAcceptedParams);
                break;
            case ADD:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
            case REPLACE:
                filterReplaceParams(parameters.getAsList(QueryParams.COMMENTS.key(), ClinicalComment.class), document,
                        ClinicalComment::getDate, QueryParams.COMMENTS_DATE.key());
                break;
            default:
                throw new IllegalStateException("Unknown operation " + basicOperation);
        }

        objectAcceptedParams = new String[]{ANALYSTS.key()};
        ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, ANALYSTS.key(),
                ParamUtils.BasicUpdateAction.ADD);
        switch (operation) {
            case SET:
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                break;
            case REMOVE:
                fixAnalystsForRemoval(parameters);
                filterObjectParams(parameters, document.getPull(), objectAcceptedParams);
                break;
            case ADD:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
            default:
                throw new IllegalStateException("Unknown operation " + basicOperation);
        }

        objectAcceptedParams = new String[]{QueryParams.FILES.key()};
        operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.FILES.key(),
                ParamUtils.BasicUpdateAction.ADD);
        switch (operation) {
            case SET:
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                clinicalConverter.validateFilesToUpdate(document.getSet());
                break;
            case REMOVE:
                fixFilesForRemoval(parameters, FILES.key());
                filterObjectParams(parameters, document.getPull(), objectAcceptedParams);
                break;
            case ADD:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                clinicalConverter.validateFilesToUpdate(document.getAddToSet());
                break;
            default:
                throw new IllegalStateException("Unknown operation " + basicOperation);
        }

        objectAcceptedParams = new String[]{FLAGS.key()};
        operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.FLAGS.key(), ParamUtils.BasicUpdateAction.ADD);
        switch (operation) {
            case SET:
                filterObjectParams(parameters, document.getSet(), objectAcceptedParams);
                break;
            case REMOVE:
                fixFlagsForRemoval(parameters);
                filterObjectParams(parameters, document.getPull(), objectAcceptedParams);
                break;
            case ADD:
                filterObjectParams(parameters, document.getAddToSet(), objectAcceptedParams);
                break;
            default:
                throw new IllegalStateException("Unknown operation " + basicOperation);
        }

        // Panels
        if (parameters.containsKey(PANELS.key())) {
            operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.PANELS.key(), ParamUtils.BasicUpdateAction.ADD);
            String[] panelParams = {QueryParams.PANELS.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), panelParams);
                    clinicalConverter.validatePanelsToUpdate(document.getSet());
                    break;
                case REMOVE:
                    fixPanelsForRemoval(parameters);
                    filterObjectParams(parameters, document.getPull(), panelParams);
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), panelParams);
                    clinicalConverter.validatePanelsToUpdate(document.getAddToSet());
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        // Secondary interpretations
        if (parameters.containsKey(QueryParams.SECONDARY_INTERPRETATIONS.key())) {
            operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.SECONDARY_INTERPRETATIONS.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            String[] secondaryInterpretationParams = {QueryParams.SECONDARY_INTERPRETATIONS.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), secondaryInterpretationParams);
                    clinicalConverter.validateSecondaryInterpretationsToUpdate(document.getSet());
                    break;
                case REMOVE:
                    filterObjectParams(parameters, document.getPullAll(), secondaryInterpretationParams);
                    clinicalConverter.validateSecondaryInterpretationsToUpdate(document.getPullAll());
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), secondaryInterpretationParams);
                    clinicalConverter.validateSecondaryInterpretationsToUpdate(document.getAddToSet());
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        if (clinicalAuditList != null && !clinicalAuditList.isEmpty()) {
            List<Document> documentAuditList = new ArrayList<>(clinicalAuditList.size());
            for (ClinicalAudit clinicalAudit : clinicalAuditList) {
                documentAuditList.add(getMongoDBDocument(clinicalAudit, "ClinicalAudit"));
            }
            document.getPush().put(AUDIT.key(), documentAuditList);
        }

        if (!document.toFinalUpdateDocument().isEmpty()) {
            String time = TimeUtils.getTime();
            if (StringUtils.isEmpty(parameters.getString(MODIFICATION_DATE.key()))) {
                // Update modificationDate param
                Date date = TimeUtils.toDate(time);
                document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
                document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
            }
            document.getSet().put(INTERNAL_LAST_MODIFIED, time);
        }

        return document;
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(clinicalCollection, studyId, permissionRuleId);
    }

    @Override
    public OpenCGAResult<?> delete(ClinicalAnalysis clinicalAnalysis, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> privateDelete(clientSession, clinicalAnalysis, clinicalAuditList));
        } catch (CatalogException e) {
            logger.error("Could not delete Clinical Analysis {}: {}", clinicalAnalysis.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete Clinical Analysis " + clinicalAnalysis.getId() + ": " + e.getMessage(),
                    e.getCause());
        }
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> delete(Query query, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        DBIterator<ClinicalAnalysis> iterator = iterator(query, ClinicalAnalysisManager.INCLUDE_CLINICAL_INTERPRETATION_IDS);

        OpenCGAResult<ClinicalAnalysis> result = OpenCGAResult.empty();
        while (iterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = iterator.next();

            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, clinicalAnalysis, clinicalAuditList)));
            } catch (CatalogException e) {
                logger.error("Could not delete Clinical Analysis {}: {}", clinicalAnalysis.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, clinicalAnalysis.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    @Override
    public OpenCGAResult delete(ClinicalAnalysis clinicalAnalysis)
            throws CatalogParameterException, CatalogAuthorizationException, CatalogDBException {
        throw new NotImplementedException("Use other delete method passing ClinicalAudit");
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        throw new NotImplementedException("Use other delete method passing ClinicalAudit");
    }

    OpenCGAResult<?> privateDelete(ClientSession clientSession, ClinicalAnalysis clinicalAnalysis, List<ClinicalAudit> clinicalAuditList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();

        // Check and delete any associated interpretation
        if (clinicalAnalysis.getInterpretation() != null && clinicalAnalysis.getInterpretation().getUid() > 0) {
            Query query = new Query()
                    .append(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), clinicalAnalysis.getStudyUid())
                    .append(InterpretationDBAdaptor.QueryParams.UID.key(), clinicalAnalysis.getInterpretation().getUid());

            OpenCGAResult<Interpretation> result = dbAdaptorFactory.getInterpretationDBAdaptor().get(clientSession, query,
                    QueryOptions.empty());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Internal error: Interpretation '" + clinicalAnalysis.getInterpretation().getId()
                        + "' not found.");
            }

            ClinicalAudit clinicalAudit = new ClinicalAudit(clinicalAuditList.get(0).getAuthor(),
                    ClinicalAudit.Action.DELETE_INTERPRETATION, "Delete interpretation '" + result.first().getId() + "'",
                    TimeUtils.getTime());

            // Delete primary interpretation
            dbAdaptorFactory.getInterpretationDBAdaptor().delete(clientSession, result.first(), Collections.singletonList(clinicalAudit),
                    clinicalAnalysis);
        }

        if (clinicalAnalysis.getSecondaryInterpretations() != null && !clinicalAnalysis.getSecondaryInterpretations().isEmpty()) {
            for (Interpretation interpretation : clinicalAnalysis.getSecondaryInterpretations()) {
                Query query = new Query()
                        .append(InterpretationDBAdaptor.QueryParams.STUDY_UID.key(), clinicalAnalysis.getStudyUid())
                        .append(InterpretationDBAdaptor.QueryParams.UID.key(), interpretation.getUid());
                OpenCGAResult<Interpretation> result = dbAdaptorFactory.getInterpretationDBAdaptor().get(clientSession, query,
                        QueryOptions.empty());
                if (result.getNumResults() == 0) {
                    throw new CatalogDBException("Internal error: Interpretation '" + interpretation.getId() + "' not found.");
                }

                ClinicalAudit clinicalAudit = new ClinicalAudit(clinicalAuditList.get(0).getAuthor(),
                        ClinicalAudit.Action.DELETE_INTERPRETATION, "Delete interpretation '" + result.first().getId() + "'",
                        TimeUtils.getTime());

                // Delete secondary interpretation
                dbAdaptorFactory.getInterpretationDBAdaptor().delete(clientSession, result.first(),
                        Collections.singletonList(clinicalAudit), clinicalAnalysis);
            }
        }

        // Add Audit to ClinicalAnalysis
        transactionalUpdate(clientSession, clinicalAnalysis, new ObjectMap(), Collections.emptyList(), clinicalAuditList,
                QueryOptions.empty());

        // And delete ClinicalAnalysis
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), clinicalAnalysis.getStudyUid())
                .append(UID.key(), clinicalAnalysis.getUid());
        Bson bsonQuery = parseQuery(query);
        versionedMongoDBAdaptor.delete(clientSession, bsonQuery);

        logger.debug("Clinical Analysis {}({}) deleted", clinicalAnalysis.getId(), clinicalAnalysis.getUid());
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
    }

    void removeFileReferences(ClientSession clientSession, long studyUid, long fileUid, Document file)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        ObjectMap parameters = new ObjectMap(FILES.key(), Collections.singletonList(file));
        ObjectMap actionMap = new ObjectMap(FILES.key(), ParamUtils.BasicUpdateAction.REMOVE);
        QueryOptions options = new QueryOptions(Constants.ACTIONS, actionMap);

        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.FILES_UID.key(), fileUid);
        OpenCGAResult<ClinicalAnalysis> result = get(query, ClinicalAnalysisManager.INCLUDE_CLINICAL_IDS);
        for (ClinicalAnalysis clinicalAnalysis : result.getResults()) {
            logger.debug("Removing file references from Clinical Analysis {}", clinicalAnalysis.getId());
            ClinicalAudit clinicalAudit = new ClinicalAudit("OPENCGA", ClinicalAudit.Action.UPDATE_CLINICAL_ANALYSIS, "File "
                    + file.getString(FileDBAdaptor.QueryParams.PATH.key()) + " was deleted. Remove file references from case.",
                    TimeUtils.getTime());
            transactionalUpdate(clientSession, clinicalAnalysis, parameters, null, Collections.singletonList(clinicalAudit), options);
        }
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(null, query, options);
    }

    public OpenCGAResult<ClinicalAnalysis> get(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<ClinicalAnalysis> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Document> nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeGet(null, query, options);
    }

    private OpenCGAResult<Document> nativeGet(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Document> nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<ClinicalAnalysis> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return iterator(null, query, options);
    }

    public DBIterator<ClinicalAnalysis> iterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options, null);
        return new ClinicalAnalysisCatalogMongoDBIterator<>(mongoCursor, clientSession, clinicalConverter, null, dbAdaptorFactory, options);
    }

    @Override
    public DBIterator<Document> nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(null, query, options);
    }

    private DBIterator<Document> nativeIterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new ClinicalAnalysisCatalogMongoDBIterator(mongoCursor, clientSession, null, null, dbAdaptorFactory, queryOptions);
    }

    @Override
    public DBIterator<ClinicalAnalysis> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, options, user);
        Document studyDocument = getStudyDocument(null, studyUid);
        UnaryOperator<Document> iteratorFilter = (d) -> filterAnnotationSets(dbAdaptorFactory.getOrganizationId(), studyDocument, d, user,
                StudyPermissions.Permissions.VIEW_CLINICAL_ANNOTATIONS.name(),
                ClinicalAnalysisPermissions.VIEW_ANNOTATIONS.name());
        return new ClinicalAnalysisCatalogMongoDBIterator(mongoCursor, null, clinicalConverter, iteratorFilter, dbAdaptorFactory, studyUid,
                user, options);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(query, queryOptions, user);
        Document studyDocument = getStudyDocument(null, studyUid);
        UnaryOperator<Document> iteratorFilter = (d) -> filterAnnotationSets(dbAdaptorFactory.getOrganizationId(), studyDocument, d, user,
                StudyPermissions.Permissions.VIEW_CLINICAL_ANNOTATIONS.name(),
                ClinicalAnalysisPermissions.VIEW_ANNOTATIONS.name());
        return new ClinicalAnalysisCatalogMongoDBIterator(mongoCursor, null, null, iteratorFilter, dbAdaptorFactory, studyUid, user,
                options);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(clientSession, query, options, null);
    }

    private MongoDBIterator<Document> getMongoCursor(Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(null, query, options, user);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = new QueryOptions(options);
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterQueryOptionsToIncludeKeys(qOptions, Arrays.asList(ID, PRIVATE_UID, PRIVATE_STUDY_UID));
        qOptions = removeInnerProjections(qOptions, PROBAND.key());
        qOptions = removeInnerProjections(qOptions, FAMILY.key());
        qOptions = removeInnerProjections(qOptions, PANELS.key());
        qOptions = removeInnerProjections(qOptions, FILES.key());
        qOptions = removeInnerProjections(qOptions, QueryParams.INTERPRETATION.key());
        qOptions = removeInnerProjections(qOptions, QueryParams.SECONDARY_INTERPRETATIONS.key());

        logger.debug("Clinical analysis query : {}", bson.toBsonDocument());
        MongoDBCollection collection = getQueryCollection(query, clinicalCollection, archiveClinicalCollection, deletedClinicalCollection);
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }

    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        return null;
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(clinicalCollection, bsonQuery, field, QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(clinicalCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        Bson bsonQuery = parseQuery(query, user);
        return groupBy(clinicalCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult distinct(long studyUid, String field, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        return new OpenCGAResult<>(clinicalCollection.distinct(field, bson));
    }

    @Override
    public OpenCGAResult<?> distinct(long studyUid, List<String> fields, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        StopWatch stopWatch = StopWatch.createStarted();
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        Set<String> results = new LinkedHashSet<>();
        for (String field : fields) {
            results.addAll(clinicalCollection.distinct(field, bson, String.class).getResults());
        }

        return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(), results.size(),
                new ArrayList<>(results), -1);
    }

    @Override
    public OpenCGAResult<FacetField> facet(long studyUid, Query query, String facet, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, userId);
        return facet(clinicalCollection, bson, facet);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(action);
        try (DBIterator<ClinicalAnalysis> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> clinicalAnalysis, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(clinicalAnalysis, "clinicalAnalysis");
        return new OpenCGAResult(clinicalCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult insert(long studyId, ClinicalAnalysis clinicalAnalysis, List<VariableSet> variableSetList,
                                List<ClinicalAudit> clinicalAuditList, QueryOptions options) throws CatalogException {
        try {
            return runTransaction(clientSession -> {
                long tmpStartTime = startQuery();
                logger.debug("Starting ClinicalAnalysis insert transaction for ClinicalAnalysis id '{}'", clinicalAnalysis.getId());
                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
                insert(clientSession, studyId, clinicalAnalysis, variableSetList, clinicalAuditList);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null);
            });
        } catch (Exception e) {
            logger.error("Could not create ClinicalAnalysis {}: {}", clinicalAnalysis.getId(), e.getMessage(), e);
            throw e;
        }
    }

    ClinicalAnalysis insert(ClientSession clientSession, long studyId, ClinicalAnalysis clinicalAnalysis, List<VariableSet> variableSetList,
                            List<ClinicalAudit> clinicalAudit)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (clinicalAnalysis.getInterpretation() != null) {
            InterpretationMongoDBAdaptor interpretationDBAdaptor = dbAdaptorFactory.getInterpretationDBAdaptor();
            Interpretation interpretation = interpretationDBAdaptor.insert(clientSession, studyId, clinicalAnalysis.getInterpretation());
            clinicalAnalysis.setInterpretation(interpretation);
        }

        if (StringUtils.isEmpty(clinicalAnalysis.getId())) {
            throw new CatalogDBException("Missing ClinicalAnalysis id");
        }
        if (!get(clientSession, new Query(ID, clinicalAnalysis.getId())
                .append(STUDY_UID.key(), studyId), new QueryOptions()).getResults().isEmpty()) {
            throw CatalogDBException.alreadyExists("ClinicalAnalysis", "id", clinicalAnalysis.getId());
        }

        long clinicalUid = getNewUid(clientSession);

        clinicalAnalysis.setAudit(clinicalAudit);

        clinicalAnalysis.setUid(clinicalUid);
        clinicalAnalysis.setStudyUid(studyId);
        if (StringUtils.isEmpty(clinicalAnalysis.getUuid())) {
            clinicalAnalysis.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.CLINICAL));
        }

        Document clinicalDocument = clinicalConverter.convertToStorageType(clinicalAnalysis, variableSetList);
        if (StringUtils.isNotEmpty(clinicalAnalysis.getCreationDate())) {
            clinicalDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(clinicalAnalysis.getCreationDate()));
        } else {
            clinicalDocument.put(PRIVATE_CREATION_DATE, TimeUtils.getDate());
        }
        clinicalDocument.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(clinicalAnalysis.getModificationDate())
                ? TimeUtils.toDate(clinicalAnalysis.getModificationDate()) : TimeUtils.getDate());
        clinicalDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());
        if (StringUtils.isEmpty(clinicalAnalysis.getDueDate())) {
            throw new CatalogDBException("Cannot create Clinical Analysis without a " + DUE_DATE.key());
        }
        clinicalDocument.put(PRIVATE_DUE_DATE, TimeUtils.toDate(clinicalAnalysis.getDueDate()));

        logger.debug("Inserting ClinicalAnalysis '{}' ({})...", clinicalAnalysis.getId(), clinicalAnalysis.getUid());
        versionedMongoDBAdaptor.insert(clientSession, clinicalDocument);
        logger.debug("ClinicalAnalysis '{}' successfully inserted", clinicalAnalysis.getId());

        return clinicalAnalysis;
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> get(long clinicalAnalysisUid, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(clinicalAnalysisUid);
        return get(new Query(QueryParams.UID.key(), clinicalAnalysisUid)
                .append(QueryParams.STUDY_UID.key(), getStudyId(clinicalAnalysisUid)), options);
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> get(long studyUid, String clinicalAnalysisId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(null, studyUid, clinicalAnalysisId, options);
    }

    public OpenCGAResult<ClinicalAnalysis> get(ClientSession clientSession, long studyUid, String clinicalAnalysisId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.ID.key(), clinicalAnalysisId);
        return get(clientSession, query, options);
    }

    @Override
    public OpenCGAResult<ClinicalAnalysis> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<ClinicalAnalysis> dbIterator = iterator(studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public long getStudyId(long clinicalAnalysisId) throws CatalogDBException {
        Bson query = new Document(PRIVATE_UID, clinicalAnalysisId);
        Bson projection = Projections.include(PRIVATE_STUDY_UID);
        DataResult<Document> queryResult = clinicalCollection.find(query, projection, null);

        if (!queryResult.getResults().isEmpty()) {
            Object studyId = queryResult.getResults().get(0).get(PRIVATE_STUDY_UID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.uidNotFound("ClinicalAnalysis", clinicalAnalysisId);
        }
    }

    /**
     * Update Family references from any Clinical Analysis where it was used.
     *
     * @param clientSession Client session.
     * @param family        Family object containing the latest version.
     * @throws CatalogDBException            CatalogDBException.
     * @throws CatalogParameterException     CatalogParameterException.
     * @throws CatalogAuthorizationException CatalogAuthorizationException.
     */
    void updateClinicalAnalysisFamilyReferences(ClientSession clientSession, Family family)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        // We only update clinical analysis that are not locked. Locked ones will remain pointing to old references
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), family.getStudyUid())
                .append(QueryParams.FAMILY_UID.key(), family.getUid())
                .append(QueryParams.LOCKED.key(), false);
        DBIterator<ClinicalAnalysis> iterator = dbAdaptorFactory.getClinicalAnalysisDBAdaptor()
                .iterator(clientSession, query, ClinicalAnalysisManager.INCLUDE_CATALOG_DATA);

        while (iterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = iterator.next();

            if (clinicalAnalysis.getFamily().getUid() == family.getUid()
                    && clinicalAnalysis.getFamily().getVersion() < family.getVersion()) {
                Family familyCopy;
                try {
                    familyCopy = JacksonUtils.copy(family, Family.class);
                } catch (IOException e) {
                    throw new CatalogDBException("Internal error copying the Family object", e);
                }

                // Extract from the Family object, the members and samples actually related to the case.
                Set<Long> individualAndSampleUids = new HashSet<>();
                if (CollectionUtils.isNotEmpty(clinicalAnalysis.getFamily().getMembers())) {
                    for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                        individualAndSampleUids.add(member.getUid());
                        if (CollectionUtils.isNotEmpty(member.getSamples())) {
                            for (Sample sample : member.getSamples()) {
                                individualAndSampleUids.add(sample.getUid());
                            }
                        }
                    }
                    if (CollectionUtils.isNotEmpty(family.getMembers())) {
                        List<Individual> memberList = new ArrayList<>(clinicalAnalysis.getFamily().getMembers().size());
                        for (Individual member : family.getMembers()) {
                            if (individualAndSampleUids.contains(member.getUid())) {
                                Individual individualCopy;
                                try {
                                    individualCopy = JacksonUtils.copy(member, Individual.class);
                                } catch (IOException e) {
                                    throw new CatalogDBException("Internal error copying the Individual object", e);
                                }

                                if (CollectionUtils.isNotEmpty(member.getSamples())) {
                                    List<Sample> sampleList = new ArrayList<>();
                                    for (Sample sample : member.getSamples()) {
                                        if (individualAndSampleUids.contains(sample.getUid())) {
                                            sampleList.add(sample);
                                        }
                                    }
                                    individualCopy.setSamples(sampleList);
                                }
                                memberList.add(individualCopy);
                            }
                        }
                        familyCopy.setMembers(memberList);
                    }
                }

                ObjectMap params = new ObjectMap(QueryParams.FAMILY.key(), familyCopy);
                OpenCGAResult<?> result = dbAdaptorFactory.getClinicalAnalysisDBAdaptor().transactionalUpdate(clientSession,
                        clinicalAnalysis, params, Collections.emptyList(), null, QueryOptions.empty());
                if (result.getNumUpdated() != 1) {
                    throw new CatalogDBException("ClinicalAnalysis '" + clinicalAnalysis.getId() + "' could not be updated to the latest "
                            + "family version of '" + family.getId() + "'");
                }
            }
        }
    }

    /**
     * Update Panel references from any Clinical Analysis where it was used.
     *
     * @param clientSession Client session.
     * @param panel         Panel object containing the new version.
     * @throws CatalogDBException            CatalogDBException.
     * @throws CatalogParameterException     CatalogParameterException.
     * @throws CatalogAuthorizationException CatalogAuthorizationException.
     */
    void updateClinicalAnalysisPanelReferences(ClientSession clientSession, Panel panel)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        // We only update clinical analysis that are not locked. Locked ones will remain pointing to old references
        Query query = new Query()
                .append(STUDY_UID.key(), panel.getStudyUid())
                .append(PANELS_UID.key(), panel.getUid())
                .append(PANEL_LOCKED.key(), false)
                .append(LOCKED.key(), false);
        QueryOptions include = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                PANELS_UID.key(),
                PANELS.key() + "." + VERSION,
                INTERPRETATION.key() + "." + InterpretationDBAdaptor.QueryParams.PANELS_UID.key(),
                INTERPRETATION.key() + "." + InterpretationDBAdaptor.QueryParams.PANELS.key() + "." + VERSION,
                INTERPRETATION.key() + "." + InterpretationDBAdaptor.QueryParams.CLINICAL_ANALYSIS_ID.key(),
                INTERPRETATION.key() + "." + InterpretationDBAdaptor.QueryParams.LOCKED.key(),
                INTERPRETATION.key() + "." + InterpretationDBAdaptor.QueryParams.STUDY_UID.key(),
                SECONDARY_INTERPRETATIONS.key() + "." + InterpretationDBAdaptor.QueryParams.PANELS_UID.key(),
                SECONDARY_INTERPRETATIONS.key() + "." + InterpretationDBAdaptor.QueryParams.PANELS.key() + "." + VERSION,
                SECONDARY_INTERPRETATIONS.key() + "." + InterpretationDBAdaptor.QueryParams.CLINICAL_ANALYSIS_ID.key(),
                SECONDARY_INTERPRETATIONS.key() + "." + InterpretationDBAdaptor.QueryParams.STUDY_UID.key()
        ));
        DBIterator<ClinicalAnalysis> iterator = dbAdaptorFactory.getClinicalAnalysisDBAdaptor().iterator(clientSession, query, include);

        while (iterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = iterator.next();

            // Update panel from CA
            List<Panel> panelList = new ArrayList<>(clinicalAnalysis.getPanels().size());
            for (Panel caPanel : clinicalAnalysis.getPanels()) {
                if (caPanel.getUid() == panel.getUid()) {
                    panelList.add(panel);
                } else {
                    panelList.add(caPanel);
                }
            }
            Map<String, Object> actionMap = new HashMap<>();
            actionMap.put(PANELS.key(), ParamUtils.BasicUpdateAction.SET);
            QueryOptions updateOptions = new QueryOptions(Constants.ACTIONS, actionMap);
            ObjectMap params = new ObjectMap(PANELS.key(), panelList);
            transactionalUpdate(clientSession, clinicalAnalysis, params, Collections.emptyList(), null, updateOptions);

            // Update references from Interpretations
            dbAdaptorFactory.getInterpretationDBAdaptor().updateInterpretationPanelReferences(clientSession, clinicalAnalysis, panel);
        }
    }

    protected Bson parseQuery(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, null);
    }

    private Bson parseQuery(Query query, String user) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, null, user);
    }

    protected Bson parseQuery(Query query, Document extraQuery)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return parseQuery(query, extraQuery, null);
    }

    private Bson parseQuery(Query query, Document extraQuery, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        List<Bson> andBsonList = new ArrayList<>();
        Document annotationDocument = null;

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        if (queryCopy.containsKey(QueryParams.STUDY_UID.key())
                && (StringUtils.isNotEmpty(user) || queryCopy.containsKey(ParamConstants.ACL_PARAM))) {
            Document studyDocument = getStudyDocument(null, queryCopy.getLong(QueryParams.STUDY_UID.key()));
            boolean simplifyPermissions = simplifyPermissions();

            if (queryCopy.containsKey(ParamConstants.ACL_PARAM)) {
                andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, queryCopy, Enums.Resource.CLINICAL_ANALYSIS, user,
                        simplifyPermissions));
            } else {
                if (containsAnnotationQuery(query)) {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user,
                            ClinicalAnalysisPermissions.VIEW_ANNOTATIONS.name(), Enums.Resource.CLINICAL_ANALYSIS, simplifyPermissions));
                } else {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, ClinicalAnalysisPermissions.VIEW.name(),
                            Enums.Resource.CLINICAL_ANALYSIS, simplifyPermissions));
                }
            }

            queryCopy.remove(ParamConstants.ACL_PARAM);
        }

        if ("all".equalsIgnoreCase(queryCopy.getString(QueryParams.VERSION.key()))) {
            queryCopy.put(Constants.ALL_VERSIONS, true);
            queryCopy.remove(QueryParams.VERSION.key());
        }
        boolean uidVersionQueryFlag = versionedMongoDBAdaptor.generateUidVersionQuery(queryCopy, andBsonList);

        for (Map.Entry<String, Object> entry : queryCopy.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                if (Constants.ALL_VERSIONS.equals(entry.getKey()) || Constants.PRIVATE_ANNOTATION_PARAM_TYPES.equals(entry.getKey())) {
                    continue;
                }
                throw new CatalogDBException("Unexpected parameter " + entry.getKey() + ". The parameter does not exist or cannot be "
                        + "queried for.");
            }
            try {
                switch (queryParam) {
                    case UID:
                        addAutoOrQuery(PRIVATE_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case STUDY_UID:
                        addAutoOrQuery(PRIVATE_STUDY_UID, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case DISORDER:
                        addDefaultOrQueryFilter(queryParam.key(), queryParam.key(), queryCopy, andBsonList);
                        break;
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case DUE_DATE:
                        addAutoOrQuery(PRIVATE_DUE_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case INDIVIDUAL:
                        List<Bson> queryList = new ArrayList<>();
                        addAutoOrQuery(PROBAND_UID.key(), queryParam.key(), queryCopy, PROBAND_UID.type(), queryList);
                        addAutoOrQuery(FAMILY_MEMBERS_UID.key(), queryParam.key(), queryCopy, FAMILY_MEMBERS_UID.type(), queryList);
                        andBsonList.add(Filters.or(queryList));
                        break;
                    case SAMPLE:
                        queryList = new ArrayList<>();
                        addAutoOrQuery(PROBAND_SAMPLES_UID.key(), queryParam.key(), queryCopy, PROBAND_SAMPLES_UID.type(), queryList);
                        addAutoOrQuery(FAMILY_MEMBERS_SAMPLES_UID.key(), queryParam.key(), queryCopy, FAMILY_MEMBERS_SAMPLES_UID.type(),
                                queryList);
                        andBsonList.add(Filters.or(queryList));
                        break;
                    case STATUS:
                    case STATUS_ID:
                        addAutoOrQuery(STATUS_ID.key(), queryParam.key(), queryCopy, STATUS_ID.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        queryCopy.put(queryParam.key(), InternalStatus.getPositiveStatus(InternalStatus.STATUS_LIST,
                                queryCopy.getString(queryParam.key())));
                        addAutoOrQuery(INTERNAL_STATUS_ID.key(), queryParam.key(), queryCopy, INTERNAL_STATUS_ID.type(), andBsonList);
                        break;
                    case ANNOTATION:
                        if (annotationDocument == null) {
                            annotationDocument = createAnnotationQuery(queryCopy.getString(QueryParams.ANNOTATION.key()),
                                    queryCopy.get(Constants.PRIVATE_ANNOTATION_PARAM_TYPES, ObjectMap.class));
                        }
                        break;
                    case SNAPSHOT:
                        addAutoOrQuery(RELEASE_FROM_VERSION, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    // Other parameter that can be queried.
                    case ID:
                    case UUID:
                    case TYPE:
                    case PANEL_LOCKED:
                    case LOCKED:
                    case FILES_UID:
                    case PROBAND_UID:
                    case PROBAND_SAMPLES_UID:
                    case FAMILY_UID:
                    case FAMILY_MEMBERS_UID:
                    case FAMILY_MEMBERS_SAMPLES_UID:
                    case PANELS_UID:
                    case ANALYSTS_ID:
                    case PRIORITY_ID:
                    case FLAGS_ID:
                    case QUALITY_CONTROL_SUMMARY:
                    case VERSION:
                    case RELEASE:
                    case COMMENTS_DATE:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                logger.error("Error with {}: {}", entry.getKey(), entry.getValue());
                throw new CatalogDBException(e);
            }
        }

        // If the user doesn't look for a concrete version...
        if (!uidVersionQueryFlag && !queryCopy.getBoolean(Constants.ALL_VERSIONS) && !queryCopy.containsKey(QueryParams.VERSION.key())) {
            if (queryCopy.containsKey(QueryParams.SNAPSHOT.key())) {
                // If the user looks for anything from some release, we will try to find the latest from the release (snapshot)
                andBsonList.add(Filters.eq(LAST_OF_RELEASE, true));
            } else {
                // Otherwise, we will always look for the latest version
                andBsonList.add(Filters.eq(LAST_OF_VERSION, true));
            }
        }

        if (annotationDocument != null && !annotationDocument.isEmpty()) {
            andBsonList.add(annotationDocument);
        }
        if (extraQuery != null && !extraQuery.isEmpty()) {
            andBsonList.add(extraQuery);
        }
        if (!andBsonList.isEmpty()) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }
}
