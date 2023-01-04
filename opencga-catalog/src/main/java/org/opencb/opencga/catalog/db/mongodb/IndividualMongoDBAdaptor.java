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
import com.mongodb.client.model.Updates;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FamilyDBAdaptor;
import org.opencb.opencga.catalog.db.api.IndividualDBAdaptor;
import org.opencb.opencga.catalog.db.mongodb.converters.IndividualConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.IndividualCatalogMongoDBIterator;
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
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.InternalStatus;
import org.opencb.opencga.core.models.common.RgaIndex;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualPermissions;
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
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.api.ClinicalAnalysisDBAdaptor.QueryParams.MODIFICATION_DATE;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.filterAnnotationSets;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class IndividualMongoDBAdaptor extends AnnotationMongoDBAdaptor<Individual> implements IndividualDBAdaptor {

    private final MongoDBCollection individualCollection;
    private final MongoDBCollection archiveIndividualCollection;
    private final MongoDBCollection deletedIndividualCollection;
    private final IndividualConverter individualConverter;
    private final VersionedMongoDBAdaptor versionedMongoDBAdaptor;

    private final FamilyMongoDBAdaptor familyDBAdaptor;

    public IndividualMongoDBAdaptor(MongoDBCollection individualCollection, MongoDBCollection archiveIndividualCollection,
                                    MongoDBCollection deletedIndividualCollection, Configuration configuration,
                                    MongoDBAdaptorFactory dbAdaptorFactory) {
        super(configuration, LoggerFactory.getLogger(IndividualMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.individualCollection = individualCollection;
        this.archiveIndividualCollection = archiveIndividualCollection;
        this.deletedIndividualCollection = deletedIndividualCollection;
        this.individualConverter = new IndividualConverter();
        this.versionedMongoDBAdaptor = new VersionedMongoDBAdaptor(individualCollection, archiveIndividualCollection,
                deletedIndividualCollection);

        this.familyDBAdaptor = dbAdaptorFactory.getCatalogFamilyDBAdaptor();
    }

    @Override
    protected MongoDBCollection getCollection() {
        return individualCollection;
    }

    public boolean exists(ClientSession clientSession, long individualId) {
        return individualCollection.count(clientSession, new Document(PRIVATE_UID, individualId)).getNumMatches() != 0;
    }

    @Override
    public OpenCGAResult nativeInsert(Map<String, Object> individual, String userId) throws CatalogDBException {
        Document document = getMongoDBDocument(individual, "individual");
        return new OpenCGAResult(individualCollection.insert(document, null));
    }

    @Override
    public OpenCGAResult insert(long studyId, Individual individual, List<VariableSet> variableSetList, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            return runTransaction(clientSession -> {
                long tmpStartTime = startQuery();
                logger.debug("Starting individual insert transaction for individual id '{}'", individual.getId());
                // TODO: Add loggers to every action. Test it.

                dbAdaptorFactory.getCatalogStudyDBAdaptor().checkId(studyId);
                insert(clientSession, studyId, individual, variableSetList);
                return endWrite(tmpStartTime, 1, 1, 0, 0, null);
            });
        } catch (Exception e) {
            logger.error("Could not create individual {}: {}", individual.getId(), e.getMessage(), e);
            throw e;
        }
    }

    Individual insert(ClientSession clientSession, long studyId, Individual individual, List<VariableSet> variableSetList)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        // First we check if we need to create any samples and update current list of samples with the ones created
        if (individual.getSamples() != null && !individual.getSamples().isEmpty()) {
            List<Sample> sampleList = new ArrayList<>(individual.getSamples().size());

            SampleMongoDBAdaptor sampleDBAdaptor = dbAdaptorFactory.getCatalogSampleDBAdaptor();

            for (Sample sample : individual.getSamples()) {
                if (sample.getUid() <= 0) {
                    logger.debug("Sample '{}' needs to be created. Inserting sample...", sample.getId());
                    // Sample needs to be created
                    Sample newSample = sampleDBAdaptor.insert(clientSession, studyId, sample, variableSetList);
                    sampleList.add(newSample);
                } else {
                    logger.debug("Sample '{}' was already registered. No need to create it.", sample.getId());
                    sampleList.add(sample);
                }
                individual.setSamples(sampleList);
            }
        }

        if (StringUtils.isEmpty(individual.getId())) {
            throw new CatalogDBException("Missing individual id");
        }
        if (!get(clientSession, new Query(QueryParams.ID.key(), individual.getId())
                .append(QueryParams.STUDY_UID.key(), studyId), new QueryOptions()).getResults().isEmpty()) {
            throw CatalogDBException.alreadyExists("Individual", "id", individual.getId());
        }
        if (individual.getFather() != null && individual.getFather().getUid() > 0
                && !exists(clientSession, individual.getFather().getUid())) {
            throw CatalogDBException.idNotFound("Individual", individual.getFather().getId());
        }
        if (individual.getMother() != null && individual.getMother().getUid() > 0
                && !exists(clientSession, individual.getMother().getUid())) {
            throw CatalogDBException.idNotFound("Individual", individual.getMother().getId());
        }

        long individualUid = getNewUid();

        individual.setUid(individualUid);
        individual.setStudyUid(studyId);
        individual.setVersion(1);
        if (StringUtils.isEmpty(individual.getUuid())) {
            individual.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.INDIVIDUAL));
        }

        Document individualDocument = individualConverter.convertToStorageType(individual, variableSetList);

        // Versioning private parameters
        individualDocument.put(RELEASE_FROM_VERSION, Arrays.asList(individual.getRelease()));
        individualDocument.put(LAST_OF_VERSION, true);
        individualDocument.put(LAST_OF_RELEASE, true);
        individualDocument.put(PRIVATE_CREATION_DATE, StringUtils.isNotEmpty(individual.getCreationDate())
                ? TimeUtils.toDate(individual.getCreationDate())
                : TimeUtils.getDate());
        individualDocument.put(PRIVATE_MODIFICATION_DATE, StringUtils.isNotEmpty(individual.getModificationDate())
                ? TimeUtils.toDate(individual.getModificationDate()) : TimeUtils.getDate());
        individualDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        logger.debug("Inserting individual '{}' ({})...", individual.getId(), individual.getUid());
        versionedMongoDBAdaptor.insert(clientSession, individualDocument);
        logger.debug("Individual '{}' successfully inserted", individual.getId());

        if (individual.getSamples() != null && !individual.getSamples().isEmpty()) {
            // We associate the samples to the individual just created
            List<Long> sampleUids = individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toList());
            dbAdaptorFactory.getCatalogSampleDBAdaptor().updateIndividualFromSampleCollection(clientSession, studyId, sampleUids,
                    individual.getId());
        }

        return individual;
    }

    void updateFamilyReferences(ClientSession clientSession, long studyUid, List<String> individualIds, String familyId,
                                ParamUtils.BasicUpdateAction action)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        Bson bsonUpdate;
        switch (action) {
            case ADD:
                bsonUpdate = Updates.addToSet(QueryParams.FAMILY_IDS.key(), familyId);
                break;
            case REMOVE:
                bsonUpdate = Updates.pull(QueryParams.FAMILY_IDS.key(), familyId);
                break;
            case SET:
            default:
                throw new IllegalArgumentException("Unexpected action '" + action + "'");
        }

        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.ID.key(), individualIds);
        Bson bsonQuery = parseQuery(query);

        versionedMongoDBAdaptor.update(clientSession, bsonQuery, () -> {
                    DataResult update = individualCollection.update(clientSession, bsonQuery, bsonUpdate,
                            new QueryOptions(MongoDBCollection.MULTI, true));
                    if (update.getNumMatches() == 0) {
                        throw new CatalogDBException("Could not update family references in individuals");
                    }
                    return null;
                }, Collections.singletonList(QueryParams.SAMPLES_IDS.key()), this::iterator,
                (DBIterator<Individual> iterator) -> updateReferencesAfterIndividualVersionIncrement(clientSession, studyUid, iterator));
    }

    @Override
    public long getStudyId(long individualId) throws CatalogDBException {
        DataResult<Document> result =
                individualCollection.find(new Document(PRIVATE_UID, individualId), new Document(PRIVATE_STUDY_UID, 1), null);

        if (!result.getResults().isEmpty()) {
            return (long) result.getResults().get(0).get(PRIVATE_STUDY_UID);
        } else {
            throw CatalogDBException.uidNotFound("Individual", individualId);
        }
    }

    @Override
    public OpenCGAResult updateProjectRelease(long studyId, int release)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyId)
                .append(QueryParams.SNAPSHOT.key(), release - 1);
        Bson bson = parseQuery(query);
        return versionedMongoDBAdaptor.updateWithoutVersionIncrement(bson, () -> {
            Document update = new Document("$addToSet", new Document(RELEASE_FROM_VERSION, release));
            QueryOptions queryOptions = new QueryOptions("multi", true);

            return new OpenCGAResult(individualCollection.update(bson, update, queryOptions));
        });
    }

    @Override
    public OpenCGAResult unmarkPermissionRule(long studyId, String permissionRuleId) throws CatalogException {
        return unmarkPermissionRule(individualCollection, studyId, permissionRuleId);
    }

    @Override
    public OpenCGAResult<Long> count(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query);
    }

    public OpenCGAResult<Long> count(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult<>(individualCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult<Long> count(Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return count(null, query, user);
    }

    OpenCGAResult<Long> count(ClientSession clientSession, Query query, String user)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query, user);
        logger.debug("Individual count: query : {}, dbTime: {}", bson.toBsonDocument());
        return new OpenCGAResult<>(individualCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult stats(Query query) {
        return null;
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

        OpenCGAResult<Individual> individualDataResult = get(id, queryOptions);
        if (individualDataResult.first().getAnnotationSets().isEmpty()) {
            return new OpenCGAResult<>(individualDataResult.getTime(), individualDataResult.getEvents(), 0, Collections.emptyList(), 0);
        } else {
            List<AnnotationSet> annotationSets = individualDataResult.first().getAnnotationSets();
            int size = annotationSets.size();
            return new OpenCGAResult<>(individualDataResult.getTime(), individualDataResult.getEvents(), size, annotationSets, size);
        }
    }

    @Override
    public OpenCGAResult update(long id, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(id, parameters, null, queryOptions);
    }

    @Override
    public OpenCGAResult update(long individualUid, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        OpenCGAResult<Individual> dataResult = get(individualUid, options);

        if (dataResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not update individual. Individual uid '" + individualUid + "' not found.");
        }

        try {
            return runTransaction(clientSession -> privateUpdate(clientSession, dataResult.first(), parameters, variableSetList,
                    queryOptions));
        } catch (CatalogDBException e) {
            logger.error("Could not update individual {}: {}", dataResult.first().getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not update individual " + dataResult.first().getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return update(query, parameters, Collections.emptyList(), queryOptions);
    }

    @Override
    public OpenCGAResult update(Query query, ObjectMap parameters, List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (parameters.containsKey(QueryParams.ID.key())) {
            // We need to check that the update is only performed over 1 single individual
            if (count(query).getNumMatches() != 1) {
                throw new CatalogDBException("Operation not supported: '" + QueryParams.ID.key()
                        + "' can only be updated for one individual");
            }
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.ID.key(), QueryParams.UID.key(), QueryParams.VERSION.key(), QueryParams.STUDY_UID.key()));
        DBIterator<Individual> iterator = iterator(query, options);

        OpenCGAResult<Individual> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Individual individual = iterator.next();

            try {
                result.append(runTransaction(clientSession -> privateUpdate(clientSession, individual, parameters, variableSetList,
                        queryOptions)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not update individual {}: {}", individual.getId(), e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, individual.getId(), e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }
        return result;
    }

    OpenCGAResult<Object> privateUpdate(ClientSession clientSession, Individual individual, ObjectMap parameters,
                                        List<VariableSet> variableSetList, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long tmpStartTime = startQuery();
        Query tmpQuery = new Query()
                .append(QueryParams.STUDY_UID.key(), individual.getStudyUid())
                .append(QueryParams.UID.key(), individual.getUid());
        Bson bson = parseQuery(tmpQuery);
        return versionedMongoDBAdaptor.update(clientSession, bson, () -> {
            DataResult result = updateAnnotationSets(clientSession, individual.getUid(), parameters, variableSetList, queryOptions, true);
            UpdateDocument updateDocument = parseAndValidateUpdateParams(clientSession, parameters, tmpQuery, queryOptions);
            Document individualUpdate = updateDocument.toFinalUpdateDocument();

            if (individualUpdate.isEmpty() && result.getNumUpdated() == 0) {
                if (!parameters.isEmpty()) {
                    logger.error("Non-processed update parameters: {}", parameters.keySet());
                }
                throw new CatalogDBException("Nothing to be updated");
            }

            List<Event> events = new ArrayList<>();
            if (!individualUpdate.isEmpty()) {
                Bson finalQuery = parseQuery(tmpQuery);

                logger.debug("Individual update: query : {}, update: {}", finalQuery.toBsonDocument(), individualUpdate.toBsonDocument());

                result = individualCollection.update(clientSession, finalQuery, individualUpdate, new QueryOptions("multi", true));

                if (result.getNumMatches() == 0) {
                    throw new CatalogDBException("Individual " + individual.getId() + " not found");
                }
                if (result.getNumUpdated() == 0) {
                    events.add(new Event(Event.Type.WARNING, individual.getId(), "Individual was already updated"));
                }

                if (!updateDocument.getAttributes().isEmpty()) {
                    List<Long> addedSamples = updateDocument.getAttributes().getAsLongList("ADDED_SAMPLES");
                    List<Long> removedSamples = updateDocument.getAttributes().getAsLongList("REMOVED_SAMPLES");

                    // Set new individual reference
                    dbAdaptorFactory.getCatalogSampleDBAdaptor().updateIndividualFromSampleCollection(clientSession,
                            individual.getStudyUid(), addedSamples, individual.getId());

                    // Set individual reference to ""
                    dbAdaptorFactory.getCatalogSampleDBAdaptor().updateIndividualFromSampleCollection(clientSession,
                            individual.getStudyUid(), removedSamples, "");
                }

                // If the list of disorders or phenotypes is altered, we will need to update the corresponding effective lists
                // of the families associated (if any)
                if (parameters.containsKey(QueryParams.DISORDERS.key()) || parameters.containsKey(QueryParams.PHENOTYPES.key())) {
                    recalculateFamilyDisordersPhenotypes(clientSession, individual);
                }

                if (StringUtils.isNotEmpty(parameters.getString(QueryParams.ID.key()))) {
                    // We need to update the individual id reference in all its samples
                    dbAdaptorFactory.getCatalogSampleDBAdaptor().updateIndividualIdFromSamples(clientSession, individual.getStudyUid(),
                            individual.getId(), parameters.getString(QueryParams.ID.key()));

                    // Update the family roles
                    familyDBAdaptor.updateIndividualIdFromFamilies(clientSession, individual.getStudyUid(),
                            individual.getUid(), individual.getId(), parameters.getString(QueryParams.ID.key()));
                }

                if (parameters.containsKey(QueryParams.FATHER_UID.key()) || parameters.containsKey(QueryParams.MOTHER_UID.key())) {
                    // If the parents have changed, we need to check family roles
                    recalculateFamilyRolesForMember(clientSession, individual.getStudyUid(), individual.getUid());
                }

                logger.debug("Individual {} successfully updated", individual.getId());
            }

            return endWrite(tmpStartTime, 1, 1, events);
        }, Collections.singletonList(QueryParams.SAMPLES_IDS.key()), this::iterator,
                (DBIterator<Individual> iterator) -> updateReferencesAfterIndividualVersionIncrement(clientSession,
                        individual.getStudyUid(), iterator));
    }

    private void recalculateFamilyRolesForMember(ClientSession clientSession, long studyUid, long memberUid)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), memberUid);
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FamilyDBAdaptor.QueryParams.ID.key(), FamilyDBAdaptor.QueryParams.UID.key(),
                        FamilyDBAdaptor.QueryParams.VERSION.key(), FamilyDBAdaptor.QueryParams.STUDY_UID.key(),
                        FamilyDBAdaptor.QueryParams.MEMBERS.key() + "." + IndividualDBAdaptor.QueryParams.ID.key()));
        try (DBIterator<Family> iterator = familyDBAdaptor.iterator(clientSession, query, options)) {
            while (iterator.hasNext()) {
                Family family = iterator.next();
                familyDBAdaptor.privateUpdate(clientSession, family, new ObjectMap(), null,
                        new QueryOptions(ParamConstants.FAMILY_UPDATE_ROLES_PARAM, true));
            }
        }
    }

    private void updateReferencesAfterIndividualVersionIncrement(ClientSession clientSession, long studyUid,
                                                                 DBIterator<Individual> iterator)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Map<Long, Individual> individualMap = new HashMap<>();
        while (iterator.hasNext()) {
            Individual individual = iterator.next();
            updateClinicalAnalysisIndividualReferences(clientSession, individual);
            individualMap.put(individual.getUid(), individual);
        }

        if (!individualMap.isEmpty()) {
            familyDBAdaptor.updateIndividualReferencesInFamily(clientSession, studyUid, individualMap);
        }
    }

    private void recalculateFamilyDisordersPhenotypes(ClientSession clientSession, Individual individual)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        // We fetch the current updated individual to know how the current list of disorders and phenotypes
        QueryOptions individualOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(QueryParams.PHENOTYPES.key(), QueryParams.DISORDERS.key()));
        Individual currentIndividual = get(clientSession, new Query()
                .append(QueryParams.STUDY_UID.key(), individual.getStudyUid())
                .append(QueryParams.UID.key(), individual.getUid()), individualOptions).first();

        Query familyQuery = new Query()
                .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), individual.getUid())
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), individual.getStudyUid());
        QueryOptions familyOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.STUDY_UID.key(),
                FamilyDBAdaptor.QueryParams.MEMBERS.key()));

        DBIterator<Family> familyIterator = familyDBAdaptor.iterator(clientSession, familyQuery, familyOptions);

        ObjectMap actionMap = new ObjectMap()
                .append(FamilyDBAdaptor.QueryParams.PHENOTYPES.key(), ParamUtils.BasicUpdateAction.SET)
                .append(FamilyDBAdaptor.QueryParams.DISORDERS.key(), ParamUtils.BasicUpdateAction.SET);
        QueryOptions familyUpdateOptions = new QueryOptions(Constants.ACTIONS, actionMap);

        while (familyIterator.hasNext()) {
            Family family = familyIterator.next();

            ObjectMap params = getNewFamilyDisordersAndPhenotypesToUpdate(family, currentIndividual.getDisorders(),
                    currentIndividual.getPhenotypes(), currentIndividual.getUid());

            familyDBAdaptor.privateUpdate(clientSession, family, params, null, familyUpdateOptions);
        }
    }

    /**
     * Calculates the new list of disorders and phenotypes considering Individual individual belongs to Family family and that individual
     * has a new list of disorders and phenotypes stored.
     *
     * @param family        Current Family as stored in DB.
     * @param disorders     List of disorders of member of the family individualUid .
     * @param phenotypes    List of phenotypes of member of the family individualUid.
     * @param individualUid Individual uid.
     * @return A new ObjectMap tp update the list of disorders and phenotypes in the Family.
     */
    private ObjectMap getNewFamilyDisordersAndPhenotypesToUpdate(Family family, List<Disorder> disorders, List<Phenotype> phenotypes,
                                                                 long individualUid) {
        Map<String, Disorder> disorderMap = new HashMap();
        Map<String, Phenotype> phenotypeMap = new HashMap();
        // Initialise the array of disorders and phenotypes with the content of the individual
        if (disorders != null) {
            for (Disorder disorder : disorders) {
                disorderMap.put(disorder.getId(), disorder);
            }
        }
        if (phenotypes != null) {
            for (Phenotype phenotype : phenotypes) {
                phenotypeMap.put(phenotype.getId(), phenotype);
            }
        }

        // We get the current list of phenotypes and disorders of the rest of family members discarding the one recently updated.
        for (Individual member : family.getMembers()) {
            if (member.getUid() != individualUid) {
                if (member.getDisorders() != null) {
                    for (Disorder disorder : member.getDisorders()) {
                        disorderMap.put(disorder.getId(), disorder);
                    }
                }
                if (member.getPhenotypes() != null) {
                    for (Phenotype phenotype : member.getPhenotypes()) {
                        phenotypeMap.put(phenotype.getId(), phenotype);
                    }
                }
            }
        }

        return new ObjectMap()
                .append(QueryParams.PHENOTYPES.key(), new ArrayList<>(phenotypeMap.values()))
                .append(QueryParams.DISORDERS.key(), new ArrayList<>(disorderMap.values()));
    }

    /**
     * Update Individual references from any Clinical Analysis where it was used.
     *
     * @param clientSession Client session.
     * @param individual    Individual object containing the latest version.
     */
    private void updateClinicalAnalysisIndividualReferences(ClientSession clientSession, Individual individual)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        // We only update clinical analysis that are not locked. Locked ones will remain pointing to old references
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), individual.getStudyUid())
                .append(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND_UID.key(), individual.getUid())
                .append(ClinicalAnalysisDBAdaptor.QueryParams.LOCKED.key(), false);
        DBIterator<ClinicalAnalysis> iterator = dbAdaptorFactory.getClinicalAnalysisDBAdaptor()
                .iterator(clientSession, query, ClinicalAnalysisManager.INCLUDE_CATALOG_DATA);

        while (iterator.hasNext()) {
            ClinicalAnalysis clinicalAnalysis = iterator.next();

            if (clinicalAnalysis.getProband().getUid() == individual.getUid()
                    && clinicalAnalysis.getProband().getVersion() < individual.getVersion()) {
                // We create a copy because we are going to modify the individual instance and it could be involved in more than one case
                Individual individualCopy;
                try {
                    individualCopy = JacksonUtils.copy(individual, Individual.class);
                } catch (IOException e) {
                    throw new CatalogDBException("Internal error copying the Individual object", e);
                }
                if (clinicalAnalysis.getProband().getSamples() != null) {
                    List<Sample> sampleList = new ArrayList<>(clinicalAnalysis.getProband().getSamples().size());
                    Set<Long> sampleUids = clinicalAnalysis.getProband().getSamples()
                            .stream()
                            .map(Sample::getUid)
                            .collect(Collectors.toSet());
                    for (Sample sample : individual.getSamples()) {
                        if (sampleUids.contains(sample.getUid())) {
                            sampleList.add(sample);
                        }
                    }
                    individualCopy.setSamples(sampleList);
                }

                ObjectMap params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), individualCopy);

                OpenCGAResult result = dbAdaptorFactory.getClinicalAnalysisDBAdaptor().update(clientSession, clinicalAnalysis, params, null,
                        QueryOptions.empty());
                if (result.getNumUpdated() != 1) {
                    throw new CatalogDBException("ClinicalAnalysis '" + clinicalAnalysis.getId() + "' could not be updated to the latest "
                            + "individual version of '" + individual.getId() + "'");
                }
            }
        }
    }

    /**
     * Checks whether the individual that is going to be deleted is in use in any Clinical Analysis.
     *
     * @param clientSession Client session.
     * @param individual    Individual to be altered.
     * @throws CatalogDBException CatalogDBException if the individual is in use in any Clinical Analysis.
     */
    private void checkInUseInClinicalAnalysis(ClientSession clientSession, Document individual)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        String individualId = individual.getString(QueryParams.ID.key());
        long individualUid = individual.getLong(PRIVATE_UID);
        long studyUid = individual.getLong(PRIVATE_STUDY_UID);

        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(ClinicalAnalysisDBAdaptor.QueryParams.INDIVIDUAL.key(), individualUid);
        OpenCGAResult<Long> count = dbAdaptorFactory.getClinicalAnalysisDBAdaptor().count(clientSession, query);
        if (count.getNumMatches() > 0) {
            throw new CatalogDBException("Could not delete individual '" + individualId + "'. Individual is in use in "
                    + count.getNumMatches() + " cases");
        }
    }

    UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one individual...
            Document individual = checkOnlyOneIndividualMatches(clientSession, query,
                    new QueryOptions(QueryOptions.INCLUDE, PRIVATE_STUDY_UID));

            // Check that the new individual name is still unique
            long studyId = individual.get(PRIVATE_STUDY_UID, Number.class).longValue();

            Query tmpQuery = new Query()
                    .append(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()))
                    .append(QueryParams.STUDY_UID.key(), studyId);
            OpenCGAResult<Long> count = count(clientSession, tmpQuery);
            if (count.getNumMatches() > 0) {
                throw new CatalogDBException("Cannot set id for individual. An individual with { id: '"
                        + parameters.get(QueryParams.ID.key()) + "'} already exists.");
            }

            document.getSet().put(QueryParams.ID.key(), parameters.get(QueryParams.ID.key()));
        }

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.POPULATION_NAME.key(), QueryParams.POPULATION_SUBPOPULATION.key(),
                QueryParams.POPULATION_DESCRIPTION.key(), QueryParams.KARYOTYPIC_SEX.key(), QueryParams.LIFE_STATUS.key(),
                QueryParams.DATE_OF_BIRTH.key()};
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

        String[] acceptedIntParams = {QueryParams.FATHER_UID.key(), QueryParams.MOTHER_UID.key()};
        filterLongParams(parameters, document.getSet(), acceptedIntParams);

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        String[] acceptedObjectParams = {QueryParams.LOCATION.key(), QueryParams.STATUS.key(), QueryParams.QUALITY_CONTROL.key(),
                QueryParams.ETHNICITY.key(), QueryParams.SEX.key(), QueryParams.INTERNAL_STATUS.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

        if (parameters.containsKey(QueryParams.INTERNAL_RGA.key())) {
            RgaIndex rgaIndex = parameters.get(QueryParams.INTERNAL_RGA.key(), RgaIndex.class);
            rgaIndex.setDate(TimeUtils.getTime());
            document.getSet().put(QueryParams.INTERNAL_RGA.key(), getMongoDBDocument(rgaIndex, "rga"));
        }

        //Check individualIds exist
        String[] individualIdParams = {QueryParams.FATHER_UID.key(), QueryParams.MOTHER_UID.key()};
        for (String individualIdParam : individualIdParams) {
            if (document.getSet().containsKey(individualIdParam)) {
                Long individualId1 = (Long) document.getSet().get(individualIdParam);
                if (individualId1 > 0 && !exists(individualId1)) {
                    throw CatalogDBException.uidNotFound("Individual " + individualIdParam, individualId1);
                }
            }
        }

        if (parameters.containsKey(QueryParams.SAMPLES.key())) {
            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.SAMPLES.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            processSampleChanges(clientSession, query, parameters, document, operation);
        }

        Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
        // Phenotypes
        if (parameters.containsKey(QueryParams.PHENOTYPES.key())) {
            ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.PHENOTYPES.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            String[] phenotypesParams = {QueryParams.PHENOTYPES.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), phenotypesParams);
                    break;
                case REMOVE:
                    dbAdaptorFactory.getCatalogSampleDBAdaptor().fixPhenotypesForRemoval(parameters);
                    filterObjectParams(parameters, document.getPull(), phenotypesParams);
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), phenotypesParams);
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }
        // Disorders
        if (parameters.containsKey(QueryParams.DISORDERS.key())) {
            ParamUtils.BasicUpdateAction operation = ParamUtils.BasicUpdateAction.from(actionMap, QueryParams.DISORDERS.key(),
                    ParamUtils.BasicUpdateAction.ADD);
            String[] disordersParams = {QueryParams.DISORDERS.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), disordersParams);
                    break;
                case REMOVE:
                    fixDisordersForRemoval(parameters);
                    filterObjectParams(parameters, document.getPull(), disordersParams);
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), disordersParams);
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
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

    void fixDisordersForRemoval(ObjectMap parameters) {
        if (parameters.get(QueryParams.DISORDERS.key()) == null) {
            return;
        }

        List<Document> disorderParamList = new LinkedList<>();
        for (Object disorder : parameters.getAsList(QueryParams.DISORDERS.key())) {
            if (disorder instanceof Phenotype) {
                disorderParamList.add(new Document("id", ((Phenotype) disorder).getId()));
            } else {
                disorderParamList.add(new Document("id", ((Map) disorder).get("id")));
            }
        }
        parameters.put(QueryParams.DISORDERS.key(), disorderParamList);
    }

    private void processSampleChanges(ClientSession clientSession, Query query, ObjectMap parameters, UpdateDocument updateDocument,
                                      ParamUtils.BasicUpdateAction operation)
            throws CatalogParameterException, CatalogDBException, CatalogAuthorizationException {
        // That can only be done to one individual...
        Document individualDoc = checkOnlyOneIndividualMatches(clientSession, query,
                new QueryOptions()
                        .append(QueryOptions.INCLUDE, Collections.singletonList(QueryParams.SAMPLES.key()))
                        .append(NATIVE_QUERY, true)
        );
        Individual individual = individualConverter.convertToDataModelType(individualDoc);

        Map<Long, Document> currentSampleUidMap = new HashMap<>();
        List<Document> currentSampleList = individualDoc.getList(QueryParams.SAMPLES.key(), Document.class);

        if (currentSampleList != null) {
            for (Document sample : currentSampleList) {
                long uid = sample.get(PRIVATE_UID, Number.class).longValue();
                currentSampleUidMap.put(uid, sample);
            }
        }

        List<Sample> sampleList = parameters.getAsList(QueryParams.SAMPLES.key(), Sample.class);
        if (operation == ParamUtils.BasicUpdateAction.SET || operation == ParamUtils.BasicUpdateAction.ADD) {
            // We will see which of the samples are actually new
            Map<Long, Integer> newSamplesMap = new HashMap<>();
            Map<Long, Integer> allSamplesMap = new HashMap<>();

            for (Sample sample : sampleList) {
                allSamplesMap.put(sample.getUid(), sample.getVersion());
                if (!currentSampleUidMap.containsKey(sample.getUid())) {
                    newSamplesMap.put(sample.getUid(), sample.getVersion());
                }
            }

            if (!newSamplesMap.isEmpty()) {
                updateDocument.getAttributes().put("ADDED_SAMPLES", new ArrayList<>(newSamplesMap.keySet()));
            }

            if (individual.getSamples() != null) {
                // If the individual already had some samples...
                switch (operation) {
                    case SET:
                        // We also need to see which samples existed and are not currently in the new list provided by the user
                        // to take the references to the individual out
                        List<Long> samplesToRemove = new ArrayList<>();
                        for (Sample sample : individual.getSamples()) {
                            if (!allSamplesMap.containsKey(sample.getUid())) {
                                samplesToRemove.add(sample.getUid());
                            }
                        }

                        if (!samplesToRemove.isEmpty()) {
                            updateDocument.getAttributes().put("REMOVED_SAMPLES", samplesToRemove);
                        }
                        break;
                    case ADD:
                        // We need to know which are the other existing samples to keep them as well (add them to the allSamplesMap)
                        for (Sample sample : individual.getSamples()) {
                            // We check instead of putting the sample directly because we don't want to keep the version passed by the user
                            if (!allSamplesMap.containsKey(sample.getUid())) {
                                allSamplesMap.put(sample.getUid(), sample.getVersion());
                            }
                        }
                        break;
                    default:
                        break;
                }
            }

            // Prepare the document containing the new list of samples to be added
            List<Document> updatedSampleList = new LinkedList<>();
            for (Map.Entry<Long, Integer> entry : allSamplesMap.entrySet()) {
                updatedSampleList.add(new Document()
                        .append(PRIVATE_UID, entry.getKey())
                        .append(VERSION, entry.getValue())
                );
            }

            // Update document operation
            updateDocument.getSet().append(QueryParams.SAMPLES.key(), updatedSampleList);
            individualConverter.validateSamplesToUpdate(updateDocument.getSet());
        } else if (operation == ParamUtils.BasicUpdateAction.REMOVE) {
            // We will only store the samples to be removed that are already associated to the individual
            List<Long> samplesToRemove = new ArrayList<>();
            List<Document> samplesToRemoveDocList = new ArrayList<>();

            for (Sample sample : sampleList) {
                if (currentSampleUidMap.containsKey(sample.getUid())) {
                    samplesToRemoveDocList.add(currentSampleUidMap.get(sample.getUid()));
                    samplesToRemove.add(sample.getUid());
                }
            }

            if (!samplesToRemove.isEmpty()) {
                updateDocument.getAttributes().put("REMOVED_SAMPLES", samplesToRemove);
            }

            // Update document operation
            updateDocument.getPullAll().append(QueryParams.SAMPLES.key(), samplesToRemoveDocList);
        }
    }

    private Document checkOnlyOneIndividualMatches(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query tmpQuery = new Query(query);
        // We take out ALL_VERSION from query just in case we get multiple results from the same individual...
        tmpQuery.remove(Constants.ALL_VERSIONS);

        OpenCGAResult<Document> individualDataResult = nativeGet(clientSession, tmpQuery, options);
        if (individualDataResult.getNumResults() == 0) {
            throw new CatalogDBException("Update individual: No individual found to be updated");
        }
        if (individualDataResult.getNumResults() > 1) {
            throw CatalogDBException.cannotUpdateMultipleEntries(QueryParams.ID.key(), "individual");
        }
        return individualDataResult.first();
    }

    @Override
    public OpenCGAResult delete(Individual individual) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        try {
            Query query = new Query()
                    .append(QueryParams.UID.key(), individual.getUid())
                    .append(QueryParams.STUDY_UID.key(), individual.getStudyUid());
            OpenCGAResult<Document> result = nativeGet(query, new QueryOptions());
            if (result.getNumResults() == 0) {
                throw new CatalogDBException("Could not find individual " + individual.getId() + " with uid " + individual.getUid());
            }
            return runTransaction(clientSession -> privateDelete(clientSession, result.first()));
        } catch (CatalogDBException e) {
            logger.error("Could not delete individual {}: {}", individual.getId(), e.getMessage(), e);
            throw new CatalogDBException("Could not delete individual " + individual.getId() + ": " + e.getMessage(), e.getCause());
        }
    }

    @Override
    public OpenCGAResult delete(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        DBIterator<Document> iterator = nativeIterator(query, QueryOptions.empty());

        OpenCGAResult<Individual> result = OpenCGAResult.empty();

        while (iterator.hasNext()) {
            Document individual = iterator.next();
            String individualId = individual.getString(QueryParams.ID.key());
            try {
                result.append(runTransaction(clientSession -> privateDelete(clientSession, individual)));
            } catch (CatalogDBException | CatalogParameterException | CatalogAuthorizationException e) {
                logger.error("Could not delete individual {}: {}", individualId, e.getMessage(), e);
                result.getEvents().add(new Event(Event.Type.ERROR, individualId, e.getMessage()));
                result.setNumMatches(result.getNumMatches() + 1);
            }
        }

        return result;
    }

    OpenCGAResult<Object> privateDelete(ClientSession clientSession, Document individualDocument)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        String individualId = individualDocument.getString(QueryParams.ID.key());
        long individualUid = individualDocument.getLong(PRIVATE_UID);
        long studyUid = individualDocument.getLong(PRIVATE_STUDY_UID);

        long tmpStartTime = startQuery();

        checkInUseInClinicalAnalysis(clientSession, individualDocument);

        logger.debug("Deleting individual {} ({})", individualId, individualUid);
        // Look for all the different family versions
        Query individualQuery = new Query()
                .append(QueryParams.UID.key(), individualUid)
                .append(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(individualQuery);
        versionedMongoDBAdaptor.delete(clientSession, bson);

        // Remove individual reference from the list of samples
        List<Document> sampleList = individualDocument.getList(QueryParams.SAMPLES.key(), Document.class);
        if (sampleList != null && !sampleList.isEmpty()) {
            // We set the individual id for those samples to ""
            List<Long> sampleUids = sampleList.stream().map(s -> s.get(PRIVATE_UID, Number.class).longValue()).collect(Collectors.toList());
            dbAdaptorFactory.getCatalogSampleDBAdaptor().updateIndividualFromSampleCollection(clientSession, studyUid, sampleUids, "");
        }

        // Remove individual from any list of members it might be part of
        Query familyQuery = new Query()
                .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), individualUid)
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(Constants.ALL_VERSIONS, true);

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.VERSION.key(),
                        FamilyDBAdaptor.QueryParams.MEMBERS.key()));
        try (DBIterator<Family> familyIterator = familyDBAdaptor.iterator(clientSession, familyQuery, queryOptions)) {
            while (familyIterator.hasNext()) {
                Family family = familyIterator.next();

                List<Individual> members = new ArrayList<>();
                for (Individual member : family.getMembers()) {
                    if (member.getUid() != individualUid) {
                        members.add(member);
                    }
                }
                // Remove the member and update the list of disorders and phenotypes
                ObjectMap params = getNewFamilyDisordersAndPhenotypesToUpdate(family, null, null, individualUid)
                        .append(FamilyDBAdaptor.QueryParams.MEMBERS.key(), members);

                Bson bsonQuery = familyDBAdaptor.parseQuery(new Query()
                        .append(FamilyDBAdaptor.QueryParams.UID.key(), family.getUid())
                        .append(FamilyDBAdaptor.QueryParams.VERSION.key(), family.getVersion())
                );
                Document update = familyDBAdaptor.parseAndValidateUpdateParams(clientSession, params, null).toFinalUpdateDocument();

                logger.debug("Remove individual references from family: Query: {}, update: {}", bsonQuery.toBsonDocument(),
                        update.toBsonDocument());
                DataResult result = familyDBAdaptor.getFamilyCollection().update(clientSession, bsonQuery, update,
                        new QueryOptions(MongoDBCollection.MULTI, true));
                logger.debug("Families found: {}, families updated: {}", result.getNumMatches(), result.getNumUpdated());
            }
        }

        logger.debug("Individual {}({}) deleted", individualId, individualUid);
        return endWrite(tmpStartTime, 1, 0, 0, 1, Collections.emptyList());
    }

    @Override
    public OpenCGAResult restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new NotImplementedException("Not yet implemented");
    }

    @Override
    public OpenCGAResult<Individual> get(long individualId, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        checkId(individualId);
        Query query = new Query(QueryParams.UID.key(), individualId)
                .append(QueryParams.STUDY_UID.key(), getStudyId(individualId));
        return get(query, options);
    }

    @Override
    public OpenCGAResult<Individual> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return get(null, studyUid, query, options, user);
    }

    OpenCGAResult<Individual> get(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Individual> dbIterator = iterator(clientSession, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult<Individual> get(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return get(null, query, options);
    }

    public OpenCGAResult<Individual> get(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Individual> dbIterator = iterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeGet(null, query, options);
    }

    public OpenCGAResult nativeGet(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, query, options)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeGet(null, studyUid, query, options, user);
    }

    public OpenCGAResult nativeGet(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        long startTime = startQuery();
        try (DBIterator<Document> dbIterator = nativeIterator(clientSession, studyUid, query, options, user)) {
            return endQuery(startTime, dbIterator);
        }
    }

    @Override
    public DBIterator<Individual> iterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return iterator(null, query, options);
    }

    DBIterator<Individual> iterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options);
        return new IndividualCatalogMongoDBIterator<>(mongoCursor, clientSession, individualConverter, null, dbAdaptorFactory, options);
    }

    @Override
    public DBIterator nativeIterator(Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return nativeIterator(null, query, options);
    }

    DBIterator nativeIterator(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions);
        return new IndividualCatalogMongoDBIterator<Document>(mongoCursor, clientSession, null, null, dbAdaptorFactory, options);
    }

    @Override
    public DBIterator<Individual> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return iterator(null, studyUid, query, options, user);
    }

    DBIterator<Individual> iterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, options, user);
        Document studyDocument = getStudyDocument(clientSession, studyUid);
        UnaryOperator<Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyPermissions.Permissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualPermissions.VIEW_ANNOTATIONS.name());

        return new IndividualCatalogMongoDBIterator<>(mongoCursor, clientSession, individualConverter, iteratorFilter, dbAdaptorFactory,
                studyUid, user, options);
    }

    @Override
    public DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        return nativeIterator(null, studyUid, query, options, user);
    }

    DBIterator nativeIterator(ClientSession clientSession, long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        QueryOptions queryOptions = options != null ? new QueryOptions(options) : new QueryOptions();
        queryOptions.put(NATIVE_QUERY, true);

        query.put(PRIVATE_STUDY_UID, studyUid);
        MongoDBIterator<Document> mongoCursor = getMongoCursor(clientSession, query, queryOptions, user);
        Document studyDocument = getStudyDocument(clientSession, studyUid);
        UnaryOperator<Document> iteratorFilter = (d) -> filterAnnotationSets(studyDocument, d, user,
                StudyPermissions.Permissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualPermissions.VIEW_ANNOTATIONS.name());

        return new IndividualCatalogMongoDBIterator<>(mongoCursor, clientSession, null, iteratorFilter, dbAdaptorFactory, studyUid, user,
                options);
    }

    private MongoDBIterator<Document> getMongoCursor(ClientSession clientSession, Query query, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return getMongoCursor(clientSession, query, options, null);
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

        qOptions = removeInnerProjections(qOptions, QueryParams.SAMPLES.key());
        qOptions = removeAnnotationProjectionOptions(qOptions);

        // FIXME we should be able to remove this now safely
        qOptions = filterOptions(qOptions, FILTER_ROUTE_INDIVIDUALS);
        fixAclProjection(qOptions);

        logger.debug("Individual get: query : {}", bson.toBsonDocument());
        MongoDBCollection collection = getQueryCollection(query, individualCollection, archiveIndividualCollection,
                deletedIndividualCollection);
        return collection.iterator(clientSession, bson, null, null, qOptions);
    }


    @Override
    public OpenCGAResult rank(Query query, String field, int numResults, boolean asc)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return rank(individualCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public OpenCGAResult groupBy(Query query, String field, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(individualCollection, bsonQuery, field, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(individualCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public OpenCGAResult groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException {
        Bson bsonQuery = parseQuery(query, user);
        return groupBy(individualCollection, bsonQuery, fields, QueryParams.ID.key(), options);
    }

    @Override
    public OpenCGAResult distinct(long studyUid, String field, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query finalQuery = query != null ? new Query(query) : new Query();
        finalQuery.put(QueryParams.STUDY_UID.key(), studyUid);
        Bson bson = parseQuery(finalQuery, userId);

        return new OpenCGAResult<>(individualCollection.distinct(field, bson));
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
            results.addAll(individualCollection.distinct(field, bson, String.class).getResults());
        }

        return new OpenCGAResult<>((int) stopWatch.getTime(TimeUnit.MILLISECONDS), Collections.emptyList(), results.size(),
                new ArrayList<>(results), -1);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Objects.requireNonNull(action);
        try (DBIterator<Individual> catalogDBIterator = iterator(query, options)) {
            while (catalogDBIterator.hasNext()) {
                action.accept(catalogDBIterator.next());
            }
        }
    }

    private Bson parseQuery(Query query) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
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

        if (query.containsKey(QueryParams.STUDY_UID.key())
                && (StringUtils.isNotEmpty(user) || query.containsKey(ParamConstants.ACL_PARAM))) {
            Document studyDocument = getStudyDocument(null, query.getLong(QueryParams.STUDY_UID.key()));

            if (query.containsKey(ParamConstants.ACL_PARAM)) {
                andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.INDIVIDUAL, user,
                        configuration));
            } else {
                if (containsAnnotationQuery(query)) {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user,
                            IndividualPermissions.VIEW_ANNOTATIONS.name(), Enums.Resource.INDIVIDUAL, configuration));
                } else {
                    andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, IndividualPermissions.VIEW.name(),
                            Enums.Resource.INDIVIDUAL, configuration));
                }
            }

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        if ("all".equalsIgnoreCase(queryCopy.getString(IndividualDBAdaptor.QueryParams.VERSION.key()))) {
            queryCopy.put(Constants.ALL_VERSIONS, true);
            queryCopy.remove(IndividualDBAdaptor.QueryParams.VERSION.key());
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
                    case PHENOTYPES:
                    case DISORDERS:
                        addDefaultOrQueryFilter(queryParam.key(), queryParam.key(), queryCopy, andBsonList);
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
                    case CREATION_DATE:
                        addAutoOrQuery(PRIVATE_CREATION_DATE, queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case MODIFICATION_DATE:
                        addAutoOrQuery(PRIVATE_MODIFICATION_DATE, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STATUS:
                    case STATUS_ID:
                        addAutoOrQuery(QueryParams.STATUS_ID.key(), queryParam.key(), query, QueryParams.STATUS_ID.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_ID:
                        // Convert the status to a positive status
                        query.put(queryParam.key(), InternalStatus.getPositiveStatus(InternalStatus.STATUS_LIST,
                                query.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_ID.key(), queryParam.key(), query,
                                QueryParams.INTERNAL_STATUS_ID.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case NAME:
                    case FATHER_UID:
                    case MOTHER_UID:
                    case FAMILY_IDS:
                    case DATE_OF_BIRTH:
                    case SEX_ID:
                    case ETHNICITY_ID:
                    case POPULATION_NAME:
                    case POPULATION_SUBPOPULATION:
                    case KARYOTYPIC_SEX:
                    case LIFE_STATUS:
                    case RELEASE:
                    case VERSION:
                    case SAMPLE_UIDS:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                    case DISORDERS_ID:
                    case DISORDERS_NAME:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    default:
                        throw new CatalogDBException("Cannot query by parameter " + queryParam.key());
                }
            } catch (Exception e) {
                throw new CatalogDBException(e);
            }
        }

        // If the user doesn't look for a concrete version...
        if (!uidVersionQueryFlag && !queryCopy.getBoolean(Constants.ALL_VERSIONS) && !queryCopy.containsKey(QueryParams.VERSION.key())
                && queryCopy.containsKey(QueryParams.SNAPSHOT.key())) {
            // If the user looks for anything from some release, we will try to find the latest from the release (snapshot)
            andBsonList.add(Filters.eq(LAST_OF_RELEASE, true));
        }

        if (annotationDocument != null && !annotationDocument.isEmpty()) {
            andBsonList.add(annotationDocument);
        }
        if (extraQuery != null && extraQuery.size() > 0) {
            andBsonList.add(extraQuery);
        }
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    public List<Individual> calculateRelationship(long studyUid, Individual proband, int maxDegree, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        return calculateRelationship(null, studyUid, proband, maxDegree, userId);
    }

    // Calculate roles
    List<Individual> calculateRelationship(ClientSession clientSession, long studyUid, Individual proband, int maxDegree, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        QueryOptions options = fixOptionsForRelatives(new QueryOptions(QueryOptions.INCLUDE, IndividualDBAdaptor.QueryParams.ID.key()));

        List<Individual> individualList = new LinkedList<>();
        individualList.add(proband);

        EnumMap<Family.FamiliarRelationship, Family.FamiliarRelationship> relationMap = new EnumMap<>(Family.FamiliarRelationship.class);
        // ------------------ Processing degree 1
        relationMap.put(Family.FamiliarRelationship.MOTHER, Family.FamiliarRelationship.MOTHER);
        relationMap.put(Family.FamiliarRelationship.FATHER, Family.FamiliarRelationship.FATHER);
        relationMap.put(Family.FamiliarRelationship.SON, Family.FamiliarRelationship.SON);
        relationMap.put(Family.FamiliarRelationship.DAUGHTER, Family.FamiliarRelationship.DAUGHTER);
        relationMap.put(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX,
                Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX);
        Map<Family.FamiliarRelationship, List<Individual>> relativeMap = lookForParentsAndChildren(clientSession, studyUid, proband,
                new HashSet<>(), options, userId);
        addDegreeRelatives(relativeMap, relationMap, 1, individualList);

        if (maxDegree == 1) {
            // Remove proband from list
            individualList.remove(0);
            return individualList;
        }

        Individual mother = relativeMap.containsKey(Family.FamiliarRelationship.MOTHER)
                ? relativeMap.get(Family.FamiliarRelationship.MOTHER).get(0) : null;
        Individual father = relativeMap.containsKey(Family.FamiliarRelationship.FATHER)
                ? relativeMap.get(Family.FamiliarRelationship.FATHER).get(0) : null;
        List<Individual> children = new LinkedList<>();
        if (relativeMap.containsKey(Family.FamiliarRelationship.SON)) {
            children.addAll(relativeMap.get(Family.FamiliarRelationship.SON));
        }
        if (relativeMap.containsKey(Family.FamiliarRelationship.DAUGHTER)) {
            children.addAll(relativeMap.get(Family.FamiliarRelationship.DAUGHTER));
        }
        if (relativeMap.containsKey(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX)) {
            children.addAll(relativeMap.get(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX));
        }

        // ------------------ Processing degree 2
        if (mother != null) {
            relationMap.put(Family.FamiliarRelationship.MOTHER, Family.FamiliarRelationship.MATERNAL_GRANDMOTHER);
            relationMap.put(Family.FamiliarRelationship.FATHER, Family.FamiliarRelationship.MATERNAL_GRANDFATHER);
            relationMap.put(Family.FamiliarRelationship.SON, Family.FamiliarRelationship.BROTHER);
            relationMap.put(Family.FamiliarRelationship.DAUGHTER, Family.FamiliarRelationship.SISTER);
            relationMap.put(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX,
                    Family.FamiliarRelationship.FULL_SIBLING);

            // Update set of already obtained individuals
            Set<String> skipIndividuals = individualList.stream().map(Individual::getId).collect(Collectors.toSet());
            relativeMap = lookForParentsAndChildren(clientSession, studyUid, mother, skipIndividuals, options, userId);
            addDegreeRelatives(relativeMap, relationMap, 2, individualList);
        }
        if (father != null) {
            relationMap.put(Family.FamiliarRelationship.MOTHER, Family.FamiliarRelationship.PATERNAL_GRANDMOTHER);
            relationMap.put(Family.FamiliarRelationship.FATHER, Family.FamiliarRelationship.PATERNAL_GRANDFATHER);
            relationMap.put(Family.FamiliarRelationship.SON, Family.FamiliarRelationship.BROTHER);
            relationMap.put(Family.FamiliarRelationship.DAUGHTER, Family.FamiliarRelationship.SISTER);
            relationMap.put(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX,
                    Family.FamiliarRelationship.FULL_SIBLING);

            // Update set of already obtained individuals
            Set<String> skipIndividuals = individualList.stream().map(Individual::getId).collect(Collectors.toSet());
            relativeMap = lookForParentsAndChildren(clientSession, studyUid, father, skipIndividuals, options, userId);
            addDegreeRelatives(relativeMap, relationMap, 2, individualList);
        }

        // Update set of already obtained individuals
        Set<String> skipIndividuals = individualList.stream().map(Individual::getId).collect(Collectors.toSet());
        for (Individual child : children) {
            relationMap.put(Family.FamiliarRelationship.SON, Family.FamiliarRelationship.GRANDSON);
            relationMap.put(Family.FamiliarRelationship.DAUGHTER, Family.FamiliarRelationship.GRANDDAUGHTER);
            relationMap.put(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX, Family.FamiliarRelationship.GRANDCHILD);

            relativeMap = lookForChildren(clientSession, studyUid, child, skipIndividuals, options, userId);
            addDegreeRelatives(relativeMap, relationMap, 2, individualList);
        }

        // Remove proband from list
        individualList.remove(0);
        return individualList;
    }

    private Map<Family.FamiliarRelationship, List<Individual>> lookForParentsAndChildren(ClientSession clientSession, long studyUid,
                                                                                         Individual proband, Set<String> skipIndividuals,
                                                                                         QueryOptions options, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        EnumMap<Family.FamiliarRelationship, List<Individual>> finalResult = new EnumMap<>(Family.FamiliarRelationship.class);

        finalResult.putAll(lookForParents(clientSession, studyUid, proband, skipIndividuals, options, userId));
        finalResult.putAll(lookForChildren(clientSession, studyUid, proband, skipIndividuals, options, userId));

        return finalResult;
    }

    private Map<Family.FamiliarRelationship, List<Individual>> lookForParents(ClientSession clientSession, long studyUid,
                                                                              Individual proband, Set<String> skipIndividuals,
                                                                              QueryOptions options, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        EnumMap<Family.FamiliarRelationship, List<Individual>> finalResult = new EnumMap<>(Family.FamiliarRelationship.class);

        // Looking for father
        if (proband.getFather() != null && proband.getFather().getUid() > 0) {
            Query query = new Query(IndividualDBAdaptor.QueryParams.UID.key(), proband.getFather().getUid());
            OpenCGAResult<Individual> result = get(clientSession, studyUid, query, options, userId);
            if (result.getNumResults() == 1 && !skipIndividuals.contains(result.first().getId())) {
                finalResult.put(Family.FamiliarRelationship.FATHER, Collections.singletonList(result.first()));
            }
        }
        // Looking for mother
        if (proband.getMother() != null && proband.getMother().getUid() > 0) {
            Query query = new Query(IndividualDBAdaptor.QueryParams.UID.key(), proband.getMother().getUid());
            OpenCGAResult<Individual> result = get(clientSession, studyUid, query, options, userId);
            if (result.getNumResults() == 1 && !skipIndividuals.contains(result.first().getId())) {
                finalResult.put(Family.FamiliarRelationship.MOTHER, Collections.singletonList(result.first()));
            }
        }
        return finalResult;
    }

    private Map<Family.FamiliarRelationship, List<Individual>> lookForChildren(ClientSession clientSession, long studyUid,
                                                                               Individual proband, Set<String> skipIndividuals,
                                                                               QueryOptions options, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        EnumMap<Family.FamiliarRelationship, List<Individual>> finalResult = new EnumMap<>(Family.FamiliarRelationship.class);

        // Looking for children
        Query query = new Query();
        if (proband.getSex().getSex() == IndividualProperty.Sex.MALE) {
            query.put(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), proband.getUid());
        } else if (proband.getSex().getSex() == IndividualProperty.Sex.FEMALE) {
            query.put(IndividualDBAdaptor.QueryParams.MOTHER_UID.key(), proband.getUid());
        }
        if (!query.isEmpty()) {
            // Sex has been defined
            OpenCGAResult<Individual> result = get(clientSession, studyUid, query, options, userId);
            for (Individual child : result.getResults()) {
                if (skipIndividuals.contains(child.getId())) {
                    // Skip current individuals
                    continue;
                }
                Family.FamiliarRelationship sex = getChildSex(child);
                finalResult.putIfAbsent(sex, new LinkedList<>());
                finalResult.get(sex).add(child);
            }
        } else {
            // Sex is undefined so we will check with both sexes
            query.put(IndividualDBAdaptor.QueryParams.FATHER_UID.key(), proband.getUid());
            OpenCGAResult<Individual> result = get(clientSession, studyUid, query, options, userId);
            if (result.getNumResults() > 0) {
                for (Individual child : result.getResults()) {
                    if (skipIndividuals.contains(child.getId())) {
                        // Skip current individuals
                        continue;
                    }
                    Family.FamiliarRelationship sex = getChildSex(child);
                    finalResult.putIfAbsent(sex, new LinkedList<>());
                    finalResult.get(sex).add(child);
                }
            } else {
                query = new Query(IndividualDBAdaptor.QueryParams.MOTHER_UID.key(), proband.getUid());
                result = get(clientSession, studyUid, query, options, userId);
                for (Individual child : result.getResults()) {
                    if (skipIndividuals.contains(child.getId())) {
                        // Skip current individuals
                        continue;
                    }
                    Family.FamiliarRelationship sex = getChildSex(child);
                    finalResult.putIfAbsent(sex, new LinkedList<>());
                    finalResult.get(sex).add(child);
                }
            }
        }

        return finalResult;
    }

    private Family.FamiliarRelationship getChildSex(Individual individual) {
        if (individual.getSex().getSex() == IndividualProperty.Sex.MALE) {
            return Family.FamiliarRelationship.SON;
        } else if (individual.getSex().getSex() == IndividualProperty.Sex.FEMALE) {
            return Family.FamiliarRelationship.DAUGHTER;
        } else {
            return Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX;
        }
    }

    void addDegreeRelatives(Map<Family.FamiliarRelationship, List<Individual>> relativeMap,
                            Map<Family.FamiliarRelationship, Family.FamiliarRelationship> relationMap, int degree,
                            List<Individual> individualList) {
        for (Map.Entry<Family.FamiliarRelationship, List<Individual>> entry : relativeMap.entrySet()) {
            switch (entry.getKey()) {
                case MOTHER:
                    if (relationMap.containsKey(Family.FamiliarRelationship.MOTHER)) {
                        addRelativeToList(entry.getValue().get(0), relationMap.get(Family.FamiliarRelationship.MOTHER), degree,
                                individualList);
                    }
                    break;
                case FATHER:
                    if (relationMap.containsKey(Family.FamiliarRelationship.FATHER)) {
                        addRelativeToList(entry.getValue().get(0), relationMap.get(Family.FamiliarRelationship.FATHER), degree,
                                individualList);
                    }
                    break;
                case SON:
                    if (relationMap.containsKey(Family.FamiliarRelationship.SON)) {
                        for (Individual child : entry.getValue()) {
                            addRelativeToList(child, relationMap.get(Family.FamiliarRelationship.SON), degree, individualList);
                        }
                    }
                    break;
                case DAUGHTER:
                    if (relationMap.containsKey(Family.FamiliarRelationship.DAUGHTER)) {
                        for (Individual child : entry.getValue()) {
                            addRelativeToList(child, relationMap.get(Family.FamiliarRelationship.DAUGHTER), degree,
                                    individualList);
                        }
                    }
                    break;
                case CHILD_OF_UNKNOWN_SEX:
                    if (relationMap.containsKey(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX)) {
                        for (Individual child : entry.getValue()) {
                            addRelativeToList(child, relationMap.get(Family.FamiliarRelationship.CHILD_OF_UNKNOWN_SEX), degree,
                                    individualList);
                        }
                    }
                    break;
                default:
                    logger.warn("Unexpected relation found: {}", entry.getKey());
                    break;
            }
        }
    }

    void removeSampleReferences(ClientSession clientSession, long studyUid, long sampleUid)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(QueryParams.SAMPLE_UIDS.key(), sampleUid);
        ObjectMap params = new ObjectMap()
                .append(QueryParams.SAMPLES.key(), Collections.singletonList(new Sample().setUid(sampleUid)));
        // Add the the Remove action for the sample provided
        QueryOptions queryOptions = new QueryOptions(Constants.ACTIONS,
                new ObjectMap(QueryParams.SAMPLES.key(), ParamUtils.BasicUpdateAction.REMOVE.name()));

        Bson update;
        try {
            update = parseAndValidateUpdateParams(clientSession, params, query, queryOptions).toFinalUpdateDocument();
        } catch (CatalogDBException e) {
            if (e.getMessage().contains("No individual found to be updated")) {
                return;
            } else {
                throw e;
            }
        }

        Bson bsonQuery = parseQuery(query);
        versionedMongoDBAdaptor.update(clientSession, bsonQuery, () -> {
                    QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
                    logger.debug("Sample references extraction. Query: {}, update: {}", bsonQuery.toBsonDocument(),
                            update.toBsonDocument());
                    DataResult updateResult = individualCollection.update(clientSession, bsonQuery, update, multi);
                    logger.debug("Sample uid '" + sampleUid + "' references removed from " + updateResult.getNumUpdated() + " out of "
                            + updateResult.getNumMatches() + " individuals");
                    return null;
                }, Collections.singletonList(QueryParams.SAMPLES_IDS.key()), this::iterator,
                (DBIterator<Individual> iterator) -> updateReferencesAfterIndividualVersionIncrement(clientSession, studyUid, iterator));
    }

    public MongoDBCollection getIndividualCollection() {
        return individualCollection;
    }

    public MongoDBCollection getIndividualArchiveCollection() {
        return archiveIndividualCollection;
    }

}
