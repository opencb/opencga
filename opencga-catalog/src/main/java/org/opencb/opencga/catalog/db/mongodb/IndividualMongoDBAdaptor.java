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

import com.mongodb.MongoClient;
import com.mongodb.client.ClientSession;
import com.mongodb.client.model.Filters;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.biodata.models.clinical.Disorder;
import org.opencb.biodata.models.clinical.Phenotype;
import org.opencb.biodata.models.pedigree.IndividualProperty;
import org.opencb.commons.datastore.core.*;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.commons.datastore.mongodb.MongoDBIterator;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.AnnotableConverter;
import org.opencb.opencga.catalog.db.mongodb.converters.IndividualConverter;
import org.opencb.opencga.catalog.db.mongodb.iterators.IndividualCatalogMongoDBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.catalog.managers.ClinicalAnalysisManager;
import org.opencb.opencga.catalog.managers.FamilyManager;
import org.opencb.opencga.catalog.utils.Constants;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UuidUtils;
import org.opencb.opencga.core.api.ParamConstants;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.models.clinical.ClinicalAnalysis;
import org.opencb.opencga.core.models.common.Annotable;
import org.opencb.opencga.core.models.common.AnnotationSet;
import org.opencb.opencga.core.models.common.Enums;
import org.opencb.opencga.core.models.common.Status;
import org.opencb.opencga.core.models.family.Family;
import org.opencb.opencga.core.models.individual.Individual;
import org.opencb.opencga.core.models.individual.IndividualAclEntry;
import org.opencb.opencga.core.models.sample.Sample;
import org.opencb.opencga.core.models.study.StudyAclEntry;
import org.opencb.opencga.core.models.study.VariableSet;
import org.opencb.opencga.core.response.OpenCGAResult;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.filterAnnotationSets;
import static org.opencb.opencga.catalog.db.mongodb.AuthorizationMongoDBUtils.getQueryForAuthorisedEntries;
import static org.opencb.opencga.catalog.db.mongodb.MongoDBUtils.*;

/**
 * Created by hpccoll1 on 19/06/15.
 */
public class IndividualMongoDBAdaptor extends AnnotationMongoDBAdaptor<Individual> implements IndividualDBAdaptor {

    private final MongoDBCollection individualCollection;
    private final MongoDBCollection deletedIndividualCollection;
    private IndividualConverter individualConverter;

    public IndividualMongoDBAdaptor(MongoDBCollection individualCollection, MongoDBCollection deletedIndividualCollection,
                                    MongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(IndividualMongoDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.individualCollection = individualCollection;
        this.deletedIndividualCollection = deletedIndividualCollection;
        this.individualConverter = new IndividualConverter();
    }

    @Override
    protected AnnotableConverter<? extends Annotable> getConverter() {
        return individualConverter;
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

        long individualUid = getNewUid(clientSession);

        individual.setUid(individualUid);
        individual.setStudyUid(studyId);
        individual.setVersion(1);
        if (StringUtils.isEmpty(individual.getUuid())) {
            individual.setUuid(UuidUtils.generateOpenCgaUuid(UuidUtils.Entity.INDIVIDUAL));
        }
        if (StringUtils.isEmpty(individual.getCreationDate())) {
            individual.setCreationDate(TimeUtils.getTime());
        }

        Document individualDocument = individualConverter.convertToStorageType(individual, variableSetList);

        // Versioning private parameters
        individualDocument.put(RELEASE_FROM_VERSION, Arrays.asList(individual.getRelease()));
        individualDocument.put(LAST_OF_VERSION, true);
        individualDocument.put(LAST_OF_RELEASE, true);
        individualDocument.put(PRIVATE_CREATION_DATE, TimeUtils.toDate(individual.getCreationDate()));
        individualDocument.put(PRIVATE_MODIFICATION_DATE, individualDocument.get(PRIVATE_CREATION_DATE));
        individualDocument.put(PERMISSION_RULES_APPLIED, Collections.emptyList());

        logger.debug("Inserting individual '{}' ({})...", individual.getId(), individual.getUid());
        individualCollection.insert(clientSession, individualDocument, null);
        logger.debug("Individual '{}' successfully inserted", individual.getId());

        if (individual.getSamples() != null && !individual.getSamples().isEmpty()) {
            for (Sample sample : individual.getSamples()) {
                // We associate the samples to the individual just created
                updateIndividualFromSampleCollection(clientSession, studyId, sample.getUid(), individual.getId());
            }
        }

        return individual;
    }

    private void updateIndividualFromSampleCollection(ClientSession clientSession, long studyId, long sampleUid, String individualId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        SampleMongoDBAdaptor sampleDBAdaptor = dbAdaptorFactory.getCatalogSampleDBAdaptor();

        ObjectMap params = new ObjectMap(SampleDBAdaptor.QueryParams.INDIVIDUAL_ID.key(), individualId);
        Document update = sampleDBAdaptor.parseAndValidateUpdateParams(clientSession, params, null).toFinalUpdateDocument();
        Bson query = sampleDBAdaptor.parseQuery(new Query()
                .append(SampleDBAdaptor.QueryParams.STUDY_UID.key(), studyId)
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleUid), null);

        sampleDBAdaptor.getCollection().update(clientSession, query, update, null);
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

        Document update = new Document()
                .append("$addToSet", new Document(RELEASE_FROM_VERSION, release));

        QueryOptions queryOptions = new QueryOptions("multi", true);

        return new OpenCGAResult(individualCollection.update(bson, update, queryOptions));
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
        logger.debug("Individual count: query : {}, dbTime: {}", bson.toBsonDocument(Document.class,
                MongoClient.getDefaultCodecRegistry()));
        return new OpenCGAResult<>(individualCollection.count(clientSession, bson));
    }

    @Override
    public OpenCGAResult distinct(Query query, String field)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Bson bson = parseQuery(query);
        return new OpenCGAResult(individualCollection.distinct(field, bson));
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

        // TODO: This shouldn't be necessary now
        if (queryOptions.getBoolean(Constants.REFRESH)) {
            // Add the latest sample versions in the parameters object
            updateToLastSampleVersions(clientSession, tmpQuery, parameters, queryOptions);
        }

        if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            createNewVersion(clientSession, individual.getStudyUid(), individual.getUid());
        } else {
            checkInUseInLockedClinicalAnalysis(clientSession, individual);
        }

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

            logger.debug("Individual update: query : {}, update: {}",
                    finalQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    individualUpdate.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));

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

                for (long sampleUid : addedSamples) {
                    // Set new individual reference
                    updateIndividualFromSampleCollection(clientSession, individual.getStudyUid(), sampleUid, individual.getId());
                }

                for (long sampleUid : removedSamples) {
                    // Set individual reference to ""
                    updateIndividualFromSampleCollection(clientSession, individual.getStudyUid(), sampleUid, "");
                }
            }

            // If the list of disorders or phenotypes is altered, we will need to update the corresponding effective lists
            // of the families associated (if any)
            if (parameters.containsKey(QueryParams.DISORDERS.key()) || parameters.containsKey(QueryParams.PHENOTYPES.key())) {
                // We fetch the current updated individual to know how the current list of disorders and phenotypes
                QueryOptions individualOptions = new QueryOptions(QueryOptions.INCLUDE,
                        Arrays.asList(QueryParams.PHENOTYPES.key(), QueryParams.DISORDERS.key()));
                Individual currentIndividual = get(clientSession, new Query()
                        .append(QueryParams.STUDY_UID.key(), individual.getStudyUid())
                        .append(QueryParams.UID.key(), individual.getUid()), individualOptions).first();

                FamilyMongoDBAdaptor familyDBAdaptor = dbAdaptorFactory.getCatalogFamilyDBAdaptor();

                Query familyQuery = new Query()
                        .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), individual.getUid())
                        .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), individual.getStudyUid());
                QueryOptions familyOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                        FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.STUDY_UID.key(),
                        FamilyDBAdaptor.QueryParams.MEMBERS.key()));

                DBIterator<Family> familyIterator = familyDBAdaptor.iterator(clientSession, familyQuery, familyOptions);

                ObjectMap actionMap = new ObjectMap()
                        .append(FamilyDBAdaptor.QueryParams.PHENOTYPES.key(), ParamUtils.UpdateAction.SET)
                        .append(FamilyDBAdaptor.QueryParams.DISORDERS.key(), ParamUtils.UpdateAction.SET);
                QueryOptions familyUpdateOptions = new QueryOptions(Constants.ACTIONS, actionMap);

                while (familyIterator.hasNext()) {
                    Family family = familyIterator.next();

                    ObjectMap params = getNewFamilyDisordersAndPhenotypesToUpdate(family, currentIndividual.getDisorders(),
                            currentIndividual.getPhenotypes(), currentIndividual.getUid());

                    familyDBAdaptor.privateUpdate(clientSession, family, params, null, familyUpdateOptions);

//                    Bson bsonQuery = familyDBAdaptor.parseQuery(new Query(FamilyDBAdaptor.QueryParams.UID.key(), family.getUid()));
//                    Document update = familyDBAdaptor.parseAndValidateUpdateParams(clientSession, params, null)
//                            .toFinalUpdateDocument();
//
//                    familyDBAdaptor.getFamilyCollection().update(clientSession, bsonQuery, update, QueryOptions.empty());
                }
            }
            logger.debug("Individual {} successfully updated", individual.getId());
        }

        if (queryOptions.getBoolean(Constants.INCREMENT_VERSION)) {
            updateFamilyIndividualReferences(clientSession, individual);
            updateClinicalAnalysisIndividualReferences(clientSession, individual);
        }

        return endWrite(tmpStartTime, 1, 1, events);
    }

    /**
     * Calculates the new list of disorders and phenotypes considering Individual individual belongs to Family family and that individual
     * has a new list of disorders and phenotypes stored.
     *
     * @param family Current Family as stored in DB.
     * @param disorders List of disorders of member of the family individualUid .
     * @param phenotypes List of phenotypes of member of the family individualUid.
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
     * @param individual Individual object containing the version stored in the Clinical Analysis (before the version increment).
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
                    && clinicalAnalysis.getProband().getVersion() == individual.getVersion()) {

                Individual proband = clinicalAnalysis.getProband();
                // Increase proband version
                proband.setVersion(proband.getVersion() + 1);

                ObjectMap params = new ObjectMap(ClinicalAnalysisDBAdaptor.QueryParams.PROBAND.key(), proband);

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
     * Update Individual references from any Family where it was used.
     *
     * @param clientSession Client session.
     * @param individual Individual object containing the previous version (before the version increment).
     */
    private void updateFamilyIndividualReferences(ClientSession clientSession, Individual individual)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), individual.getStudyUid())
                .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), individual.getUid());

        List<String> include = new ArrayList<>(FamilyManager.INCLUDE_FAMILY_IDS.getAsStringList(QueryOptions.INCLUDE));
        include.add(FamilyDBAdaptor.QueryParams.MEMBERS.key());
        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, include);

        DBIterator<Family> iterator = dbAdaptorFactory.getCatalogFamilyDBAdaptor().iterator(clientSession, query, options);

        while (iterator.hasNext()) {
            Family family = iterator.next();

            List<Individual> members = new ArrayList<>(family.getMembers().size());
            for (Individual member : family.getMembers()) {
                if (member.getUid() == individual.getUid()) {
                    member.setVersion(individual.getVersion() + 1);
                }
                members.add(member);
            }

            ObjectMap params = new ObjectMap(FamilyDBAdaptor.QueryParams.MEMBERS.key(), members);

            ObjectMap action = new ObjectMap(FamilyDBAdaptor.QueryParams.MEMBERS.key(), ParamUtils.UpdateAction.SET);
            options = new QueryOptions()
                    .append(Constants.INCREMENT_VERSION, true)
                    .append(Constants.ACTIONS, action);

            OpenCGAResult result = dbAdaptorFactory.getCatalogFamilyDBAdaptor().privateUpdate(clientSession, family, params, null, options);
            if (result.getNumUpdated() != 1) {
                throw new CatalogDBException("Family '" + family.getId() + "' could not be updated to the latest "
                        + "member version of '" + individual.getId() + "'");
            }
        }
    }

    /**
     * Checks whether the individual that is going to be updated is in use in any locked Clinical Analysis.
     *
     * @param clientSession Client session.
     * @param individual Individual to be updated.
     * @throws CatalogDBException CatalogDBException if the individual is in use in any Clinical Analysis.
     */
    private void checkInUseInLockedClinicalAnalysis(ClientSession clientSession, Individual individual)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {

        // We only need to focus on locked clinical analyses
        Query query = new Query()
                .append(ClinicalAnalysisDBAdaptor.QueryParams.STUDY_UID.key(), individual.getStudyUid())
                .append(ClinicalAnalysisDBAdaptor.QueryParams.INDIVIDUAL.key(), individual.getUid())
                .append(ClinicalAnalysisDBAdaptor.QueryParams.LOCKED.key(), true);

        OpenCGAResult<ClinicalAnalysis> result = dbAdaptorFactory.getClinicalAnalysisDBAdaptor().get(clientSession, query,
                ClinicalAnalysisManager.INCLUDE_CATALOG_DATA);

        if (result.getNumResults() == 0) {
            // No Clinical Analyses are using the member...
            return;
        }

        // We need to check if the member version is being used in any of the clinical analyses manually
        Set<String> clinicalAnalysisIds = new HashSet<>(result.getNumResults());
        for (ClinicalAnalysis clinicalAnalysis : result.getResults()) {
            if (clinicalAnalysis.getProband() != null && clinicalAnalysis.getProband().getUid() == individual.getUid()
                    && clinicalAnalysis.getProband().getVersion() == individual.getVersion()) {
                clinicalAnalysisIds.add(clinicalAnalysis.getId());
                continue;
            }
            if (clinicalAnalysis.getFamily() != null && clinicalAnalysis.getFamily().getMembers() != null) {
                for (Individual member : clinicalAnalysis.getFamily().getMembers()) {
                    if (member.getUid() == individual.getUid() && member.getVersion() == individual.getVersion()) {
                        clinicalAnalysisIds.add(clinicalAnalysis.getId());
                    }
                }
            }
        }

        if (!clinicalAnalysisIds.isEmpty()) {
            throw new CatalogDBException("Individual '" + individual.getId() + "' is being used in the following clinical analyses: '"
                    + String.join("', '", clinicalAnalysisIds) + "'.");
        }
    }

    private void createNewVersion(ClientSession clientSession, long studyUid, long individualUid)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query query = new Query()
                .append(QueryParams.UID.key(), individualUid)
                .append(QueryParams.STUDY_UID.key(), studyUid);
        OpenCGAResult<Document> queryResult = nativeGet(query, new QueryOptions(QueryOptions.EXCLUDE, "_id"));

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Could not find individual '" + individualUid + "'");
        }

        createNewVersion(clientSession, individualCollection, queryResult.first());
    }

    private void updateToLastSampleVersions(ClientSession clientSession, Query query, ObjectMap parameters, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        if (parameters.containsKey(QueryParams.SAMPLES.key())) {
            throw new CatalogDBException("Invalid option: Cannot update to the last version of samples and update to different samples at "
                    + "the same time.");
        }

        QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.SAMPLES.key());
        OpenCGAResult<Individual> queryResult = get(query, options);

        if (queryResult.getNumResults() == 0) {
            throw new CatalogDBException("Individual not found.");
        }
        if (queryResult.getNumResults() > 1) {
            throw new CatalogDBException("Update to the last version of samples in multiple individuals at once not supported.");
        }

        Individual individual = queryResult.first();
        if (individual.getSamples() == null || individual.getSamples().isEmpty()) {
            // Nothing to do
            return;
        }

        List<Long> sampleIds = individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toList());
        Query sampleQuery = new Query()
                .append(SampleDBAdaptor.QueryParams.UID.key(), sampleIds);
        options = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                SampleDBAdaptor.QueryParams.UID.key(), SampleDBAdaptor.QueryParams.VERSION.key()
        ));
        OpenCGAResult<Sample> sampleDataResult = dbAdaptorFactory.getCatalogSampleDBAdaptor().get(clientSession, sampleQuery, options);
        parameters.put(QueryParams.SAMPLES.key(), sampleDataResult.getResults());

        // Add SET action for samples
        queryOptions.putIfAbsent(Constants.ACTIONS, new HashMap<>());
        queryOptions.getMap(Constants.ACTIONS).put(QueryParams.SAMPLES.key(), SET);
    }

    UpdateDocument parseAndValidateUpdateParams(ClientSession clientSession, ObjectMap parameters, Query query, QueryOptions queryOptions)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        UpdateDocument document = new UpdateDocument();

        if (parameters.containsKey(QueryParams.ID.key())) {
            // That can only be done to one individual...
            Individual individual = checkOnlyOneIndividualMatches(clientSession, query);

            // Check that the new individual name is still unique
            long studyId = getStudyId(individual.getUid());

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

        String[] acceptedParams = {QueryParams.NAME.key(), QueryParams.ETHNICITY.key(), QueryParams.SEX.key(),
                QueryParams.POPULATION_NAME.key(), QueryParams.POPULATION_SUBPOPULATION.key(), QueryParams.POPULATION_DESCRIPTION.key(),
                QueryParams.KARYOTYPIC_SEX.key(), QueryParams.LIFE_STATUS.key(), QueryParams.DATE_OF_BIRTH.key(), };
        filterStringParams(parameters, document.getSet(), acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = Collections.singletonMap((QueryParams.SEX.key()), IndividualProperty.Sex.class);
        filterEnumParams(parameters, document.getSet(), acceptedEnums);

        String[] acceptedIntParams = {QueryParams.FATHER_UID.key(), QueryParams.MOTHER_UID.key()};
        filterLongParams(parameters, document.getSet(), acceptedIntParams);

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key()};
        filterMapParams(parameters, document.getSet(), acceptedMapParams);

        String[] acceptedObjectParams = {QueryParams.PHENOTYPES.key(), QueryParams.DISORDERS.key(),
                QueryParams.LOCATION.key(), QueryParams.STATUS.key(), QueryParams.QUALITY_CONTROL.key()};
        filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
        if (document.getSet().containsKey(QueryParams.STATUS.key())) {
            nestedPut(QueryParams.STATUS_DATE.key(), TimeUtils.getTime(), document.getSet());
        }

        if (parameters.containsKey(QueryParams.INTERNAL_STATUS_NAME.key())) {
            document.getSet().put(QueryParams.INTERNAL_STATUS_NAME.key(), parameters.get(QueryParams.INTERNAL_STATUS_NAME.key()));
            document.getSet().put(QueryParams.INTERNAL_STATUS_DATE.key(), TimeUtils.getTime());
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
            // That can only be done to one individual...
            Individual individual = checkOnlyOneIndividualMatches(clientSession, query);

            Map<String, Object> actionMap = queryOptions.getMap(Constants.ACTIONS, new HashMap<>());
            ParamUtils.UpdateAction operation = ParamUtils.UpdateAction.from(actionMap, QueryParams.SAMPLES.key(),
                    ParamUtils.UpdateAction.ADD);
            getSampleChanges(individual, parameters, document, operation);

            acceptedObjectParams = new String[]{QueryParams.SAMPLES.key()};
            switch (operation) {
                case SET:
                    filterObjectParams(parameters, document.getSet(), acceptedObjectParams);
                    individualConverter.validateSamplesToUpdate(document.getSet());
                    break;
                case REMOVE:
                    filterObjectParams(parameters, document.getPullAll(), acceptedObjectParams);
                    individualConverter.validateSamplesToUpdate(document.getPullAll());
                    break;
                case ADD:
                    filterObjectParams(parameters, document.getAddToSet(), acceptedObjectParams);
                    individualConverter.validateSamplesToUpdate(document.getAddToSet());
                    break;
                default:
                    throw new IllegalStateException("Unknown operation " + operation);
            }
        }

        if (!document.toFinalUpdateDocument().isEmpty()) {
            // Update modificationDate param
            String time = TimeUtils.getTime();
            Date date = TimeUtils.toDate(time);
            document.getSet().put(QueryParams.MODIFICATION_DATE.key(), time);
            document.getSet().put(PRIVATE_MODIFICATION_DATE, date);
        }

        return document;
    }

    private void getSampleChanges(Individual individual, ObjectMap parameters, UpdateDocument updateDocument,
                                  ParamUtils.UpdateAction operation) {
        List<Sample> sampleList = parameters.getAsList(QueryParams.SAMPLES.key(), Sample.class);

        Set<Long> currentSampleUidList = new HashSet<>();
        if (individual.getSamples() != null) {
            currentSampleUidList = individual.getSamples().stream().map(Sample::getUid).collect(Collectors.toSet());
        }

        if (operation == ParamUtils.UpdateAction.SET || operation == ParamUtils.UpdateAction.ADD) {
            // We will see which of the samples are actually new
            List<Long> samplesToAdd = new ArrayList<>();

            for (Sample sample : sampleList) {
                if (!currentSampleUidList.contains(sample.getUid())) {
                    samplesToAdd.add(sample.getUid());
                }
            }

            if (!samplesToAdd.isEmpty()) {
                updateDocument.getAttributes().put("ADDED_SAMPLES", samplesToAdd);
            }

            if (operation == ParamUtils.UpdateAction.SET && individual.getSamples() != null) {
                // We also need to see which samples existed and are not currently in the new list provided by the user to take them out
                Set<Long> newSampleUids = sampleList.stream().map(Sample::getUid).collect(Collectors.toSet());

                List<Long> samplesToRemove = new ArrayList<>();
                for (Sample sample : individual.getSamples()) {
                    if (!newSampleUids.contains(sample.getUid())) {
                        samplesToRemove.add(sample.getUid());
                    }
                }

                if (!samplesToRemove.isEmpty()) {
                    updateDocument.getAttributes().put("REMOVED_SAMPLES", samplesToRemove);
                }
            }
        } else if (operation == ParamUtils.UpdateAction.REMOVE) {
            // We will only store the samples to be removed that are already associated to the individual
            List<Long> samplesToRemove = new ArrayList<>();

            for (Sample sample : sampleList) {
                if (currentSampleUidList.contains(sample.getUid())) {
                    samplesToRemove.add(sample.getUid());
                }
            }

            if (!samplesToRemove.isEmpty()) {
                updateDocument.getAttributes().put("REMOVED_SAMPLES", samplesToRemove);
            }
        }
    }

    private Individual checkOnlyOneIndividualMatches(ClientSession clientSession, Query query)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException {
        Query tmpQuery = new Query(query);
        // We take out ALL_VERSION from query just in case we get multiple results from the same individual...
        tmpQuery.remove(Constants.ALL_VERSIONS);

        OpenCGAResult<Individual> individualDataResult = get(clientSession, tmpQuery, new QueryOptions());
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
        FamilyMongoDBAdaptor familyDBAdaptor = dbAdaptorFactory.getCatalogFamilyDBAdaptor();

        String individualId = individualDocument.getString(QueryParams.ID.key());
        long individualUid = individualDocument.getLong(PRIVATE_UID);
        long studyUid = individualDocument.getLong(PRIVATE_STUDY_UID);

        long tmpStartTime = startQuery();
        logger.debug("Deleting individual {} ({})", individualId, individualUid);

        // Remove individual reference from the list of samples
        List<Document> sampleList = individualDocument.getList(QueryParams.SAMPLES.key(), Document.class);
        if (sampleList != null && !sampleList.isEmpty()) {
            for (Document sample : sampleList) {
                // We set the individual id for those samples to ""
                updateIndividualFromSampleCollection(clientSession, studyUid, sample.getLong(PRIVATE_UID), "");
            }
        }

        // Remove individual from any list of members it might be part of
        Query familyQuery = new Query()
                .append(FamilyDBAdaptor.QueryParams.MEMBER_UID.key(), individualUid)
                .append(FamilyDBAdaptor.QueryParams.STUDY_UID.key(), studyUid)
                .append(Constants.ALL_VERSIONS, true);

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE,
                Arrays.asList(FamilyDBAdaptor.QueryParams.UID.key(), FamilyDBAdaptor.QueryParams.VERSION.key(),
                        FamilyDBAdaptor.QueryParams.MEMBERS.key()));
        DBIterator<Family> familyIterator = familyDBAdaptor.iterator(familyQuery, queryOptions);

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

            logger.debug("Remove individual references from family: Query: {}, update: {}",
                    bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                    update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
            DataResult result = familyDBAdaptor.getFamilyCollection().update(clientSession, bsonQuery, update,
                    new QueryOptions(MongoDBCollection.MULTI, true));
            logger.debug("Families found: {}, families updated: {}", result.getNumMatches(), result.getNumUpdated());
        }

        // Look for all the different individual versions
        Query individualQuery = new Query()
                .append(QueryParams.UID.key(), individualUid)
                .append(QueryParams.STUDY_UID.key(), studyUid)
                .append(Constants.ALL_VERSIONS, true);
        DBIterator<Document> individualDBIterator = nativeIterator(individualQuery, QueryOptions.empty());

        // Delete any documents that might have been already deleted with that id
        Bson query = new Document()
                .append(QueryParams.ID.key(), individualId)
                .append(PRIVATE_STUDY_UID, studyUid);
        deletedIndividualCollection.remove(clientSession, query, new QueryOptions(MongoDBCollection.MULTI, true));

        while (individualDBIterator.hasNext()) {
            Document tmpIndividual = individualDBIterator.next();

            // Set status to DELETED
            nestedPut(QueryParams.INTERNAL_STATUS.key(), getMongoDBDocument(new Status(Status.DELETED), "status"), tmpIndividual);

            int individualVersion = tmpIndividual.getInteger(QueryParams.VERSION.key());

            // Insert the document in the DELETE collection
            deletedIndividualCollection.insert(clientSession, tmpIndividual, null);
            logger.debug("Inserted individual uid '{}' version '{}' in DELETE collection", individualUid, individualVersion);

            // Remove the document from the main INDIVIDUAL collection
            query = parseQuery(new Query()
                    .append(QueryParams.UID.key(), individualUid)
                    .append(QueryParams.VERSION.key(), individualVersion));
            DataResult remove = individualCollection.remove(clientSession, query, null);
            if (remove.getNumMatches() == 0) {
                throw new CatalogDBException("Individual " + individualId + " not found");
            }
            if (remove.getNumDeleted() == 0) {
                throw new CatalogDBException("Individual " + individualId + " could not be deleted");
            }

            logger.debug("Individual uid '{}' version '{}' deleted from main INDIVIDUAL collection", individualUid, individualVersion);
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
        return new IndividualCatalogMongoDBIterator<>(mongoCursor, individualConverter, null, dbAdaptorFactory, options);
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
        return new IndividualCatalogMongoDBIterator(mongoCursor, null, null, dbAdaptorFactory, options);
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
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name());

        return new IndividualCatalogMongoDBIterator<>(mongoCursor, individualConverter, iteratorFilter, dbAdaptorFactory, studyUid, user,
                options);
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
        Function<Document, Document> iteratorFilter = (d) ->  filterAnnotationSets(studyDocument, d, user,
                StudyAclEntry.StudyPermissions.VIEW_INDIVIDUAL_ANNOTATIONS.name(),
                IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name());

        return new IndividualCatalogMongoDBIterator(mongoCursor, null, iteratorFilter, dbAdaptorFactory, studyUid, user, options);
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

        logger.debug("Individual get: query : {}", bson.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        if (!query.getBoolean(QueryParams.DELETED.key())) {
            return individualCollection.iterator(clientSession, bson, null, null, qOptions);
        } else {
            return deletedIndividualCollection.iterator(clientSession, bson, null, null, qOptions);
        }
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
            if (containsAnnotationQuery(query)) {
                andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user,
                        IndividualAclEntry.IndividualPermissions.VIEW_ANNOTATIONS.name(), Enums.Resource.INDIVIDUAL));
            } else {
                andBsonList.add(getQueryForAuthorisedEntries(studyDocument, user, IndividualAclEntry.IndividualPermissions.VIEW.name(),
                        Enums.Resource.INDIVIDUAL));
            }

            andBsonList.addAll(AuthorizationMongoDBUtils.parseAclQuery(studyDocument, query, Enums.Resource.INDIVIDUAL, user));

            query.remove(ParamConstants.ACL_PARAM);
        }

        Query queryCopy = new Query(query);
        queryCopy.remove(QueryParams.DELETED.key());

        fixComplexQueryParam(QueryParams.ATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.BATTRIBUTES.key(), queryCopy);
        fixComplexQueryParam(QueryParams.NATTRIBUTES.key(), queryCopy);

        boolean uidVersionQueryFlag = generateUidVersionQuery(queryCopy, andBsonList);

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
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), queryCopy, queryParam.type(), andBsonList);
                        break;
                    case PHENOTYPES:
                    case DISORDERS:
                        addOntologyQueryFilter(queryParam.key(), queryParam.key(), queryCopy, andBsonList);
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
                    case STATUS_NAME:
                        addAutoOrQuery(QueryParams.STATUS_NAME.key(), queryParam.key(), query, QueryParams.STATUS_NAME.type(), andBsonList);
                        break;
                    case INTERNAL_STATUS:
                    case INTERNAL_STATUS_NAME:
                        // Convert the status to a positive status
                        query.put(queryParam.key(), Status.getPositiveStatus(Status.STATUS_LIST, query.getString(queryParam.key())));
                        addAutoOrQuery(QueryParams.INTERNAL_STATUS_NAME.key(), queryParam.key(), query,
                                QueryParams.INTERNAL_STATUS_NAME.type(), andBsonList);
                        break;
                    case ID:
                    case UUID:
                    case NAME:
                    case FATHER_UID:
                    case MOTHER_UID:
                    case DATE_OF_BIRTH:
                    case SEX:
                    case ETHNICITY:
                    case INTERNAL_STATUS_DATE:
                    case POPULATION_NAME:
                    case POPULATION_SUBPOPULATION:
                    case POPULATION_DESCRIPTION:
                    case KARYOTYPIC_SEX:
                    case LIFE_STATUS:
                    case RELEASE:
                    case VERSION:
                    case SAMPLE_UIDS:
//                    case ANNOTATION_SETS:
                    case PHENOTYPES_ID:
                    case PHENOTYPES_NAME:
                    case PHENOTYPES_SOURCE:
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
        if (extraQuery != null && extraQuery.size() > 0) {
            andBsonList.add(extraQuery);
        }
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
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
                new ObjectMap(QueryParams.SAMPLES.key(), ParamUtils.UpdateAction.REMOVE.name()));

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

        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);

        Bson bsonQuery = parseQuery(query);

        logger.debug("Sample references extraction. Query: {}, update: {}",
                bsonQuery.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()),
                update.toBsonDocument(Document.class, MongoClient.getDefaultCodecRegistry()));
        DataResult updateResult = individualCollection.update(clientSession, bsonQuery, update, multi);
        logger.debug("Sample uid '" + sampleUid + "' references removed from " + updateResult.getNumUpdated() + " out of "
                + updateResult.getNumMatches() + " individuals");
    }

    public MongoDBCollection getIndividualCollection() {
        return individualCollection;
    }

}
