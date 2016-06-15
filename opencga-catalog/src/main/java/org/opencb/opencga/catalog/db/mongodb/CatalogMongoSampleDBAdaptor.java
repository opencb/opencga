/*
 * Copyright 2015 OpenCB
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

import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.SampleConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.SampleAcl;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.Math.toIntExact;
import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * Created by hpccoll1 on 14/08/15.
 */
public class CatalogMongoSampleDBAdaptor extends CatalogMongoDBAdaptor implements CatalogSampleDBAdaptor {

    private final MongoDBCollection sampleCollection;
    private SampleConverter sampleConverter;

    public CatalogMongoSampleDBAdaptor(MongoDBCollection sampleCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoSampleDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.sampleCollection = sampleCollection;
        this.sampleConverter = new SampleConverter();
    }

    /*
     * Samples methods
     * ***************************
     */

    @Override
    public QueryResult<Sample> createSample(long studyId, Sample sample, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        /*
        QueryResult<Long> count = sampleCollection.count(
                new BasicDBObject("name", sample.getName()).append(PRIVATE_STUDY_ID, studyId));
                */
        Bson bson = Filters.and(Filters.eq("name", sample.getName()), Filters.eq(PRIVATE_STUDY_ID, studyId));
        QueryResult<Long> count = sampleCollection.count(bson);
//                new BsonDocument("name", new BsonString(sample.getName())).append(PRIVATE_STUDY_ID, new BsonInt32(studyId)));
        if (count.getResult().get(0) > 0) {
            throw new CatalogDBException("Sample { name: '" + sample.getName() + "'} already exists.");
        }

        long sampleId = getNewId();
        sample.setId(sampleId);
        sample.setAnnotationSets(Collections.<AnnotationSet>emptyList());
        //TODO: Add annotationSets
        Document sampleObject = getMongoDBDocument(sample, "sample");
        sampleObject.put(PRIVATE_STUDY_ID, studyId);
        sampleObject.put(PRIVATE_ID, sampleId);
        sampleCollection.insert(sampleObject, null);

        return endQuery("createSample", startTime, getSample(sampleId, options));
    }


    @Override
    public QueryResult<Sample> getSample(long sampleId, QueryOptions options) throws CatalogDBException {
        checkSampleId(sampleId);
        return get(new Query(QueryParams.ID.key(), sampleId).append(QueryParams.STATUS_STATUS.key(), "!=" + Status.REMOVED), options);
//        long startTime = startQuery();
//        //QueryOptions filteredOptions = filterOptions(options, FILTER_ROUTE_SAMPLES);
//
////        DBObject query = new BasicDBObject(PRIVATE_ID, sampleId);
//        Query query1 = new Query(QueryParams.ID.key(), sampleId);
//        QueryResult<Sample> sampleQueryResult = get(query1, options);
//
////        QueryResult<Document> queryResult = sampleCollection.find(bson, filteredOptions);
////        List<Sample> samples = parseSamples(queryResult);
//
//        if (sampleQueryResult.getResult().size() == 0) {
//            throw CatalogDBException.idNotFound("Sample", sampleId);
//        }
//
//        return endQuery("getSample", startTime, sampleQueryResult);
    }

//    @Override
//    public QueryResult<Sample> getAllSamples(QueryOptions options) throws CatalogDBException {
//        throw new UnsupportedOperationException("Deprecated method. Use get instead");
//        /*
//        int variableSetId = options.getInt(SampleFilterOption.variableSetId.toString());
//        Map<String, Variable> variableMap = null;
//        if (variableSetId > 0) {
//            variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
//                    .getVariables().stream().collect(Collectors.toMap(Variable::getId, Function.identity()));
//        }
//        return getAllSamples(variableMap, options);*/
//    }

    @Override
    public QueryResult<Sample> getAllSamplesInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId);
        return endQuery("Get all files", startTime, get(query, options).getResult());
    }

    @Override
    public QueryResult<Sample> modifySample(long sampleId, QueryOptions parameters) throws CatalogDBException {
        long startTime = startQuery();

        Map<String, Object> sampleParams = new HashMap<>();
        //List<Bson> sampleParams = new ArrayList<>();

        String[] acceptedParams = {"source", "description", "name"};
        filterStringParams(parameters, sampleParams, acceptedParams);

        if (sampleParams.containsKey("name")) {
            // Check that the new sample name is still unique
            long studyId = getStudyIdBySampleId(sampleId);

            QueryResult<Long> count = sampleCollection.count(
                    new Document("name", sampleParams.get("name")).append(PRIVATE_STUDY_ID, studyId));
            if (count.getResult().get(0) > 0) {
                throw new CatalogDBException("Sample { name: '" + sampleParams.get("name") + "'} already exists.");
            }
        }

        String[] acceptedIntParams = {"individualId"};
        filterIntParams(parameters, sampleParams, acceptedIntParams);

        String[] acceptedMapParams = {"attributes"};
        filterMapParams(parameters, sampleParams, acceptedMapParams);

        if (sampleParams.containsKey("individualId")) {
            int individualId = parameters.getInt("individualId");
            if (individualId > 0 && !dbAdaptorFactory.getCatalogIndividualDBAdaptor().individualExists(individualId)) {
                throw CatalogDBException.idNotFound("Individual", individualId);
            }
        }

        if (!sampleParams.isEmpty()) {
            /*QueryResult<WriteResult> update = sampleCollection.update(new BasicDBObject(PRIVATE_ID, sampleId),
                    new BasicDBObject("$set", sampleParams), null);
                    */
            Bson query = Filters.eq(PRIVATE_ID, sampleId);
            Bson operation = new Document("$set", sampleParams);
            QueryResult<UpdateResult> update = sampleCollection.update(query, operation, null);

            if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
                throw CatalogDBException.idNotFound("Sample", sampleId);
            }
        }

        return endQuery("Modify sample", startTime, getSample(sampleId, parameters));
    }

    @Override
    public QueryResult<SampleAcl> getSampleAcl(long sampleId, String userId) throws CatalogDBException {
        long startTime = startQuery();
        checkSampleId(sampleId);
        Bson match = Aggregates.match(Filters.eq(PRIVATE_ID, sampleId));
        Bson unwind = Aggregates.unwind("$" + QueryParams.ACLS.key());
        Bson match2 = Aggregates.match(Filters.in(QueryParams.ACLS_USERS.key(), userId));
        Bson project = Aggregates.project(Projections.include(QueryParams.ID.key(), QueryParams.ACLS.key()));

        List<SampleAcl> sampleAcl = null;
        QueryResult<Document> aggregate = sampleCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);
        List<Sample> sampleList = parseSamples(aggregate);

        if (sampleList.size() > 0) {
            sampleAcl = sampleList.get(0).getAcls();
        }

        return endQuery("getSampleAcl", startTime, sampleAcl);
    }

    @Override
    public QueryResult<SampleAcl> getSampleAcl(long sampleId, List<String> members) throws CatalogDBException {
        long startTime = startQuery();
        checkSampleId(sampleId);
        Bson match = Aggregates.match(Filters.eq(PRIVATE_ID, sampleId));
        Bson unwind = Aggregates.unwind("$" + QueryParams.ACLS.key());
        Bson match2 = Aggregates.match(Filters.in(QueryParams.ACLS_USERS.key(), members));
        Bson project = Aggregates.project(Projections.include(QueryParams.ID.key(), QueryParams.ACLS.key()));

        List<SampleAcl> sampleAcl = null;
        QueryResult<Document> aggregate = sampleCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);
        List<Sample> sampleList = parseSamples(aggregate);

        if (sampleList.size() > 0) {
            sampleAcl = sampleList.get(0).getAcls();
        }

        return endQuery("getSampleAcl", startTime, sampleAcl);
    }

    @Deprecated
    @Override
    public QueryResult<AclEntry> setSampleAcl(long sampleId, AclEntry acl) throws CatalogDBException {
        return null;
//        long startTime = startQuery();
//
//        String userId = acl.getUserId();
//        /*DBObject query;
//        DBObject newAclObject = getDbObject(acl, "ACL");
//        DBObject update;
//*/
//        Bson query;
//        Document newAclObject = getMongoDBDocument(acl, "ACL");
//        Bson update;
//
//        /*
//        List<AclEntry> aclList = getSampleAcl(sampleId, userId).getResult();
//        if (aclList.isEmpty()) {  // there is no acl for that user in that file. push
//            query = new BasicDBObject(PRIVATE_ID, sampleId);
//            update = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
//        } else {    // there is already another ACL: overwrite
//            query = BasicDBObjectBuilder
//                    .start(PRIVATE_ID, sampleId)
//                    .append("acl.userId", userId).get();
//            update = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));
//        }
//        */
//        CatalogMongoDBUtils.checkAclUserId(dbAdaptorFactory, userId, getStudyIdBySampleId(sampleId));
//
//        List<AclEntry> aclList = getSampleAcl(sampleId, userId).getResult();
//        if (aclList.isEmpty()) { // there is no acl for that user in that file. Push
//            query = new Document(PRIVATE_ID, sampleId);
//            update = Updates.push("acl", newAclObject);
//        } else {
//            query = new Document(PRIVATE_ID, sampleId).append("acl.userId", userId);
//            update = Updates.set("acl.$", newAclObject);
//        }
//
//        QueryResult<UpdateResult> queryResult = sampleCollection.update(query, update, null);
//        if (queryResult.first().getModifiedCount() != 1) {
//            throw CatalogDBException.idNotFound("Sample", sampleId);
//        }
//
//        return endQuery("setSampleAcl", startTime, getSampleAcl(sampleId, userId));
    }

    @Override
    public QueryResult<SampleAcl> setSampleAcl(long sampleId, SampleAcl acl, boolean override) throws CatalogDBException {
        long startTime = startQuery();
        checkSampleId(sampleId);
        long studyId = getStudyIdBySampleId(sampleId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, studyId, acl.getUsers());

        // If there are groups in acl.getUsers(), we will obtain all the users belonging to the groups and will check if any of them
        // already have permissions on its own.
        Map<String, List<String>> groups = new HashMap<>();
        Set<String> users = new HashSet<>();

        for (String member : acl.getUsers()) {
            if (member.startsWith("@")) {
                Group group = dbAdaptorFactory.getCatalogStudyDBAdaptor().getGroup(studyId, member, Collections.emptyList()).first();
                groups.put(group.getId(), group.getUserIds());
            } else {
                users.add(member);
            }
        }
        if (groups.size() > 0) {
            // Check if any user already have permissions set on their own.
            for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
                QueryResult<SampleAcl> sampleAcl = getSampleAcl(sampleId, entry.getValue());
                if (sampleAcl.getNumResults() > 0) {
                    throw new CatalogDBException("Error when adding permissions in sample. At least one user in " + entry.getKey()
                            + " has already defined permissions for sample " + sampleId);
                }
            }
        }

        // Check if any of the users in the set of users also belongs to any introduced group. In that case, we will remove the user
        // because the group will be given the permission.
        for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
            for (String userId : entry.getValue()) {
                if (users.contains(userId)) {
                    users.remove(userId);
                }
            }
        }

        // Create the definitive list of members that will be added in the acl
        List<String> members = new ArrayList<>(users.size() + groups.size());
        members.addAll(users.stream().collect(Collectors.toList()));
        members.addAll(groups.entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList()));
        acl.setUsers(members);

        // Check if the members of the new acl already have some permissions set
        QueryResult<SampleAcl> sampleAcls = getSampleAcl(sampleId, acl.getUsers());
        if (sampleAcls.getNumResults() > 0 && override) {
            Set<String> usersSet = new HashSet<>(acl.getUsers().size());
            usersSet.addAll(acl.getUsers().stream().collect(Collectors.toList()));

            List<String> usersToOverride = new ArrayList<>();
            for (SampleAcl sampleAcl : sampleAcls.getResult()) {
                for (String member : sampleAcl.getUsers()) {
                    if (usersSet.contains(member)) {
                        // Add the user to the list of users that will be taken out from the Acls.
                        usersToOverride.add(member);
                    }
                }
            }

            // Now we remove the old permissions set for the users that already existed so the permissions are overriden by the new ones.
            unsetSampleAcl(sampleId, usersToOverride, Collections.emptyList());
        }  else if (sampleAcls.getNumResults() > 0 && !override) {
            throw new CatalogDBException("setSampleAcl: " + sampleAcls.getNumResults() + " of the members already had an Acl set. If you "
                    + "still want to set the Acls for them and remove the old one, please use the override parameter.");
        }

        // Append the users to the existing acl.
        List<String> permissions = acl.getPermissions().stream().map(SampleAcl.SamplePermissions::name).collect(Collectors.toList());

        // Check if the permissions found on acl already exist on sample id
        Document queryDocument = new Document(PRIVATE_ID, sampleId);
        if (permissions.size() > 0) {
            queryDocument.append(QueryParams.ACLS_PERMISSIONS.key(), new Document("$size", permissions.size()).append("$all", permissions));
        } else {
            queryDocument.append(QueryParams.ACLS_PERMISSIONS.key(), new Document("$size", 0));
        }

        Bson update;
        if (sampleCollection.count(queryDocument).first() > 0) {
            // Append the users to the existing acl.
            update = new Document("$addToSet", new Document("acls.$.users", new Document("$each", acl.getUsers())));
        } else {
            queryDocument = new Document(PRIVATE_ID, sampleId);
            // Push the new acl to the list of acls.
            update = new Document("$push", new Document(QueryParams.ACLS.key(), getMongoDBDocument(acl, "SampleAcl")));

        }

        QueryResult<UpdateResult> updateResult = sampleCollection.update(queryDocument, update, null);
        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("setSampleAcl: An error occurred when trying to share sample " + sampleId
                    + " with other members.");
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, QueryParams.ACLS.key());
        Sample sample = sampleConverter.convertToDataModelType(sampleCollection.find(queryDocument, queryOptions).first());

        return endQuery("setSampleAcl", startTime, sample.getAcls());
    }

    @Deprecated
    @Override
    public QueryResult<AclEntry> unsetSampleAcl(long sampleId, String userId) throws CatalogDBException {
        return null;
//
//        long startTime = startQuery();
///*
//        QueryResult<SampleAcl> sampleAcl = getSampleAcl(sampleId, userId);
//        DBObject query = new BasicDBObject(PRIVATE_ID, sampleId);
//        ;
//        DBObject update = new BasicDBObject("$pull", new BasicDBObject("acl", new BasicDBObject("userId", userId)));
//
//        QueryResult queryResult = sampleCollection.update(query, update, null);
//*/
//        QueryResult<AclEntry> sampleAcl = getSampleAcl(sampleId, userId);
//
//        if (!sampleAcl.getResult().isEmpty()) {
//            Bson query = new Document(PRIVATE_ID, sampleId);
//            Bson update = Updates.pull("acl", new Document("userId", userId));
//            sampleCollection.update(query, update, null);
//        }
//
//        return endQuery("unsetSampleAcl", startTime, sampleAcl);

    }

    public void unsetSampleAcl(long sampleId, List<String> members, List<String> permissions) throws CatalogDBException {
        checkSampleId(sampleId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, getStudyIdBySampleId(sampleId), members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_ID, sampleId)
                    .append("acls", new Document("$elemMatch", new Document("users", member)));
            Bson update;
            if (permissions.size() == 0) {
                update = new Document("$pull", new Document("acls.$.users", member));
            } else {
                update = new Document("$pull", new Document("acls.$.permissions", new Document("$in", permissions)));
            }
            QueryResult<UpdateResult> updateResult = sampleCollection.update(query, update, null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("unsetSampleAcl: An error occurred when trying to stop sharing sample " + sampleId
                        + " with other " + member + ".");
            }
        }

        // Remove possible SampleAcls that might have permissions defined but no users
        Bson queryBson = new Document(QueryParams.ID.key(), sampleId)
                .append(QueryParams.ACLS_USERS.key(),
                        new Document("$exists", true).append("$eq", Collections.emptyList()));
        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
        sampleCollection.update(queryBson, update, null);
    }

    @Override
    public void unsetSampleAclsInStudy(long studyId, List<String> members) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, studyId, members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_STUDY_ID, studyId)
                    .append("acls", new Document("$elemMatch", new Document("users", member)));
            Bson update = new Document("$pull", new Document("acls.$.users", member));
            sampleCollection.update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));
        }

        // Remove possible SampleAcls that might have permissions defined but no users
        Bson queryBson = new Document(PRIVATE_STUDY_ID, studyId)
                .append(QueryParams.ACLS_USERS.key(),
                        new Document("$exists", true).append("$eq", Collections.emptyList()));
        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
        sampleCollection.update(queryBson, update, new QueryOptions(MongoDBCollection.MULTI, true));
    }


//    @Override
//    public QueryResult<Sample> deleteSample(int sampleId) throws CatalogDBException {
//        Query query = new Query(CatalogStudyDBAdaptor.QueryParams.ID.key(), sampleId);
//        QueryResult<Sample> sampleQueryResult = get(query, new QueryOptions());
//        if (sampleQueryResult.getResult().size() == 1) {
//            QueryResult<Long> delete = delete(query);
//            if (delete.getResult().size() == 0) {
//                throw CatalogDBException.newInstance("Sample id '{}' has not been deleted", sampleId);
//            }
//        } else {
//            throw CatalogDBException.newInstance("Sample id '{}' does not exist", sampleId);
//        }
//        return sampleQueryResult;
//    }


//    @Override
//    public QueryResult<Sample> deleteSample(int sampleId) throws CatalogDBException {
//        long startTime = startQuery();
//
//        QueryResult<Sample> sampleQueryResult = getSample(sampleId, null);
//
//        checkInUse(sampleId);
//        DeleteResult id = sampleCollection.remove(new BasicDBObject(PRIVATE_ID, sampleId), null).getResult().get(0);
//        if (id.getDeletedCount() == 0) {
//            throw CatalogDBException.idNotFound("Sample", sampleId);
//        } else {
//            return endQuery("delete sample", startTime, sampleQueryResult);
//        }
//    }


//    public void checkInUse(int sampleId) throws CatalogDBException {
//        int studyId = getStudyIdBySampleId(sampleId);
//
//        QueryOptions query = new QueryOptions(FileFilterOption.sampleIds.toString(), sampleId);
//        QueryOptions queryOptions = new QueryOptions("include", Arrays.asList("projects.studies.files.id", "projects.studies.files
// .path"));
//        QueryResult<File> fileQueryResult = dbAdaptorFactory.getCatalogFileDBAdaptor().getAllFiles(query, queryOptions);
//        if (fileQueryResult.getNumResults() != 0) {
//            String msg = "Can't delete Sample " + sampleId + ", still in use in \"sampleId\" array of files : " +
//                    fileQueryResult.getResult().stream()
//                            .map(file -> "{ id: " + file.getId() + ", path: \"" + file.getPath() + "\" }")
//                            .collect(Collectors.joining(", ", "[", "]"));
//            throw new CatalogDBException(msg);
//        }
//
//
//        queryOptions = new QueryOptions(CohortFilterOption.samples.toString(), sampleId)
//                .append("include", Arrays.asList("projects.studies.cohorts.id", "projects.studies.cohorts.name"));
//        QueryResult<Cohort> cohortQueryResult = getAllCohorts(studyId, queryOptions);/**/
//        if (cohortQueryResult.getNumResults() != 0) {
//            String msg = "Can't delete Sample " + sampleId + ", still in use in cohorts : " +
//                    cohortQueryResult.getResult().stream()
//                            .map(cohort -> "{ id: " + cohort.getId() + ", name: \"" + cohort.getName() + "\" }")
//                            .collect(Collectors.joining(", ", "[", "]"));
//            throw new CatalogDBException(msg);
//        }
//
//    }

    @Override
    public long getStudyIdBySampleId(long sampleId) throws CatalogDBException {
        /*DBObject query = new BasicDBObject(PRIVATE_ID, sampleId);
        BasicDBObject projection = new BasicDBObject(PRIVATE_STUDY_ID, true);
        QueryResult<DBObject> queryResult = sampleCollection.find(query, projection, null);*/
        Bson query = new Document(PRIVATE_ID, sampleId);
        Bson projection = Projections.include(PRIVATE_STUDY_ID);
        QueryResult<Document> queryResult = sampleCollection.find(query, projection, null);

        if (!queryResult.getResult().isEmpty()) {
            Object studyId = queryResult.getResult().get(0).get(PRIVATE_STUDY_ID);
            return studyId instanceof Number ? ((Number) studyId).longValue() : Long.parseLong(studyId.toString());
        } else {
            throw CatalogDBException.idNotFound("Sample", sampleId);
        }
    }

    @Override
    public List<Long> getStudyIdsBySampleIds(String sampleIds) throws CatalogDBException {
        Bson query = parseQuery(new Query(QueryParams.ID.key(), sampleIds));
        return sampleCollection.distinct(PRIVATE_STUDY_ID, query, Long.class).getResult();
    }

    /*
     * Annotations Methods
     * ***************************
     */

    @Override
    public QueryResult<AnnotationSet> annotateSample(long sampleId, AnnotationSet annotationSet, boolean overwrite) throws
            CatalogDBException {
        long startTime = startQuery();

        /*QueryResult<Long> count = sampleCollection.count(
                new BasicDBObject("annotationSets.id", annotationSet.getId()).append(PRIVATE_ID, sampleId));*/
        QueryResult<Long> count = sampleCollection.count(new Document("annotationSets.id", annotationSet.getId())
                .append(PRIVATE_ID, sampleId));
        if (overwrite) {
            if (count.getResult().get(0) == 0) {
                throw CatalogDBException.idNotFound("AnnotationSet", annotationSet.getId());
            }
        } else {
            if (count.getResult().get(0) > 0) {
                throw CatalogDBException.alreadyExists("AnnotationSet", "id", annotationSet.getId());
            }
        }

        /*DBObject object = getDbObject(annotationSet, "AnnotationSet");

        DBObject query = new BasicDBObject(PRIVATE_ID, sampleId);*/
        Document object = getMongoDBDocument(annotationSet, "AnnotationSet");

        Bson query = new Document(PRIVATE_ID, sampleId);
        if (overwrite) {
            ((Document) query).put("annotationSets.id", annotationSet.getId());
        } else {
            ((Document) query).put("annotationSets.id", new Document("$ne", annotationSet.getId()));
        }

        /*
        DBObject update;
        if (overwrite) {
            update = new BasicDBObject("$set", new BasicDBObject("annotationSets.$", object));
        } else {
            update = new BasicDBObject("$push", new BasicDBObject("annotationSets", object));
        }
*/

        Bson update;
        if (overwrite) {
            update = Updates.set("annotationSets.$", object);
        } else {
            update = Updates.push("annotationSets", object);
        }

        QueryResult<UpdateResult> queryResult = sampleCollection.update(query, update, null);

        if (queryResult.first().getModifiedCount() != 1) {
            throw CatalogDBException.alreadyExists("AnnotationSet", "id", annotationSet.getId());
        }

        return endQuery("", startTime, Collections.singletonList(annotationSet));
    }

    @Override
    public QueryResult<AnnotationSet> deleteAnnotation(long sampleId, String annotationId) throws CatalogDBException {

        long startTime = startQuery();

        Sample sample = getSample(sampleId, new QueryOptions("include", "projects.studies.samples.annotationSets")).first();
        AnnotationSet annotationSet = null;
        for (AnnotationSet as : sample.getAnnotationSets()) {
            if (as.getId().equals(annotationId)) {
                annotationSet = as;
                break;
            }
        }

        if (annotationSet == null) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

        /*
        DBObject query = new BasicDBObject(PRIVATE_ID, sampleId);
        DBObject update = new BasicDBObject("$pull", new BasicDBObject("annotationSets", new BasicDBObject("id", annotationId)));
        */
        Bson query = new Document(PRIVATE_ID, sampleId);
        Bson update = Updates.pull("annotationSets", new Document("id", annotationId));
        QueryResult<UpdateResult> resultQueryResult = sampleCollection.update(query, update, null);
        if (resultQueryResult.first().getModifiedCount() < 1) {
            throw CatalogDBException.idNotFound("AnnotationSet", annotationId);
        }

        return endQuery("Delete annotation", startTime, Collections.singletonList(annotationSet));
    }

    /**
     * The method will add the new variable to each annotation using the default value.
     * @param variableSetId id of the variableSet.
     * @param variable new variable that will be pushed to the annotations.
     */
    @Override
    public QueryResult<Long> addVariableToAnnotations(long variableSetId, Variable variable) throws CatalogDBException {
        long startTime = startQuery();

        Annotation annotation = new Annotation(variable.getId(), variable.getDefaultValue());
        // Obtain the annotation ids of the annotations that are using the variableSet variableSetId
        List<Bson> aggregation = new ArrayList<>(4);
        aggregation.add(Aggregates.match(Filters.eq("annotationSets.variableSetId", variableSetId)));
        aggregation.add(Aggregates.unwind("$annotationSets"));
        aggregation.add(Aggregates.project(Projections.include("annotationSets.id", "annotationSets.variableSetId")));
        aggregation.add(Aggregates.match(Filters.eq("annotationSets.variableSetId", variableSetId)));
        QueryResult<Document> aggregationResult = sampleCollection.aggregate(aggregation, null);

        Set<String> annotationIds = new HashSet<>(aggregationResult.getNumResults());
        for (Document document : aggregationResult.getResult()) {
            annotationIds.add((String) ((Document) document.get("annotationSets")).get("id"));
        }

        Bson bsonQuery;
        Bson update = Updates.push("annotationSets.$." + AnnotationSetParams.ANNOTATIONS.key(),
                getMongoDBDocument(annotation, "annotation"));
        long modifiedCount = 0;
        for (String annotationId : annotationIds) {
            bsonQuery = Filters.elemMatch("annotationSets", Filters.and(
                    Filters.eq("variableSetId", variableSetId),
                    Filters.eq("id", annotationId)
            ));

            modifiedCount += sampleCollection.update(bsonQuery, update, new QueryOptions(MongoDBCollection.MULTI, true)).first()
                    .getModifiedCount();
        }

        return endQuery("Add new variable to annotations", startTime, Collections.singletonList(modifiedCount));
    }

    @Override
    public QueryResult<Long> renameAnnotationField(long variableSetId, String oldName, String newName) throws CatalogDBException {
        long renamedAnnotations = 0;

        // 1. we obtain the variable
        List<Sample> sampleAnnotations = getAnnotation(variableSetId, oldName);

        if (sampleAnnotations.size() > 0) {
            // Fixme: Change the hard coded annotationSets names per their corresponding QueryParam objects.
            for (Sample sample : sampleAnnotations) {
                for (AnnotationSet annotationSet : sample.getAnnotationSets()) {
                    Bson bsonQuery = Filters.and(
                            Filters.eq(QueryParams.ID.key(), sample.getId()),
                            Filters.eq("annotationSets.id", annotationSet.getId()),
                            Filters.eq("annotationSets.annotations.id", oldName)
                    );

                    // 1. We extract the annotation.
                    Bson update = Updates.pull("annotationSets.$.annotations", Filters.eq("id", oldName));
                    QueryResult<UpdateResult> queryResult = sampleCollection.update(bsonQuery, update, null);
                    if (queryResult.first().getModifiedCount() != 1) {
                        throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - AnnotationSet {id: "
                                + annotationSet.getId() + "} - An unexpected error happened when extracting the annotation " + oldName
                                + ". Please, report this error to the OpenCGA developers.");
                    }

                    // 2. We change the id and push it again
                    Iterator<Annotation> iterator = annotationSet.getAnnotations().iterator();
                    Annotation annotation = iterator.next();
                    annotation.setId(newName);
                    bsonQuery = Filters.and(
                            Filters.eq(QueryParams.ID.key(), sample.getId()),
                            Filters.eq("annotationSets.id", annotationSet.getId())
                    );
                    update = Updates.push("annotationSets.$.annotations", getMongoDBDocument(annotation, "Annotation"));
                    queryResult = sampleCollection.update(bsonQuery, update, null);

                    if (queryResult.first().getModifiedCount() != 1) {
                        throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - AnnotationSet {id: "
                                + annotationSet.getId() + "} - A critical error happened when trying to rename the annotation " + oldName
                                + ". Please, report this error to the OpenCGA developers.");
                    }
                    renamedAnnotations += 1;
                }
            }
        }

        return new QueryResult<>("Rename annotation field", -1, toIntExact(renamedAnnotations), renamedAnnotations, "", "",
                Collections.singletonList(renamedAnnotations));
    }

    @Override
    public QueryResult<Long> removeAnnotationField(long variableSetId, String fieldId) throws CatalogDBException {
        long renamedAnnotations = 0;

        // 1. we obtain the variable
        List<Sample> sampleAnnotations = getAnnotation(variableSetId, fieldId);

        if (sampleAnnotations.size() > 0) {
            // Fixme: Change the hard coded annotationSets names per their corresponding QueryParam objects.
            for (Sample sample : sampleAnnotations) {
                for (AnnotationSet annotationSet : sample.getAnnotationSets()) {
                    Bson bsonQuery = Filters.and(
                            Filters.eq(QueryParams.ID.key(), sample.getId()),
                            Filters.eq("annotationSets.id", annotationSet.getId()),
                            Filters.eq("annotationSets.annotations.id", fieldId)
                    );

                    // We extract the annotation.
                    Bson update = Updates.pull("annotationSets.$.annotations", Filters.eq("id", fieldId));
                    QueryResult<UpdateResult> queryResult = sampleCollection.update(bsonQuery, update, null);
                    if (queryResult.first().getModifiedCount() != 1) {
                        throw new CatalogDBException("VariableSet {id: " + variableSetId + "} - AnnotationSet {id: "
                                + annotationSet.getId() + "} - An unexpected error happened when extracting the annotation " + fieldId
                                + ". Please, report this error to the OpenCGA developers.");
                    }

                    renamedAnnotations += 1;
                }
            }
        }

        return new QueryResult<>("Remove annotation field", -1, toIntExact(renamedAnnotations), renamedAnnotations, "", "",
                Collections.singletonList(renamedAnnotations));
    }

    /**
     * The method will return the list of samples containing the annotation.
     * @param variableSetId Id of the variableSet.
     * @param annotationFieldId Name of the field of the annotation from all the annotationSets.
     * @return list of samples containing an array of annotationSets, containing just the annotation that matches with annotationFieldId.
     */
    private List<Sample> getAnnotation(long variableSetId, String annotationFieldId) {
        // Fixme: Change the hard coded annotationSets names per their corresponding QueryParam objects.
        List<Bson> aggregation = new ArrayList<>();
        aggregation.add(Aggregates.match(Filters.elemMatch("annotationSets", Filters.eq("variableSetId", variableSetId))));
        aggregation.add(Aggregates.project(Projections.include("annotationSets", "id")));
        aggregation.add(Aggregates.unwind("$annotationSets"));
        aggregation.add(Aggregates.match(Filters.eq("annotationSets.variableSetId", variableSetId)));
        aggregation.add(Aggregates.unwind("$annotationSets.annotations"));
        aggregation.add(Aggregates.match(
                Filters.eq("annotationSets.annotations.id", annotationFieldId)));

        return sampleCollection.aggregate(aggregation, sampleConverter, new QueryOptions()).getResult();
    }

    @Deprecated
    public void checkInUse(long sampleId) throws CatalogDBException {
        long studyId = getStudyIdBySampleId(sampleId);

        Query query = new Query(CatalogFileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleId);
        QueryOptions queryOptions = new QueryOptions(MongoDBCollection.INCLUDE, Arrays.asList(FILTER_ROUTE_FILES + CatalogFileDBAdaptor
                .QueryParams.ID.key(), FILTER_ROUTE_FILES + CatalogFileDBAdaptor.QueryParams.PATH.key()));
        QueryResult<File> fileQueryResult = dbAdaptorFactory.getCatalogFileDBAdaptor().get(query, queryOptions);
        if (fileQueryResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in \"sampleId\" array of files : "
                    + fileQueryResult.getResult().stream()
                            .map(file -> "{ id: " + file.getId() + ", path: \"" + file.getPath() + "\" }")
                            .collect(Collectors.joining(", ", "[", "]"));
            throw new CatalogDBException(msg);
        }


        queryOptions = new QueryOptions(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId)
                .append(MongoDBCollection.INCLUDE, Arrays.asList(FILTER_ROUTE_COHORTS + CatalogCohortDBAdaptor.QueryParams.ID.key(),
                        FILTER_ROUTE_COHORTS + CatalogCohortDBAdaptor.QueryParams.NAME.key()));
        QueryResult<Cohort> cohortQueryResult = dbAdaptorFactory.getCatalogCohortDBAdaptor().getAllCohorts(studyId, queryOptions);
        if (cohortQueryResult.getNumResults() != 0) {
            String msg = "Can't delete Sample " + sampleId + ", still in use in cohorts : "
                    + cohortQueryResult.getResult().stream()
                            .map(cohort -> "{ id: " + cohort.getId() + ", name: \"" + cohort.getName() + "\" }")
                            .collect(Collectors.joining(", ", "[", "]"));
            throw new CatalogDBException(msg);
        }
    }

    /**
     * To be able to delete a sample, the sample does not have to be part of any cohort.
     *
     * @param sampleId sample id.
     * @throws CatalogDBException if the sampleId is used on any cohort.
     */
    private void checkCanDelete(long sampleId) throws CatalogDBException {
        Query query = new Query(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId);
        if (dbAdaptorFactory.getCatalogCohortDBAdaptor().count(query).first() > 0) {
            List<Cohort> cohorts = dbAdaptorFactory.getCatalogCohortDBAdaptor()
                    .get(query, new QueryOptions(QueryOptions.INCLUDE, CatalogCohortDBAdaptor.QueryParams.ID.key())).getResult();
            throw new CatalogDBException("The sample {" + sampleId + "} cannot be deleted/removed. It is being used in "
                    + cohorts.size() + " cohorts: [" + cohorts.stream().map(Cohort::getId).collect(Collectors.toList()).toString() + "]");
        }
    }


    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return sampleCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return sampleCollection.distinct(field, bson);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Sample> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_SAMPLES);
        QueryResult<Sample> sampleQueryResult = sampleCollection.find(bson, sampleConverter, qOptions);
        logger.debug("Sample get: query : {}, dbTime: {}", bson, qOptions == null ? "" : qOptions.toJson(),
                sampleQueryResult.getDbTime());
        return endQuery("Get sample", startTime, sampleQueryResult);
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        //qOptions.append(MongoDBCollection.EXCLUDE, Arrays.asList(PRIVATE_ID, PRIVATE_STUDY_ID));
        qOptions = filterOptions(qOptions, FILTER_ROUTE_SAMPLES);
        return sampleCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        Map<String, Object> sampleParameters = new HashMap<>();

        final String[] acceptedParams = {QueryParams.SOURCE.key(), QueryParams.DESCRIPTION.key()};
        filterStringParams(parameters, sampleParameters, acceptedParams);

        final String[] acceptedIntParams = {QueryParams.ID.key(), QueryParams.INDIVIDUAL_ID.key()};
        filterIntParams(parameters, sampleParameters, acceptedIntParams);

        final String[] acceptedMapParams = {"attributes"};
        filterMapParams(parameters, sampleParameters, acceptedMapParams);

        if (parameters.containsKey(QueryParams.STATUS_STATUS.key())) {
            sampleParameters.put(QueryParams.STATUS_STATUS.key(), parameters.get(QueryParams.STATUS_STATUS.key()));
            sampleParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }

        if (!sampleParameters.isEmpty()) {
            QueryResult<UpdateResult> update = sampleCollection.update(parseQuery(query),
                    new Document("$set", sampleParameters), null);
            return endQuery("Update sample", startTime, Arrays.asList(update.getNumTotalResults()));
        }

        return endQuery("Update sample", startTime, new QueryResult<>());
    }

    @Override
    public QueryResult<Sample> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<Long> update = update(new Query(QueryParams.ID.key(), id), parameters);
        if (update.getNumTotalResults() != 1) {
            throw new CatalogDBException("Could not update sample with id " + id);
        }
        return endQuery("Update sample", startTime, getSample(id, null));
    }

    // TODO: Check clean
    public QueryResult<Sample> clean(long id) throws CatalogDBException {
        throw new UnsupportedOperationException("Clean is not yet implemented.");
    }

    @Override
    public QueryResult<Sample> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkSampleId(id);
        // Check the sample is active
        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_STATUS.key(), Status.READY);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_STATUS.key(), Status.DELETED + "," + Status.REMOVED);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.STATUS_STATUS.key());
            Sample sample = get(query, options).first();
            throw new CatalogDBException("The sample {" + id + "} was already " + sample.getStatus().getStatus());
        }

        // If we don't find the force parameter, we check first if the file could be deleted.
        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
            checkCanDelete(id);
        }

        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
            deleteReferencesToSample(id);
        }

        // Change the status of the sample to deleted
        setStatus(id, Status.DELETED);

        query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_STATUS.key(), Status.DELETED);

        return endQuery("Delete sample", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_STATUS.key(), Status.READY);
        QueryResult<Sample> sampleQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
        for (Sample sample : sampleQueryResult.getResult()) {
            delete(sample.getId(), queryOptions);
        }
        return endQuery("Delete sample", startTime, Collections.singletonList(sampleQueryResult.getNumTotalResults()));
    }

    @Override
    public QueryResult<Sample> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_STATUS.key(), Status.DELETED);
        return endQuery("Restore samples", startTime, setStatus(query, Status.READY));
    }

    @Override
    public QueryResult<Sample> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkSampleId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_STATUS.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The sample {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, Status.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore sample", startTime, get(query, null));
    }

    /***
     * Checks whether the sample id corresponds to any Individual and if it is parent of any other individual.
     * @param id Sample id that will be checked.
     * @throws CatalogDBException when the sample is parent of other individual.
     */
    @Deprecated
    public void checkSampleIsParentOfFamily(int id) throws CatalogDBException {
        Sample sample = getSample(id, new QueryOptions()).first();
        if (sample.getIndividualId() > 0) {
            Query query = new Query(CatalogIndividualDBAdaptor.QueryParams.FATHER_ID.key(), sample.getIndividualId())
                    .append(CatalogIndividualDBAdaptor.QueryParams.MOTHER_ID.key(), sample.getIndividualId());
            Long count = dbAdaptorFactory.getCatalogIndividualDBAdaptor().count(query).first();
            if (count > 0) {
                throw CatalogDBException.sampleIdIsParentOfOtherIndividual(id);
            }
        }
    }

    @Override
    public CatalogDBIterator<Sample> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = sampleCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, sampleConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = sampleCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(sampleCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(sampleCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(sampleCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        CatalogDBIterator<Sample> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();
        List<Bson> annotationList = new ArrayList<>();
        // We declare variableMap here just in case we have different annotation queries
        Map<String, Variable> variableMap = null;

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam =  QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            if (queryParam == null) {
                throw CatalogDBException.queryParamNotFound(key, "Samples");
            }
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_ID:
                        addOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case ATTRIBUTES:
                        addAutoOrQuery(entry.getKey(), entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case BATTRIBUTES:
                        String mongoKey = entry.getKey().replace(QueryParams.BATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case NATTRIBUTES:
                        mongoKey = entry.getKey().replace(QueryParams.NATTRIBUTES.key(), QueryParams.ATTRIBUTES.key());
                        addAutoOrQuery(mongoKey, entry.getKey(), query, queryParam.type(), andBsonList);
                        break;
                    case VARIABLE_SET_ID:
                        addOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), annotationList);
                        break;
                    case ANNOTATION:
                        if (variableMap == null) {
                            int variableSetId = query.getInt(QueryParams.VARIABLE_SET_ID.key());
                            if (variableSetId > 0) {
                                variableMap = dbAdaptorFactory.getCatalogStudyDBAdaptor().getVariableSet(variableSetId, null).first()
                                        .getVariables().stream().collect(Collectors.toMap(Variable::getId, Function.identity()));
                            }
                        }
                        addAnnotationQueryFilter(entry.getKey(), query, variableMap, annotationList);
                        break;
                    case ANNOTATION_SET_ID:
                        addOrQuery("id", queryParam.key(), query, queryParam.type(), annotationList);
                        break;
                    default:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                }
            } catch (Exception e) {
                if (e instanceof CatalogDBException) {
                    throw e;
                } else {
                    throw new CatalogDBException("Error parsing query : " + query.toJson(), e);
                }
            }
        }

        if (annotationList.size() > 0) {
            Bson projection = Projections.elemMatch(QueryParams.ANNOTATION_SETS.key(), Filters.and(annotationList));
            andBsonList.add(projection);
        }
        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    public MongoDBCollection getSampleCollection() {
        return sampleCollection;
    }

    QueryResult<Sample> setStatus(long sampleId, String status) throws CatalogDBException {
        return update(sampleId, new ObjectMap(QueryParams.STATUS_STATUS.key(), status));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_STATUS.key(), status));
    }

    private void deleteReferencesToSample(long sampleId) throws CatalogDBException {
        // Remove references from files
        Query query = new Query(CatalogFileDBAdaptor.QueryParams.SAMPLE_IDS.key(), sampleId);
        QueryResult<Long> result = dbAdaptorFactory.getCatalogFileDBAdaptor()
                .extractSampleFromFiles(query, Collections.singletonList(sampleId));
        logger.debug("SampleId {} extracted from {} files", sampleId, result.first());

        // Remove references from cohorts
        query = new Query(CatalogCohortDBAdaptor.QueryParams.SAMPLES.key(), sampleId);
        result = dbAdaptorFactory.getCatalogCohortDBAdaptor().extractSamplesFromCohorts(query, Collections.singletonList(sampleId));
        logger.debug("SampleId {} extracted from {} cohorts", sampleId, result.first());
    }
}
