package org.opencb.opencga.catalog.db.mongodb;

import com.mongodb.DuplicateKeyException;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.result.DeleteResult;
import com.mongodb.client.result.UpdateResult;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.db.mongodb.converters.FileConverter;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.AclEntry;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Group;
import org.opencb.opencga.catalog.models.Status;
import org.opencb.opencga.catalog.models.acls.FileAcl;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.db.mongodb.CatalogMongoDBUtils.*;

/**
 * Created by pfurio on 08/01/16.
 */
public class CatalogMongoFileDBAdaptor extends CatalogMongoDBAdaptor implements CatalogFileDBAdaptor {

    private final MongoDBCollection fileCollection;
    private FileConverter fileConverter;

    /***
     * CatalogMongoFileDBAdaptor constructor.
     *
     * @param fileCollection MongoDB connection to the file collection.
     * @param dbAdaptorFactory Generic dbAdaptorFactory containing all the different collections.
     */
    public CatalogMongoFileDBAdaptor(MongoDBCollection fileCollection, CatalogMongoDBAdaptorFactory dbAdaptorFactory) {
        super(LoggerFactory.getLogger(CatalogMongoFileDBAdaptor.class));
        this.dbAdaptorFactory = dbAdaptorFactory;
        this.fileCollection = fileCollection;
        this.fileConverter = new FileConverter();
    }

    /**
     *
     * @return MongoDB connection to the file collection.
     */
    public MongoDBCollection getFileCollection() {
        return fileCollection;
    }

    @Override
    public QueryResult<File> createFile(long studyId, File file, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();

        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        String ownerId = dbAdaptorFactory.getCatalogStudyDBAdaptor().getStudyOwnerId(studyId);

        if (filePathExists(studyId, file.getPath())) {
            throw CatalogDBException.alreadyExists("File from study { id:" + studyId + "}", "path", file.getPath());
        }

        //new File Id
        long newFileId = getNewId();
        file.setId(newFileId);
        if (file.getOwnerId() == null) {
            file.setOwnerId(ownerId);
        }
        Document fileDocument = fileConverter.convertToStorageType(file);
        fileDocument.append(PRIVATE_STUDY_ID, studyId);
        fileDocument.append(PRIVATE_ID, newFileId);

        try {
            fileCollection.insert(fileDocument, null);
        } catch (DuplicateKeyException e) {
            throw CatalogDBException.alreadyExists("File from study { id:" + studyId + "}", "path", file.getPath());
        }

        // Update the diskUsage field from the study collection
        try {
            dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, file.getDiskUsage());
        } catch (CatalogDBException e) {
            delete(newFileId, options);
            throw new CatalogDBException("File from study { id:" + studyId + "} was removed from the database due to problems "
                    + "with the study collection.");
        }

        return endQuery("Create file", startTime, getFile(newFileId, options));
    }

    @Override
    public QueryResult<File> get(Query query, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        if (!query.containsKey(QueryParams.STATUS_STATUS.key())) {
            query.append(QueryParams.STATUS_STATUS.key(), "!=" + Status.DELETED + ";!=" + Status.REMOVED);
        }
        Bson bson;
        try {
            bson = parseQuery(query);
        } catch (NumberFormatException e) {
            throw new CatalogDBException("Get file: Could not parse all the arguments from query - " + e.getMessage(), e.getCause());
        }
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_FILES);

        QueryResult<File> fileQueryResult = fileCollection.find(bson, fileConverter, qOptions);
        logger.debug("File get: query : {}, project: {}, dbTime: {}", bson, qOptions == null ? "" : qOptions.toJson(),
                fileQueryResult.getDbTime());
        return endQuery("get File", startTime, fileQueryResult);
    }

    @Override
    public QueryResult<File> getFile(long fileId, QueryOptions options) throws CatalogDBException {
        checkFileId(fileId);
        Query query = new Query(QueryParams.ID.key(), fileId).append(QueryParams.STATUS_STATUS.key(), "!=" + File.FileStatus.REMOVED);
        return get(query, options);
    }

    @Override
    public long getFileId(long studyId, String path) throws CatalogDBException {
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId).append(QueryParams.PATH.key(), path);
        QueryOptions options = new QueryOptions(MongoDBCollection.INCLUDE, "id");
        QueryResult<File> fileQueryResult = get(query, options);
        return fileQueryResult.getNumTotalResults() == 1 ? fileQueryResult.getResult().get(0).getId() : -1;
    }

    @Override
    public QueryResult<File> getAllFilesInStudy(long studyId, QueryOptions options) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        Query query = new Query(QueryParams.STUDY_ID.key(), studyId);
        return get(query, options);
    }

    @Override
    public QueryResult<File> getAllFilesInFolder(long studyId, String path, QueryOptions options) throws CatalogDBException {
        long startTime = startQuery();
        Bson query = Filters.and(Filters.eq(PRIVATE_STUDY_ID, studyId), Filters.regex("path", "^" + path + "[^/]+/?$"));
        List<File> fileResults = fileCollection.find(query, fileConverter, null).getResult();
        return endQuery("Get all files", startTime, fileResults);
    }

    @Override
    public QueryResult<FileAcl> getFileAcl(long fileId, List<String> members) throws CatalogDBException {
        long startTime = startQuery();
        checkFileId(fileId);
        Bson match = Aggregates.match(Filters.eq(PRIVATE_ID, fileId));
        Bson unwind = Aggregates.unwind("$" + QueryParams.ACLS.key());
        Bson match2 = Aggregates.match(Filters.in(QueryParams.ACLS_USERS.key(), members));
        Bson project = Aggregates.project(Projections.include(QueryParams.ID.key(), QueryParams.ACLS.key()));

        List<FileAcl> fileAcl = null;
        QueryResult<Document> aggregate = fileCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);
        File file = fileConverter.convertToDataModelType(aggregate.first());

        if (file != null) {
            fileAcl = file.getAcls();
        }

        return endQuery("get file Acl", startTime, fileAcl);
    }

    @Override
    public QueryResult<FileAcl> setFileAcl(long fileId, FileAcl acl, boolean override) throws CatalogDBException {
        long startTime = startQuery();

        checkFileId(fileId);
        long studyId = getStudyIdByFileId(fileId);
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
                QueryResult<FileAcl> fileAcl = getFileAcl(fileId, entry.getValue());
                if (fileAcl.getNumResults() > 0) {
                    throw new CatalogDBException("Error when adding permissions in file. At least one user in " + entry.getKey()
                            + " has already defined permissions for file " + fileId);
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
        QueryResult<FileAcl> fileAcls = getFileAcl(fileId, acl.getUsers());
        if (fileAcls.getNumResults() > 0 && override) {
            Set<String> usersSet = new HashSet<>(acl.getUsers().size());
            usersSet.addAll(acl.getUsers().stream().collect(Collectors.toList()));

            List<String> usersToOverride = new ArrayList<>();
            for (FileAcl fileAcl : fileAcls.getResult()) {
                for (String member : fileAcl.getUsers()) {
                    if (usersSet.contains(member)) {
                        // Add the user to the list of users that will be taken out from the Acls.
                        usersToOverride.add(member);
                    }
                }
            }

            // Now we remove the old permissions set for the users that already existed so the permissions are overriden by the new ones.
            unsetFileAcl(fileId, usersToOverride, Collections.emptyList());
        } else if (fileAcls.getNumResults() > 0 && !override) {
            throw new CatalogDBException("setFileAcl: " + fileAcls.getNumResults() + " of the members already had an Acl set. If you "
                    + "still want to set the Acls for them and remove the old one, please use the override parameter.");
        }

        // Append the users to the existing acl.
        List<String> permissions = acl.getPermissions().stream().map(FileAcl.FilePermissions::name).collect(Collectors.toList());

        // Check if the permissions found on acl already exist on file id
        Document queryDocument = new Document(PRIVATE_ID, fileId);
        if (permissions.size() > 0) {
            queryDocument.append(QueryParams.ACLS_PERMISSIONS.key(), new Document("$size", permissions.size()).append("$all", permissions));
        } else {
            queryDocument.append(QueryParams.ACLS_PERMISSIONS.key(), new Document("$size", 0));
        }

        Bson update;
        if (fileCollection.count(queryDocument).first() > 0) {
            // Append the users to the existing acl.
            update = new Document("$addToSet", new Document("acls.$.users", new Document("$each", acl.getUsers())));
        } else {
            queryDocument = new Document(PRIVATE_ID, fileId);
            // Push the new acl to the list of acls.
            update = new Document("$push", new Document(QueryParams.ACLS.key(), getMongoDBDocument(acl, "FileAcl")));

        }

        QueryResult<UpdateResult> updateResult = fileCollection.update(queryDocument, update, null);
        if (updateResult.first().getModifiedCount() == 0) {
            throw new CatalogDBException("setFileAcl: An error occurred when trying to share file " + fileId
                    + " with other members.");
        }

        QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, QueryParams.ACLS.key());
        File file = fileConverter.convertToDataModelType(fileCollection.find(queryDocument, queryOptions).first());

        return endQuery("setFileAcl", startTime, file.getAcls());
    }

    @Override
    public void unsetFileAcl(long fileId, List<String> members, List<String> permissions) throws CatalogDBException {

        checkFileId(fileId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, getStudyIdByFileId(fileId), members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_ID, fileId)
                    .append("acls", new Document("$elemMatch", new Document("users", member)));
            Bson update;
            if (permissions.size() == 0) {
                update = new Document("$pull", new Document("acls.$.users", member));
            } else {
                update = new Document("$pull", new Document("acls.$.permissions", new Document("$in", permissions)));
            }
            QueryResult<UpdateResult> updateResult = fileCollection.update(query, update, null);
            if (updateResult.first().getModifiedCount() == 0) {
                throw new CatalogDBException("unsetFileAcl: An error occurred when trying to remove the ACL in file " + fileId
                        + " for member " + member + ".");
            }
        }

        // Remove possible fileAcls that might have permissions defined but no users
        Bson queryBson = new Document(QueryParams.ID.key(), fileId)
                .append(QueryParams.ACLS_USERS.key(),
                        new Document("$exists", true).append("$eq", Collections.emptyList()));
        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
        fileCollection.update(queryBson, update, null);
    }

    @Override
    public void unsetFileAclsInStudy(long studyId, List<String> members) throws CatalogDBException {
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
        // Check that all the members (users) are correct and exist.
        checkMembers(dbAdaptorFactory, studyId, members);

        // Remove the permissions the members might have had
        for (String member : members) {
            Document query = new Document(PRIVATE_STUDY_ID, studyId)
                    .append("acls", new Document("$elemMatch", new Document("users", member)));
            Bson update = new Document("$pull", new Document("acls.$.users", member));
            fileCollection.update(query, update, new QueryOptions(MongoDBCollection.MULTI, true));
        }

        // Remove possible FileAcls that might have permissions defined but no users
        Bson queryBson = new Document(PRIVATE_STUDY_ID, studyId)
                .append(CatalogSampleDBAdaptor.QueryParams.ACLS_USERS.key(),
                        new Document("$exists", true).append("$eq", Collections.emptyList()));
        Bson update = new Document("$pull", new Document("acls", new Document("users", Collections.emptyList())));
        fileCollection.update(queryBson, update, new QueryOptions(MongoDBCollection.MULTI, true));
    }

    @Override
    public QueryResult<Map<String, Map<String, FileAcl>>> getFilesAcl(long studyId, List<String> filePaths, List<String> userIds)
            throws CatalogDBException {

        long startTime = startQuery();
        dbAdaptorFactory.getCatalogStudyDBAdaptor().checkStudyId(studyId);
//        DBObject match = new BasicDBObject("$match", new BasicDBObject(PRIVATE_STUDY_ID, studyId).append("path", new BasicDBObject("$in",
//                filePaths)));
//        DBObject unwind = new BasicDBObject("$unwind", "$acl");
//        DBObject match2 = new BasicDBObject("$match", new BasicDBObject("acl.userId", new BasicDBObject("$in", userIds)));
//        DBObject project = new BasicDBObject("$project", new BasicDBObject("path", 1).append("id", 1).append("acl", 1));
//        QueryResult<DBObject> result = fileCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);

        Bson match = Aggregates.match(Filters.and(Filters.eq(PRIVATE_STUDY_ID, studyId), Filters.in(QueryParams.PATH.key(), filePaths)));
        Bson unwind = Aggregates.unwind("$" + QueryParams.ACLS.key());
        Bson match2 = Aggregates.match(Filters.in(QueryParams.ACLS_USERS.key(), userIds));
        Bson project = Aggregates.project(Projections.include(QueryParams.ID.key(), QueryParams.PATH.key(), QueryParams.ACLS.key()));
        QueryResult<Document> result = fileCollection.aggregate(Arrays.asList(match, unwind, match2, project), null);

        List<File> files = parseFiles(result);
        Map<String, Map<String, FileAcl>> pathAclMap = new HashMap<>();
        for (File file : files) {
//            AclEntry acl = file.getAcls().get(0);
            for (FileAcl acl : file.getAcls()) {
                if (pathAclMap.containsKey(file.getPath())) {
                    Map<String, FileAcl> userAclMap = pathAclMap.get(file.getPath());
                    for (String user : acl.getUsers()) {
                        if (!userAclMap.containsKey(user)) {
                            userAclMap.put(user, acl);
                        }
                    }
                } else {
                    HashMap<String, FileAcl> userAclMap = new HashMap<>();
                    for (String user : acl.getUsers()) {
                        userAclMap.put(user, acl);
                    }
                    pathAclMap.put(file.getPath(), userAclMap);
                }
            }
        }
//        Map<String, Acl> pathAclMap = files.stream().collect(Collectors.toMap(File::getPath, file -> file.getAcls().get(0)));
        logger.debug("getFilesAcl for {} paths and {} users, dbTime: {} ", filePaths.size(), userIds.size(), result.getDbTime());
        return endQuery("getFilesAcl", startTime, Collections.singletonList(pathAclMap));
    }

    @Override
    public long getStudyIdByFileId(long fileId) throws CatalogDBException {
        QueryResult queryResult = nativeGet(new Query(QueryParams.ID.key(), fileId), null);

        if (!queryResult.getResult().isEmpty()) {
            return (long) ((Document) queryResult.getResult().get(0)).get(PRIVATE_STUDY_ID);
        } else {
            throw CatalogDBException.idNotFound("File", fileId);
        }
    }

    @Override
    public List<Long> getStudyIdsByFileIds(String fileIds) throws CatalogDBException {
        Bson query = parseQuery(new Query(QueryParams.ID.key(), fileIds));
        return fileCollection.distinct(PRIVATE_STUDY_ID, query, Long.class).getResult();
    }

    @Override
    public String getFileOwnerId(long fileId) throws CatalogDBException {
        return getFile(fileId, null).getResult().get(0).getOwnerId();
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        QueryOptions qOptions;
        if (options != null) {
            qOptions = options;
        } else {
            qOptions = new QueryOptions();
        }
        qOptions = filterOptions(qOptions, FILTER_ROUTE_FILES);

        return fileCollection.find(bson, qOptions);
    }

    @Override
    public QueryResult<File> update(long id, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();
        checkFileId(id);
        Query query = new Query(QueryParams.ID.key(), id);
        update(query, parameters);
        return endQuery("Update file", startTime, get(query, null));
    }

    @Override
    public QueryResult<Long> update(Query query, ObjectMap parameters) throws CatalogDBException {
        long startTime = startQuery();

        // If the user wants to change the diskUsages of the file(s), we first make a query to obtain the old values.
        QueryResult fileQueryResult = null;
        if (parameters.containsKey(QueryParams.DISK_USAGE.key())) {
            QueryOptions queryOptions = new QueryOptions(QueryOptions.INCLUDE, Arrays.asList(
                    FILTER_ROUTE_FILES + QueryParams.DISK_USAGE.key(), FILTER_ROUTE_FILES + PRIVATE_STUDY_ID));
            fileQueryResult = nativeGet(query, queryOptions);
        }

        // We perform the update.
        Bson queryBson = parseQuery(query);
        Map<String, Object> fileParameters = new HashMap<>();

        String[] acceptedParams = {
                QueryParams.DESCRIPTION.key(), QueryParams.URI.key(), QueryParams.CREATION_DATE.key(),
                QueryParams.MODIFICATION_DATE.key(), };
        // Fixme: Add "name", "path" and "ownerId" at some point. At the moment, it would lead to inconsistencies.
        filterStringParams(parameters, fileParameters, acceptedParams);

        Map<String, Class<? extends Enum>> acceptedEnums = new HashMap<>();
        acceptedEnums.put(QueryParams.TYPE.key(), File.Type.class);
        acceptedEnums.put(QueryParams.FORMAT.key(), File.Format.class);
        acceptedEnums.put(QueryParams.BIOFORMAT.key(), File.Bioformat.class);
       // acceptedEnums.put("fileStatus", File.FileStatusEnum.class);
        if (parameters.containsKey(QueryParams.STATUS_STATUS.key())) {
            fileParameters.put(QueryParams.STATUS_STATUS.key(), parameters.get(QueryParams.STATUS_STATUS.key()));
            fileParameters.put(QueryParams.STATUS_DATE.key(), TimeUtils.getTime());
        }
        try {
            filterEnumParams(parameters, fileParameters, acceptedEnums);
        } catch (CatalogDBException e) {
            e.printStackTrace();
            throw new CatalogDBException("File update: It was impossible updating the files. " + e.getMessage());
        }

        String[] acceptedLongParams = {QueryParams.DISK_USAGE.key()};
        filterLongParams(parameters, fileParameters, acceptedLongParams);

        String[] acceptedIntParams = {QueryParams.JOB_ID.key()};
        // Fixme: Add "experiment_id" ?
        filterIntParams(parameters, fileParameters, acceptedIntParams);
        // Check if the job exists.
        if (parameters.containsKey(QueryParams.JOB_ID.key())) {
            if (!this.dbAdaptorFactory.getCatalogJobDBAdaptor().jobExists(parameters.getInt(QueryParams.JOB_ID.key()))) {
                throw CatalogDBException.idNotFound("Job", parameters.getInt(QueryParams.JOB_ID.key()));
            }
        }

        String[] acceptedIntegerListParams = {QueryParams.SAMPLE_IDS.key()};
        filterIntegerListParams(parameters, fileParameters, acceptedIntegerListParams);
        // Check if the sample ids exist.
        if (parameters.containsKey(QueryParams.SAMPLE_IDS.key())) {
            for (Integer sampleId : parameters.getAsIntegerList(QueryParams.SAMPLE_IDS.key())) {
                if (!dbAdaptorFactory.getCatalogSampleDBAdaptor().sampleExists(sampleId)) {
                    throw CatalogDBException.idNotFound("Sample", sampleId);
                }
            }
        }

        String[] acceptedMapParams = {QueryParams.ATTRIBUTES.key(), QueryParams.STATS.key()};
        filterMapParams(parameters, fileParameters, acceptedMapParams);
        // Fixme: Attributes and stats can be also parsed to numeric or boolean

        String[] acceptedObjectParams = {"index"};
        filterObjectParams(parameters, fileParameters, acceptedObjectParams);

        if (!fileParameters.isEmpty()) {
            QueryResult<UpdateResult> update = fileCollection.update(queryBson, new Document("$set", fileParameters), null);

            // If the diskUsage of some of the files have been changed, notify to the correspondent study
            if (fileQueryResult != null) {
                long newDiskUsage = parameters.getLong(QueryParams.DISK_USAGE.key());
                for (Document file : (List<Document>) fileQueryResult.getResult()) {
                    long difDiskUsage = newDiskUsage - Long.parseLong(file.get(QueryParams.DISK_USAGE.key()).toString());
                    long studyId = (long) file.get(PRIVATE_STUDY_ID);
                    dbAdaptorFactory.getCatalogStudyDBAdaptor().updateDiskUsage(studyId, difDiskUsage);
                }
            }
            return endQuery("Update file", startTime, Collections.singletonList(update.getResult().get(0).getModifiedCount()));
        }
        return endQuery("Update file", startTime, Collections.singletonList(0L));
    }

    @Override
    public QueryResult<File> renameFile(long fileId, String filePath, QueryOptions options)
            throws CatalogDBException {
        long startTime = startQuery();

        checkFileId(fileId);

        Path path = Paths.get(filePath);
        String fileName = path.getFileName().toString();

        Document fileDoc = (Document) nativeGet(new Query(QueryParams.ID.key(), fileId), null).getResult().get(0);
        File file = fileConverter.convertToDataModelType(fileDoc);

        long studyId = (long) fileDoc.get(PRIVATE_STUDY_ID);
        long collisionFileId = getFileId(studyId, filePath);
        if (collisionFileId >= 0) {
            throw new CatalogDBException("Can not rename: " + filePath + " already exists");
        }

        if (file.getType().equals(File.Type.FOLDER)) {  // recursive over the files inside folder
            QueryResult<File> allFilesInFolder = getAllFilesInFolder(studyId, file.getPath(), null);
            String oldPath = file.getPath();
            filePath += filePath.endsWith("/") ? "" : "/";
            for (File subFile : allFilesInFolder.getResult()) {
                String replacedPath = subFile.getPath().replaceFirst(oldPath, filePath);
                renameFile(subFile.getId(), replacedPath, null); // first part of the path in the subfiles 3
            }
        }

        Document query = new Document(PRIVATE_ID, fileId);
        Document set = new Document("$set", new Document("name", fileName).append("path", filePath));
        QueryResult<UpdateResult> update = fileCollection.update(query, set, null);
        if (update.getResult().isEmpty() || update.getResult().get(0).getModifiedCount() == 0) {
            throw CatalogDBException.idNotFound("File", fileId);
        }
        return endQuery("Rename file", startTime, getFile(fileId, options));
    }

    @Override
    public QueryResult<File> delete(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkFileId(id);
        // Check the file is active
        Query query = new Query(QueryParams.ID.key(), id).append(QueryParams.STATUS_STATUS.key(), "!=" + File.FileStatus.DELETED + ";!="
                + File.FileStatus.REMOVED);
        if (count(query).first() == 0) {
            query.put(QueryParams.STATUS_STATUS.key(), Status.DELETED + "," + Status.REMOVED);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, QueryParams.STATUS_STATUS.key());
            File file = get(query, options).first();
            throw new CatalogDBException("The file {" + id + "} was already " + file.getStatus().getStatus());
        }

        // If we don't find the force parameter, we check first if the file could be deleted.
        if (!queryOptions.containsKey(FORCE) || !queryOptions.getBoolean(FORCE)) {
            checkCanDelete(id);
        }

        if (queryOptions.containsKey(FORCE) && queryOptions.getBoolean(FORCE)) {
            deleteReferencesToFile(id);
        }

        // Change the status of the project to deleted
        setStatus(id, Status.DELETED);

        query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_STATUS.key(), Status.DELETED);

        return endQuery("Delete file", startTime, get(query, queryOptions));
    }

    @Override
    public QueryResult<Long> delete(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_STATUS.key(), Status.READY);
        QueryResult<File> fileQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
        for (File file : fileQueryResult.getResult()) {
            delete(file.getId(), queryOptions);
        }
        return endQuery("Delete file", startTime, Collections.singletonList(fileQueryResult.getNumTotalResults()));
    }

    @Override
    public QueryResult<File> remove(long id, QueryOptions queryOptions) throws CatalogDBException {
        throw new UnsupportedOperationException("Remove not yet implemented.");
    }

    @Override
    public QueryResult<Long> remove(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.append(QueryParams.STATUS_STATUS.key(), Status.READY);
        QueryResult<File> fileQueryResult = get(query, new QueryOptions(MongoDBCollection.INCLUDE, QueryParams.ID.key()));
        for (File file : fileQueryResult.getResult()) {
            remove(file.getId(), queryOptions);
        }
        return endQuery("Remove file", startTime, Collections.singletonList(fileQueryResult.getNumTotalResults()));
    }

    @Override
    public QueryResult<Long> restore(Query query, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();
        query.put(QueryParams.STATUS_STATUS.key(), Status.DELETED);
        return endQuery("Restore files", startTime, setStatus(query, File.FileStatus.READY));
    }

    @Override
    public QueryResult<File> restore(long id, QueryOptions queryOptions) throws CatalogDBException {
        long startTime = startQuery();

        checkFileId(id);
        // Check if the cohort is active
        Query query = new Query(QueryParams.ID.key(), id)
                .append(QueryParams.STATUS_STATUS.key(), Status.DELETED);
        if (count(query).first() == 0) {
            throw new CatalogDBException("The file {" + id + "} is not deleted");
        }

        // Change the status of the cohort to deleted
        setStatus(id, File.FileStatus.READY);
        query = new Query(QueryParams.ID.key(), id);

        return endQuery("Restore file", startTime, get(query, null));
    }

    public QueryResult<File> clean(int id) throws CatalogDBException {
        long startTime = startQuery();
        QueryResult<File> file = getFile(id, new QueryOptions());
        QueryResult<DeleteResult> deleteResult = fileCollection.remove(new Document(QueryParams.ID.key(), id), null);
        if (deleteResult.getNumResults() == 1) {
            return endQuery("Delete file", startTime, file);
        } else {
            throw CatalogDBException.deleteError("File");
        }
    }

    public void checkFileNotInUse(long fileId) throws CatalogDBException {
        Query query = new Query(CatalogJobDBAdaptor.QueryParams.INPUT.key(), fileId);
        QueryResult<Long> count = dbAdaptorFactory.getCatalogJobDBAdaptor().count(query);

        if (count.first() > 0) {
            throw CatalogDBException.fileInUse(fileId, count.getNumResults());
        }

    }

    @Override
    public QueryResult<Long> count(Query query) throws CatalogDBException {
        Bson bson = parseQuery(query);
        return fileCollection.count(bson);
    }

    @Override
    public QueryResult distinct(Query query, String field) throws CatalogDBException {
        Bson bsonDocument = parseQuery(query);
        return fileCollection.distinct(field, bsonDocument);
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public CatalogDBIterator<File> iterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = fileCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator, fileConverter);
    }

    @Override
    public CatalogDBIterator nativeIterator(Query query, QueryOptions options) throws CatalogDBException {
        Bson bson = parseQuery(query);
        MongoCursor<Document> iterator = fileCollection.nativeQuery().find(bson, options).iterator();
        return new CatalogMongoDBIterator<>(iterator);
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return rank(fileCollection, bsonQuery, field, "name", numResults, asc);
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(fileCollection, bsonQuery, field, "name", options);
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) throws CatalogDBException {
        Bson bsonQuery = parseQuery(query);
        return groupBy(fileCollection, bsonQuery, fields, "name", options);
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) throws CatalogDBException {
        Objects.requireNonNull(action);
        CatalogDBIterator<File> catalogDBIterator = iterator(query, options);
        while (catalogDBIterator.hasNext()) {
            action.accept(catalogDBIterator.next());
        }
        catalogDBIterator.close();
    }

    @Override
    @Deprecated
    public QueryResult<AclEntry> setFileAcl(long fileId, AclEntry newAcl) throws CatalogDBException {
        return null;
//        long startTime = startQuery();
//        String userId = newAcl.getUserId();
//
//        checkAclUserId(dbAdaptorFactory, userId, getStudyIdByFileId(fileId));
//
//        //DBObject newAclObject = getDbObject(newAcl, "ACL");
//        Document newAclObject = getMongoDBDocument(newAcl, "ACL");
//
//        List<AclEntry> aclList = getFileAcl(fileId, userId).getResult();
//        Bson query;
//        Bson update;
//        if (aclList.isEmpty()) {  // there is no acl for that user in that file. push
//            //query = new BasicDBObject(PRIVATE_ID, fileId);
//            // update = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
//            query = Filters.eq(PRIVATE_ID, fileId);
//            update = Updates.push("acl", newAclObject);
//        } else {    // there is already another ACL: overwrite
//            /*query = BasicDBObjectBuilder
//                    .start(PRIVATE_ID, fileId)
//                    .append("acl.userId", userId).get();
//            update = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));*/
//            query = Filters.and(Filters.eq(PRIVATE_ID, fileId), Filters.eq("acl.userId", userId));
//            update = Updates.set("acl.$", newAclObject);
//        }
//        QueryResult<UpdateResult> queryResult = fileCollection.update(query, update, null);
//        if (queryResult.first().getModifiedCount() != 1) {
//            throw CatalogDBException.idNotFound("File", fileId);
//        }
//
//        return endQuery("setFileAcl", startTime, getFileAcl(fileId, userId));
    }

    @Override
    @Deprecated
    public QueryResult<AclEntry> unsetFileAcl(long fileId, String userId) throws CatalogDBException {
        return null;
//        long startTime = startQuery();
//
//        QueryResult<AclEntry> fileAcl = getFileAcl(fileId, userId);
////        DBObject query = new BasicDBObject(PRIVATE_ID, fileId);
////        DBObject update = new BasicDBObject("$pull", new BasicDBObject("acl", new BasicDBObject("userId", userId)));
////        QueryResult queryResult = fileCollection.update(query, update, null);
//
//        Bson query = Filters.eq(PRIVATE_ID, fileId);
//        Bson update = Updates.pull("acl", new Document("userId", userId));
//        QueryResult<UpdateResult> queryResult = fileCollection.update(query, update, null);
//
//        return endQuery("unsetFileAcl", startTime, fileAcl);
    }


    // Auxiliar methods

    private Bson parseQuery(Query query) throws CatalogDBException {
        List<Bson> andBsonList = new ArrayList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            QueryParams queryParam = QueryParams.getParam(entry.getKey()) != null ? QueryParams.getParam(entry.getKey())
                    : QueryParams.getParam(key);
            try {
                switch (queryParam) {
                    case ID:
                        addOrQuery(PRIVATE_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case STUDY_ID:
                        addOrQuery(PRIVATE_STUDY_ID, queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                    case DIRECTORY:
                        // We add the regex in order to look for all the files under the given directory
                        String value = (String) query.get(queryParam.key());
                        String regExPath = "~^" + value + "[^/]+/?$";
                        Query pathQuery = new Query(QueryParams.PATH.key(), regExPath);
                        addAutoOrQuery(QueryParams.PATH.key(), QueryParams.PATH.key(), pathQuery, QueryParams.PATH.type(), andBsonList);
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
                    default:
                        addAutoOrQuery(queryParam.key(), queryParam.key(), query, queryParam.type(), andBsonList);
                        break;
                }
            } catch (Exception e) {
                logger.error("Error with " + entry.getKey() + " " + entry.getValue());
                throw new CatalogDBException(e);
            }
        }

        if (andBsonList.size() > 0) {
            return Filters.and(andBsonList);
        } else {
            return new Document();
        }
    }

    private boolean filePathExists(long studyId, String path) {
        Document query = new Document(PRIVATE_STUDY_ID, studyId).append(QueryParams.PATH.key(), path);
        QueryResult<Long> count = fileCollection.count(query);
        return count.getResult().get(0) != 0;
    }

    // TODO: Check these deprecated methods and get rid of them at some point

    @Deprecated
    @Override
    public QueryResult<File> deleteFile(long fileId) throws CatalogDBException {
        throw new UnsupportedOperationException("Deprecated method. Use delete instead.");
        //return delete(fileId);
    }

    @Deprecated
    @Override
    public QueryResult<File> modifyFile(long fileId, ObjectMap parameters) throws CatalogDBException {
        throw new UnsupportedOperationException("Deprecated method. Use update instead.");
        //return update(fileId, parameters);
    }

    @Deprecated
    public QueryResult<File> getAllFiles(Query query, QueryOptions options) throws CatalogDBException {
        throw new UnsupportedOperationException("Deprecated method. Use get instead.");
/*
        long startTime = startQuery();

        List<DBObject> mongoQueryList = new LinkedList<>();

        for (Map.Entry<String, Object> entry : query.entrySet()) {
            String key = entry.getKey().split("\\.")[0];
            try {
                if (isDataStoreOption(key) || isOtherKnownOption(key)) {
                    continue;   //Exclude DataStore options
                }
                FileFilterOption option = FileFilterOption.valueOf(key);
                switch (option) {
                    case id:
                        addCompQueryFilter(option, option.name(), query, PRIVATE_ID, mongoQueryList);
                        break;
                    case studyId:
                        addCompQueryFilter(option, option.name(), query, PRIVATE_STUDY_ID, mongoQueryList);
                        break;
                    case directory:
                        mongoQueryList.add(new BasicDBObject("path", new BasicDBObject("$regex", "^" + query.getString("directory") +
                                "[^/]+/?$")));
                        break;
                    default:
                        String queryKey = entry.getKey().replaceFirst(option.name(), option.getKey());
                        addCompQueryFilter(option, entry.getKey(), query, queryKey, mongoQueryList);
                        break;
                    case minSize:
                        mongoQueryList.add(new BasicDBObject("size", new BasicDBObject("$gt", query.getInt("minSize"))));
                        break;
                    case maxSize:
                        mongoQueryList.add(new BasicDBObject("size", new BasicDBObject("$lt", query.getInt("maxSize"))));
                        break;
                    case like:
                        mongoQueryList.add(new BasicDBObject("name", new BasicDBObject("$regex", query.getString("like"))));
                        break;
                    case startsWith:
                        mongoQueryList.add(new BasicDBObject("name", new BasicDBObject("$regex", "^" + query.getString("startsWith"))));
                        break;
                    case startDate:
                        mongoQueryList.add(new BasicDBObject("creationDate", new BasicDBObject("$lt", query.getString("startDate"))));
                        break;
                    case endDate:
                        mongoQueryList.add(new BasicDBObject("creationDate", new BasicDBObject("$gt", query.getString("endDate"))));
                        break;
                }
            } catch (IllegalArgumentException e) {
                throw new CatalogDBException(e);
            }
        }

        BasicDBObject mongoQuery = new BasicDBObject("$and", mongoQueryList);
        QueryOptions queryOptions = filterOptions(options, FILTER_ROUTE_FILES);
//        QueryResult<DBObject> queryResult = fileCollection.find(mongoQuery, null, File.class, queryOptions);
        QueryResult<File> queryResult = fileCollection.find(mongoQuery, null, new ComplexTypeConverter<File, DBObject>() {
            @Override
            public File convertToDataModelType(DBObject object) {
                try {
                    return getObjectReader(File.class).readValue(restoreDotsInKeys(object).toString());
                } catch (IOException e) {
                    return null;
                }
            }

            @Override
            public DBObject convertToStorageType(File object) {
                return null;
            }
        }, queryOptions);
        logger.debug("File search: query : {}, project: {}, dbTime: {}", mongoQuery, queryOptions == null ? "" : queryOptions.toJson(),
                queryResult.getDbTime());
//        List<File> files = parseFiles(queryResult);

        return endQuery("Search File", startTime, queryResult);
        */
    }

    QueryResult<File> setStatus(long fileId, String status) throws CatalogDBException {
        return update(fileId, new ObjectMap(QueryParams.STATUS_STATUS.key(), status));
    }

    QueryResult<Long> setStatus(Query query, String status) throws CatalogDBException {
        return update(query, new ObjectMap(QueryParams.STATUS_STATUS.key(), status));
    }

    /**
     * Checks whether the fileId is being referred on any other document.
     *
     * @param fileId file id.
     * @throws CatalogDBException when the fileId is being used as input of any job or dataset.
     */
    private void checkCanDelete(long fileId) throws CatalogDBException {

        // Check if the file is being used as input of any job
        Query query = new Query(CatalogJobDBAdaptor.QueryParams.INPUT.key(), fileId);
        Long count = dbAdaptorFactory.getCatalogJobDBAdaptor().count(query).first();
        if ((count > 0)) {
            throw new CatalogDBException("The file " + fileId + " cannot be deleted/removed because it is being used as input of "
                    + count + " jobs. Please, consider using the parameter force if you are sure you want to delete it.");
        }

        query = new Query(CatalogDatasetDBAdaptor.QueryParams.FILES.key(), fileId);
        count = dbAdaptorFactory.getCatalogDatasetDBAdaptor().count(query).first();
        if ((count > 0)) {
            throw new CatalogDBException("The file " + fileId + " cannot be deleted/removed because it is part of "
                    + count + " dataset(s). Please, consider using the parameter force if you are sure you want to delete it.");
        }

    }

    /**
     * Remove the references from active documents that are pointing to the current fileId.
     *
     * @param fileId file Id.
     * @throws CatalogDBException when there is any kind of error.
     */
    private void deleteReferencesToFile(long fileId) throws CatalogDBException {
        // Remove references from datasets
        Query query = new Query(CatalogDatasetDBAdaptor.QueryParams.FILES.key(), fileId);
        QueryResult<Long> result = dbAdaptorFactory.getCatalogDatasetDBAdaptor()
                .extractFilesFromDatasets(query, Collections.singletonList(fileId));
        logger.debug("FileId {} extracted from {} datasets", fileId, result.first());

        // Remove references from jobs
        result = dbAdaptorFactory.getCatalogJobDBAdaptor().extractFilesFromJobs(Collections.singletonList(fileId));
        logger.debug("FileId {} extracted from {} jobs", fileId, result.first());
    }

    public QueryResult<Long> extractSampleFromFiles(Query query, List<Long> sampleIds) throws CatalogDBException {
        long startTime = startQuery();
        Bson bsonQuery = parseQuery(query);
        Bson update = new Document("$pull", new Document(QueryParams.SAMPLE_IDS.key(), new Document("$in", sampleIds)));
        QueryOptions multi = new QueryOptions(MongoDBCollection.MULTI, true);
        QueryResult<UpdateResult> updateQueryResult = fileCollection.update(bsonQuery, update, multi);
        return endQuery("Extract samples from files", startTime, Collections.singletonList(updateQueryResult.first().getModifiedCount()));
    }
}
