package org.opencb.opencga.catalog.db.mongodb;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.*;
import com.mongodb.util.JSON;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.datastore.mongodb.MongoDBCollection;
import org.opencb.opencga.catalog.beans.Acl;
import org.opencb.opencga.catalog.beans.File;
import org.opencb.opencga.catalog.db.CatalogManagerException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by imedina on 21/11/14.
 */
public class FileMongoDBAdaptor {

    private MongoDBCollection fileCollection;

    private static final String FILE_COLLECTION = "file";

    public FileMongoDBAdaptor(MongoDBCollection mongoDBCollection) {
        this.fileCollection = mongoDBCollection;
    }

//    QueryResult<File> createFileToStudy(String userId, String projectAlias, String studyAlias, File file) throws CatalogManagerException;
//    QueryResult<File> createFileToStudy(int studyId, File file) throws CatalogManagerException;
//    QueryResult<Integer> deleteFile(String userId, String projectAlias, String studyAlias, String path) throws CatalogManagerException, IOException;
//    QueryResult<Integer> deleteFile(int studyId, String path) throws CatalogManagerException;
//    QueryResult<Integer> deleteFile(int fileId) throws CatalogManagerException;
//    QueryResult deleteFilesFromStudy(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException;
//    QueryResult deleteFilesFromStudy(int studyId, String studyAlias, String sessionId) throws CatalogManagerException;
//    int getFileId(String userId, String projectAlias, String studyAlias, String path) throws CatalogManagerException;
//    int getFileId(int studyId, String path) throws CatalogManagerException;
//    QueryResult<File> getAllFiles(int studyId) throws CatalogManagerException;
//    QueryResult<File> getAllFilesInFolder(int folderId) throws CatalogManagerException;
//    QueryResult<File> getFile(int fileId) throws CatalogManagerException;
//    QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogManagerException;
//    QueryResult setFileStatus(String userId, String projectAlias, String studyAlias, String path, String status) throws CatalogManagerException, IOException;
//    QueryResult setFileStatus(int studyId, String path, String status) throws CatalogManagerException, IOException;
//    QueryResult setFileStatus(int fileId, String status) throws CatalogManagerException, IOException;
//    QueryResult modifyFile(int fileId, ObjectMap parameters) throws CatalogManagerException;
//    QueryResult setIndexFile(int fileId, String backend, Index index) throws CatalogManagerException;
//    QueryResult<WriteResult> renameFile(int fileId, String name) throws CatalogManagerException;
//    int getStudyIdByFileId(int fileId) throws CatalogManagerException;
//    String getFileOwner(int fileId) throws CatalogManagerException;
//    QueryResult<Acl> getFileAcl(int fileId, String userId) throws CatalogManagerException;
//    QueryResult setFileAcl(int fileId, Acl newAcl) throws CatalogManagerException;
//    QueryResult<File> searchFile(QueryOptions query, QueryOptions options) throws CatalogManagerException;


//    @Override
//    public QueryResult<File> createFileToStudy(String userId, String projectAlias, String studyAlias, File file) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        int studyId = getStudyId(userId, projectAlias, studyAlias);
//        if(studyId < 0){
//            throw new CatalogManagerException("Study {alias:"+studyAlias+"} does not exists");
//        }
//        QueryResult<File> fileToStudy = createFileToStudy(studyId, file);
//        return endQuery("Create file", startTime, fileToStudy);
//    }
//
//    @Override
//    public QueryResult<File> createFileToStudy(int studyId, File file) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        String ownerId = getStudyOwnerId(studyId);
//        if(ownerId == null || ownerId.isEmpty()) {
//            throw new CatalogManagerException("StudyID " + studyId + " not found");
//        }
//        BasicDBObject query = new BasicDBObject("studyId", studyId);
//        query.put("path", file.getPath());
//        QueryResult<Long> count = fileCollection.count(query);
//        if(count.getResult().get(0) != 0){
//            throw new CatalogManagerException("File {studyId:"+ studyId + /*", name:\"" + file.getName() +*/ "\", path:\""+file.getPath()+"\"} already exists");
//        }
//
//
//        int newFileId = getNewFileId();
//        file.setId(newFileId);
//        if(file.getOwnerId() == null) {
//            file.setOwnerId(ownerId);
//        }
//        DBObject fileDBObject;
//        try {
//            fileDBObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(file));
//        } catch (JsonProcessingException e) {
//            throw new CatalogManagerException("Error parsing file", e);
//        }
//        fileDBObject.put("studyId", studyId);
//        try {
//            fileCollection.insert(fileDBObject);
//        } catch (MongoException.DuplicateKey e) {
//            throw new CatalogManagerException("File {studyId:"+ studyId + /*", name:\"" + file.getName() +*/ "\", path:\""+file.getPath()+"\"} already exists");
//        }
//
//        return endQuery("Create file", startTime, Arrays.asList(file));
//    }
//
//    /**
//     * At the moment it does not clean external references to itself.
//     */
//    @Override
//    public QueryResult<Integer> deleteFile(String userId, String projectAlias, String studyAlias, String path) throws CatalogManagerException, IOException {
//        return deleteFile(getFileId(userId, projectAlias, studyAlias, path));
//    }
//
//    @Override
//    public QueryResult<Integer> deleteFile(int studyId, String path) throws CatalogManagerException {
//        return deleteFile(getFileId(studyId, path));
//    }
//
//    @Override
//    public QueryResult<Integer> deleteFile(int fileId) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        WriteResult id = fileCollection.remove(new BasicDBObject("id", fileId)).getResult().get(0);
//        List<Integer> deletes = new LinkedList<>();
//        if(id.getN() == 0) {
//            throw new CatalogManagerException("file {id:" + fileId + "} not found");
//        } else {
//            deletes.add(id.getN());
//            return endQuery("delete file", startTime, deletes);
//        }
//    }
//
//    @Override
//    public QueryResult deleteFilesFromStudy(String userId, String projectAlias, String studyAlias, String sessionId) throws CatalogManagerException {
//        return null;
//    }
//
//    @Override
//    public QueryResult deleteFilesFromStudy(int studyId, String studyAlias, String sessionId) throws CatalogManagerException {
//        return null;
//    }
//
//    @Override
//    public int getFileId(String userId, String projectAlias, String studyAlias, String path) throws CatalogManagerException {
//        int studyId = getStudyId(userId, projectAlias, studyAlias);
//        return getFileId(studyId, path);
//    }
//
//    @Override
//    public int getFileId(int studyId, String path) throws CatalogManagerException {
//
//        DBObject query = BasicDBObjectBuilder
//                .start("studyId", studyId)
//                .append("path", path).get();
//        BasicDBObject fields = new BasicDBObject("id", true);
//        QueryResult queryResult = fileCollection.find(query, null, null, fields);
//        File file = parseFile(queryResult);
//        if(file != null) {
//            return file.getId();
//        } else {
//            return -1;
//        }
//    }
//
//    @Override
//    public QueryResult<File> getAllFiles(int studyId) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        QueryResult queryResult = fileCollection.find( new BasicDBObject("studyId", studyId), null, null, null);
//        List<File> files = parseFiles(queryResult);
//
//        return endQuery("Get all files", startTime, files);
//    }
//
//    @Override
//    public QueryResult<File> getAllFilesInFolder(int folderId) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        QueryResult<DBObject> folderResult = fileCollection.find( new BasicDBObject("id", folderId), null, null, null);
//
//        File folder = parseFile(folderResult);
//        if (!folder.getType().equals(File.TYPE_FOLDER)) {
//            throw new CatalogManagerException("File {id:" + folderId + ", path:'" + folder.getPath() + "'} is not a folder.");
//        }
//        Object studyId = folderResult.getResult().get(0).get("studyId");
//
//        BasicDBObject query = new BasicDBObject("studyId", studyId);
//        query.put("path", new BasicDBObject("$regex", "^" + folder.getPath() + "[^/]+/?$"));
//        QueryResult filesResult = fileCollection.find(query, null, null, null);
//        List<File> files = parseFiles(filesResult);
//
//        return endQuery("Get all files", startTime, files);
//    }
//
//
//    @Override
//    public QueryResult<File> getFile(int fileId) throws CatalogManagerException {
//        return getFile(fileId, null);
//    }
//
//    @Override
//    public QueryResult<File> getFile(int fileId, QueryOptions options) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        QueryResult queryResult = fileCollection.find( new BasicDBObject("id", fileId), options);
//
//        File file = parseFile(queryResult);
//        if(file != null) {
//            return endQuery("Get file", startTime, Arrays.asList(file));
//        } else {
//            throw new CatalogManagerException("File {id:"+fileId+"} not found");
//        }
//    }
//
//    @Override
//    public QueryResult setFileStatus(String userId, String projectAlias, String studyAlias, String path, String status) throws CatalogManagerException, IOException {
//        int fileId = getFileId(userId, projectAlias, studyAlias, path);
//        return setFileStatus(fileId, status);
//    }
//
//    @Override
//    public QueryResult setFileStatus(int studyId, String path, String status) throws CatalogManagerException, IOException {
//        int fileId = getFileId(studyId, path);
//        return setFileStatus(fileId, status);
//    }
//
//    @Override
//    public QueryResult setFileStatus(int fileId, String status) throws CatalogManagerException, IOException {
//        long startTime = startQuery();
////        BasicDBObject query = new BasicDBObject("id", fileId);
////        BasicDBObject updates = new BasicDBObject("$set",
////                new BasicDBObject("status", status));
////        QueryResult<WriteResult> update = fileCollection.update(query, updates, false, false);
////        if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
////            throw new CatalogManagerException("File {id:"+fileId+"} not found");
////        }
////        return endQuery("Set file status", startTime);
//        return endQuery("Set file status", startTime, modifyFile(fileId, new ObjectMap("status", status)));
//    }
//
//    @Override
//    public QueryResult modifyFile(int fileId, ObjectMap parameters) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        Map<String, Object> fileParameters = new HashMap<>();
//
//        String[] acceptedParams = {"type", "format", "bioformat", "uriScheme", "description", "status"};
//        for (String s : acceptedParams) {
//            if(parameters.containsKey(s)) {
//                fileParameters.put(s, parameters.getString(s));
//            }
//        }
//
//        String[] acceptedLongParams = {"diskUsage"};
//        for (String s : acceptedLongParams) {
//            if(parameters.containsKey(s)) {
//                Object value = parameters.get(s);    //TODO: Add "getLong" to "ObjectMap"
//                if(value instanceof Long) {
//                    fileParameters.put(s, value);
//                }
//            }
//        }
//
//        Map<String, Object> attributes = parameters.getMap("attributes");
//        if(attributes != null) {
//            for (Map.Entry<String, Object> entry : attributes.entrySet()) {
//                fileParameters.put("attributes." + entry.getKey(), entry.getValue());
//            }
//        }
//        Map<String, Object> stats = parameters.getMap("stats");
//        if(stats != null) {
//            for (Map.Entry<String, Object> entry : stats.entrySet()) {
//                fileParameters.put("stats." + entry.getKey(), entry.getValue());
//            }
//        }
//
//        if(!fileParameters.isEmpty()) {
//            QueryResult<WriteResult> update = fileCollection.update(new BasicDBObject("id", fileId), new BasicDBObject("$set", fileParameters), false, false);
//            if(update.getResult().isEmpty() || update.getResult().get(0).getN() == 0){
//                throw new CatalogManagerException("File {id:"+fileId+"} not found");
//            }
//        }
//
//        return endQuery("Modify file", startTime);
//    }
//
//    @Override
//    public QueryResult setIndexFile(int fileId, String backend, Index index) throws CatalogManagerException {
//        long startTime = startQuery();
//
//
//        fileCollection.update(
//                new BasicDBObject("id", fileId),
//                new BasicDBObject("$pull",
//                        new BasicDBObject("indices",
//                                new BasicDBObject("backend",
//                                        backend
//                                )
//                        )
//                ), false, false);
//        if(index != null){
//            try {
//                fileCollection.update(
//                        new BasicDBObject("id", fileId),
//                        new BasicDBObject("$push",
//                                new BasicDBObject("indices",
//                                        JSON.parse(jsonObjectWriter.writeValueAsString(index))
//                                )
//                        ), false, false);
//            } catch (JsonProcessingException e) {
//                throw new CatalogManagerException(e);
//            }
//        }
//
//        return endQuery("Set index file", startTime);
//    }
//    /**
//     * @param name assuming 'pathRelativeToStudy + name'
//     */
//    @Override
//    public QueryResult<WriteResult> renameFile(int fileId, String name) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        Path path = Paths.get(name);
//        String fileName = path.getFileName().toString();
//
//        File file = getFile(fileId, null).getResult().get(0);
//        if (file.getType().equals(File.TYPE_FOLDER)) {
//            throw new UnsupportedOperationException("Renaming folders still not supported");  // no renaming folders. it will be a future feature
//        }
//
//        int studyId = getStudyIdByFileId(fileId);
//        int collisionFileId = getFileId(studyId, name);
//        if (collisionFileId >= 0) {
//            throw new CatalogManagerException("Can not rename: " + name + " already exists");
//        }
//
//        BasicDBObject query = new BasicDBObject("id", fileId);
//        BasicDBObject set = new BasicDBObject("$set", BasicDBObjectBuilder
//                .start("name", fileName)
//                .append("path", name).get());
//        QueryResult<WriteResult> update = fileCollection.update(query, set, false, false);
//        if (update.getResult().isEmpty() || update.getResult().get(0).getN() == 0) {
//            throw new CatalogManagerException("File {id:" + fileId + "} not found");
//        }
//        return endQuery("rename file", startTime, update);
//    }
//
//
//    @Override
//    public int getStudyIdByFileId(int fileId) throws CatalogManagerException {
//        DBObject query = new BasicDBObject("id", fileId);
//        DBObject projection = new BasicDBObject("studyId", "true");
//        QueryResult<DBObject> result = fileCollection.find(query, null, null, projection);
//
//        if (!result.getResult().isEmpty()) {
//            return (int) result.getResult().get(0).get("studyId");
//        } else {
//            throw new CatalogManagerException("Study not found");
//        }
//    }
//
//    @Override
//    public String getFileOwner(int fileId) throws CatalogManagerException {
//        int studyId = getStudyIdByFileId(fileId);
//        return getStudyOwnerId(studyId);
//    }
//
//    private int getDiskUsageByStudy(int studyId){
//        List<DBObject> operations = Arrays.<DBObject>asList(
//                new BasicDBObject(
//                        "$match",
//                        new BasicDBObject(
//                                "studyId",
//                                studyId
//                                //new BasicDBObject("$in",studyIds)
//                        )
//                ),
//                new BasicDBObject(
//                        "$group",
//                        BasicDBObjectBuilder
//                                .start("_id", "$studyId")
//                                .append("diskUsage",
//                                        new BasicDBObject(
//                                                "$sum",
//                                                "$diskUsage"
//                                        )).get()
//                )
//        );
//        QueryResult<DBObject> aggregate = (QueryResult<DBObject>) fileCollection.aggregate(null, operations, null);
//        if(aggregate.getNumResults() == 1){
//            Object diskUsage = aggregate.getResult().get(0).get("diskUsage");
//            if(diskUsage instanceof Integer){
//                return (Integer)diskUsage;
//            } else {
//                return Integer.parseInt(diskUsage.toString());
//            }
//        } else {
//            return 0;
//        }
//    }
//
//    /**
//     * query: db.file.find({id:2}, {acl:{$elemMatch:{userId:"jcoll"}}, studyId:1})
//     */
//    @Override
//    public QueryResult<Acl> getFileAcl(int fileId, String userId) throws CatalogManagerException {
//        long startTime = startQuery();
//        DBObject projection = BasicDBObjectBuilder
//                .start("acl",
//                        new BasicDBObject("$elemMatch",
//                                new BasicDBObject("userId", userId)))
//                .append("_id", false)
//                .get();
//
//        QueryResult queryResult = fileCollection.find(new BasicDBObject("id", fileId), null, null, projection);
//        if (queryResult.getNumResults() == 0) {
//            throw new CatalogManagerException("getFileAcl: There is no file with fileId = " + fileId);
//        }
//        List<Acl> acl = parseFile(queryResult).getAcl();
//        return endQuery("get file acl", startTime, acl);
//    }
//
//    @Override
//    public QueryResult setFileAcl(int fileId, Acl newAcl) throws CatalogManagerException {
//        long startTime = startQuery();
//        String userId = newAcl.getUserId();
//        if (!userExists(userId)) {
//            throw new CatalogManagerException("Can not set ACL to non-existent user: " + userId);
//        }
//
//        DBObject newAclObject;
//        try {
//            newAclObject = (DBObject) JSON.parse(jsonObjectWriter.writeValueAsString(newAcl));
//        } catch (JsonProcessingException e) {
//            throw new CatalogManagerException("could not put ACL: parsing error");
//        }
//
//        List<Acl> acls = getFileAcl(fileId, userId).getResult();
//        DBObject match;
//        DBObject updateOperation;
//        if (acls.isEmpty()) {  // there is no acl for that user in that file. push
//            match = new BasicDBObject("id", fileId);
//            updateOperation = new BasicDBObject("$push", new BasicDBObject("acl", newAclObject));
//        } else {    // there is already another ACL: overwrite
//            match = BasicDBObjectBuilder
//                    .start("id", fileId)
//                    .append("acl.userId", userId).get();
//            updateOperation = new BasicDBObject("$set", new BasicDBObject("acl.$", newAclObject));
//        }
//        QueryResult update = fileCollection.update(match, updateOperation, false, false);
//        return endQuery("set file acl", startTime);
//    }
//
//    public QueryResult<File> searchFile(QueryOptions query, QueryOptions options) throws CatalogManagerException {
//        long startTime = startQuery();
//
//        BasicDBList filters = new BasicDBList();
//
//        if(query.containsKey("name")){
//            filters.add(new BasicDBObject("name", query.getString("name")));
//        }
//        if(query.containsKey("type")){
//            filters.add(new BasicDBObject("type", query.getString("type")));
//        }
//        if(query.containsKey("path")){
//            filters.add(new BasicDBObject("path", query.getString("path")));
//        }
//        if(query.containsKey("bioformat")){
//            filters.add(new BasicDBObject("bioformat", query.getString("bioformat")));
//        }
//        if(query.containsKey("maxSize")){
//            filters.add(new BasicDBObject("size", new BasicDBObject("$lt", query.getInt("maxSize"))));
//        }
//        if(query.containsKey("minSize")){
//            filters.add(new BasicDBObject("size", new BasicDBObject("$gt", query.getInt("minSize"))));
//        }
//        if(query.containsKey("startDate")){
//            filters.add(new BasicDBObject("creationDate", new BasicDBObject("$lt", query.getString("startDate"))));
//        }
//        if(query.containsKey("endDate")){
//            filters.add(new BasicDBObject("creationDate", new BasicDBObject("$gt", query.getString("endDate"))));
//        }
//        if(query.containsKey("like")){
//            filters.add(new BasicDBObject("name", new BasicDBObject("$regex", query.getString("like"))));
//        }
//        if(query.containsKey("startsWith")){
//            filters.add(new BasicDBObject("name", new BasicDBObject("$regex", "^"+query.getString("startsWith"))));
//        }
//        if(query.containsKey("directory")){
//            filters.add(new BasicDBObject("path", new BasicDBObject("$regex", "^"+query.getString("directory")+"[^/]+/?$")));
//        }
//        if(query.containsKey("studyId")){
//            filters.add(new BasicDBObject("studyId", query.getInt("studyId")));
//        }
//        if(query.containsKey("indexJobId")){
//            filters.add(new BasicDBObject("indices.jobId", query.getString("indexJobId")));
//        }
//        if(query.containsKey("indexState")){
//            filters.add(new BasicDBObject("indices.state", query.getString("indexState")));
//        }
//
//        QueryResult<DBObject> queryResult = fileCollection.find(new BasicDBObject("$and", filters), options);
//
//        List<File> files = parseFiles(queryResult);
//
//        return endQuery("Search File", startTime, files);
//    }


}
