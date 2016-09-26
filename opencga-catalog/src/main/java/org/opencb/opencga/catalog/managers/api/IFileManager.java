package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.DatasetAclEntry;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public interface IFileManager extends ResourceManager<Long, File> {

    /*-------------*/
    /* URI METHODS */
    /*-------------*/
    URI getStudyUri(long studyId) throws CatalogException;

    URI getUri(Study study, File file) throws CatalogException;

    URI getUri(File file) throws CatalogException;

    URI getUri(long studyId, String relativeFilePath) throws CatalogException;

    @Deprecated
    URI getUri(URI studyUri, String relativeFilePath) throws CatalogException;



    /*-------------*/
    /* ID METHODS  */
    /*-------------*/
    Long getStudyId(long fileId) throws CatalogException;

    /**
     * Obtains the numeric file id given a string.
     *
     * @param fileStr File id in string format. Could be one of [id | user@aliasProject:aliasStudy:{fileName|path}
     *                | user@aliasStudy:{fileName|path} | aliasStudy:{fileName|path} | {fileName|path}].
     * @param studyId study id where the file will be looked for.
     * @param sessionId session id of the user asking for the file.
     * @return the numeric file id.
     * @throws CatalogException when more than one file id is found.
     */
    Long getId(String fileStr, long studyId, String sessionId) throws CatalogException;

    /**
     * Obtains the numeric file id given a string.
     *
     * @param userId User id of the user asking for the file id.
     * @param fileStr File id in string format. Could be one of [id | user@aliasProject:aliasStudy:{fileName|path}
     *                | user@aliasStudy:{fileName|path} | aliasStudy:{fileName|path} | {fileName|path}].
     * @return the numeric file id.
     * @throws CatalogException when more than one file id is found.
     */
    Long getId(String userId, String fileStr) throws CatalogException;

    /**
     * Obtains the list of fileIds corresponding to the comma separated list of file strings given in fileStr.
     *
     * @param userId User demanding the action.
     * @param fileStr Comma separated list of file ids.
     * @return A list of file ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getIds(String userId, String fileStr) throws CatalogException {
        List<Long> fileIds = new ArrayList<>();
        for (String fileId : fileStr.split(",")) {
            fileIds.add(getId(userId, fileId));
        }
        return fileIds;
    }

    void matchUpVariantFiles(List<File> avroFiles, String sessionId) throws CatalogException;

    @Deprecated
    Long getId(String fileId) throws CatalogException;

    boolean isExternal(File file) throws CatalogException;

    QueryResult<FileIndex>  updateFileIndexStatus(File file, String newStatus, String sessionId) throws CatalogException;


    /*--------------*/
    /* CRUD METHODS */
    /*--------------*/
    QueryResult<File> create(long studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path, String creationDate,
                             String description, File.FileStatus status, long diskUsage, long experimentId, List<Long> sampleIds,
                             long jobId, Map<String, Object> stats, Map<String, Object> attributes, boolean parents, QueryOptions options,
                             String sessionId) throws CatalogException;

    QueryResult<File> createFolder(long studyId, String path, File.FileStatus status, boolean parents, String description,
                                   QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Long> count(Query query, String sessionId) throws CatalogException;

    /**
     * Look for files inside the path.
     *
     * @param path Directory where the files are to be found.
     * @param recursive Boolean indicating whether to look inside the folder recursively.
     * @param query Query object.
     * @param options Query options object.
     * @param sessionId session id of the user doing the query.
     * @return A queryResult object containing the files found.
     * @throws CatalogException catalogException.
     */
    QueryResult<File> get(String path, boolean recursive, Query query, QueryOptions options, String sessionId)
            throws CatalogException;

    QueryResult<File> getParent(long fileId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> getParents(long fileId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> rename(long fileId, String newName, String sessionId) throws CatalogException;

    QueryResult<File> link(URI uriOrigin, String pathDestiny, long studyId, ObjectMap params, String sessionId)
            throws CatalogException, IOException;

    QueryResult<FileTree> getTree(String fileIdStr, Query query, QueryOptions queryOptions, int maxDepth, String sessionId)
            throws CatalogException;

    @Deprecated
    QueryResult<File> unlink(long fileId, String sessionId) throws CatalogException;

    QueryResult<File> unlink(String fileIdStr, QueryOptions options, String sessionId) throws CatalogException, IOException;

    /**
     * Retrieve the file Acls for the given members in the file.
     *
     * @param fileStr File id of which the acls will be obtained.
     * @param members userIds/groupIds for which the acls will be retrieved. When this is null, it will obtain all the acls.
     * @param sessionId Session of the user that wants to retrieve the acls.
     * @return A queryResult containing the file acls.
     * @throws CatalogException when the userId does not have permissions (only the users with an "admin" role will be able to do this),
     * the file id is not valid or the members given do not exist.
     */
    QueryResult<FileAclEntry> getAcls(String fileStr, List<String> members, String sessionId) throws CatalogException;
    default List<QueryResult<FileAclEntry>> getAcls(List<String> fileIds, List<String> members, String sessionId)
            throws CatalogException {
        List<QueryResult<FileAclEntry>> result = new ArrayList<>(fileIds.size());
        for (String fileStr : fileIds) {
            result.add(getAcls(fileStr, members, sessionId));
        }
        return result;
    }



    @Deprecated
    QueryResult move(long fileId, String newPath, QueryOptions options, String sessionId)
            throws CatalogException;

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param studyId    Study id.
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param sessionId  sessionId.
     * @return           A QueryResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException;

    default QueryResult rank(Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException {
        long studyId = query.getLong(FileDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("File[rank]: Study id not found in the query");
        }
        return rank(studyId, query, field, numResults, asc, sessionId);
    }

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param studyId Study id.
     * @param query   Query object containing the query that will be executed.
     * @param field   Field by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the field.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(long studyId, Query query, String field, QueryOptions options, String sessionId) throws CatalogException;

    default QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        long studyId = query.getLong(FileDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("File[groupBy]: Study id not found in the query");
        }
        return groupBy(studyId, query, field, options, sessionId);
    }

    /**
     * Groups the elements queried by the field(s) given.
     *
     * @param studyId Study id.
     * @param query   Query object containing the query that will be executed.
     * @param fields  List of fields by which the results will be grouped in.
     * @param options QueryOptions object.
     * @param sessionId  sessionId.
     * @return        A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(long studyId, Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException;

    default QueryResult groupBy(Query query, List<String> field, QueryOptions options, String sessionId) throws CatalogException {
        long studyId = query.getLong(FileDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("File[groupBy]: Study id not found in the query");
        }
        return groupBy(studyId, query, field, options, sessionId);
    }

    QueryResult<Dataset> createDataset(long studyId, String name, String description, List<Long> files, Map<String, Object> attributes,
                                       QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Dataset> readDataset(long dataSetId, QueryOptions options, String sessionId) throws CatalogException;

    Long getStudyIdByDataset(long datasetId) throws CatalogException;

    /**
     * Obtains the numeric dataset id given a string.
     *
     * @param userId User id of the user asking for the file id.
     * @param datasetStr Dataset id in string format. Could be one of [id | user@aliasProject:aliasStudy:datasetName
     *                | user@aliasStudy:datasetName | aliasStudy:datasetName | datasetName].
     * @return the numeric dataset id.
     * @throws CatalogException when more than one dataset id is found.
     */
    Long getDatasetId(String userId, String datasetStr) throws CatalogException;

    /**
     * Obtains the list of dataset ids corresponding to the comma separated list of dataset strings given in datasetStr.
     *
     * @param userId User demanding the action.
     * @param datasetStr Comma separated list of dataset ids.
     * @return A list of dataset ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getDatasetIds(String userId, String datasetStr) throws CatalogException {
        List<Long> datasetIds = new ArrayList<>();
        for (String datasetId : datasetStr.split(",")) {
            datasetIds.add(getDatasetId(userId, datasetId));
        }
        return datasetIds;
    }

    /**
     * Retrieve the dataset Acls for the given members in the dataset.
     *
     * @param datasetStr Dataset id of which the acls will be obtained.
     * @param members userIds/groupIds for which the acls will be retrieved. When this is null, it will obtain all the acls.
     * @param sessionId Session of the user that wants to retrieve the acls.
     * @return A queryResult containing the file acls.
     * @throws CatalogException when the userId does not have permissions (only the users with an "admin" role will be able to do this),
     * the dataset id is not valid or the members given do not exist.
     */
    QueryResult<DatasetAclEntry> getDatasetAcls(String datasetStr, List<String> members, String sessionId) throws CatalogException;
    default List<QueryResult<DatasetAclEntry>> getDatasetAcls(List<String> datasetIds, List<String> members, String sessionId)
            throws CatalogException {
        List<QueryResult<DatasetAclEntry>> result = new ArrayList<>(datasetIds.size());
        for (String datasetId : datasetIds) {
            result.add(getDatasetAcls(datasetId, members, sessionId));
        }
        return result;
    }

    DataInputStream grep(long fileId, String pattern, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream download(long fileId, int offset, int limit, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream head(long fileId, int lines, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Index variants or alignments.
     *
     * @param fileIdStr Comma separated list of file ids (directories or files)
     * @param type Type of the file(s) to be indexed (VCF or BAM)
     * @param params Object map containing the extra parameters for the indexation.
     * @param sessionId session id of the user asking for the index.
     * @return .
     * @throws CatalogException when the files or folders are not in catalog or the study does not match between them.
     * */
    QueryResult index(String fileIdStr, String type, Map<String, String> params, String sessionId) throws CatalogException;
}
