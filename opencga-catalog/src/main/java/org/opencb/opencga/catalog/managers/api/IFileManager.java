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

package org.opencb.opencga.catalog.managers.api;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.FileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.models.*;
import org.opencb.opencga.catalog.models.acls.permissions.FileAclEntry;

import javax.annotation.Nullable;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
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

//    /**
//     * Obtains the numeric file id given a string.
//     *
//     * @param fileStr File id in string format. Could be one of [id | user@aliasProject:aliasStudy:{fileName|path}
//     *                | user@aliasStudy:{fileName|path} | aliasStudy:{fileName|path} | {fileName|path}].
//     * @param studyId study id where the file will be looked for.
//     * @param sessionId session id of the user asking for the file.
//     * @return the numeric file id.
//     * @throws CatalogException when more than one file id is found.
//     */
//    @Deprecated
//    Long getId(String fileStr, long studyId, String sessionId) throws CatalogException;

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param fileStr File id in string format. Could be either the id, name or path.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException when more than one file id is found.
     */
    AbstractManager.MyResourceId getId(String fileStr, @Nullable String studyStr, String sessionId) throws CatalogException;

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param fileStr File id in string format. Could be either the id, name or path.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException CatalogException.
     */
    AbstractManager.MyResourceIds getIds(String fileStr, @Nullable String studyStr, String sessionId) throws CatalogException;

    /**
     * Obtains the numeric file id given a string.
     *
     * @param userId User id of the user asking for the file id.
     * @param fileStr File id in string format. Could be one of [id | user@aliasProject:aliasStudy:{fileName|path}
     *                | user@aliasStudy:{fileName|path} | aliasStudy:{fileName|path} | {fileName|path}].
     * @return the numeric file id.
     * @throws CatalogException when more than one file id is found.
     */
    @Deprecated
    Long getId(String userId, String fileStr) throws CatalogException;

    /**
     * Obtains the list of fileIds corresponding to the comma separated list of file strings given in fileStr.
     *
     * @param userId User demanding the action.
     * @param fileStr Comma separated list of file ids.
     * @return A list of file ids.
     * @throws CatalogException CatalogException.
     */
    @Deprecated
    default List<Long> getIds(String userId, String fileStr) throws CatalogException {
        List<Long> fileIds = new ArrayList<>();
        for (String fileId : fileStr.split(",")) {
            fileIds.add(getId(userId, fileId));
        }
        return fileIds;
    }

    void matchUpVariantFiles(List<File> transformedFiles, String sessionId) throws CatalogException;

    @Deprecated
    Long getId(String fileId) throws CatalogException;

    /**
     * Delete entries from Catalog.
     *
     * @param ids       Comma separated list of ids corresponding to the objects to delete
     * @param studyStr  Study string.
     * @param params   Deleting options.
     * @param sessionId sessionId
     * @return A list with the deleted objects
     * @throws CatalogException CatalogException
     * @throws IOException IOException.
     */
    List<QueryResult<File>> delete(String ids, @Nullable String studyStr, ObjectMap params, String sessionId)
            throws CatalogException, IOException;

    boolean isExternal(File file) throws CatalogException;

    QueryResult<FileIndex>  updateFileIndexStatus(File file, String newStatus, String message, String sessionId) throws CatalogException;


    /*--------------*/
    /* CRUD METHODS */
    /*--------------*/
    QueryResult<File> create(String studyStr, File.Type type, File.Format format, File.Bioformat bioformat, String path,
                             String creationDate, String description, File.FileStatus status, long size, long experimentId,
                             List<Sample> samples, long jobId, Map<String, Object> stats, Map<String, Object> attributes, boolean parents,
                             String content, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> createFolder(String studyStr, String path, File.FileStatus status, boolean parents, String description,
                                   QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    DBIterator<File> iterator(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Multi-study search of files in catalog.
     *
     * @param studyStr Study string that can point to several studies of the same project.
     * @param query    Query object.
     * @param options  QueryOptions object.
     * @param sessionId Session id.
     * @return The list of files matching the query.
     * @throws CatalogException catalogException.
     */
    QueryResult<File> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> count(String studyStr, Query query, String sessionId) throws CatalogException;

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

    QueryResult<FileTree> getTree(String fileIdStr, @Nullable String studyId, Query query, QueryOptions queryOptions, int maxDepth,
                                  String sessionId) throws CatalogException;

    QueryResult<File> unlink(String fileIdStr, @Nullable String studyStr, String sessionId) throws CatalogException, IOException;

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

    default QueryResult groupBy(@Nullable String studyStr, Query query, QueryOptions options, String fields, String sessionId)
            throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(studyStr, query, Arrays.asList(fields.split(",")), options, sessionId);
    }

    QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException;

    @Deprecated
    @Override
    default QueryResult groupBy(Query query, String field, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Group by has to be called passing the study string");
    }

    @Deprecated
    @Override
    default QueryResult groupBy(Query query, List<String> fields, QueryOptions options, String sessionId) throws CatalogException {
        throw new NotImplementedException("Group by has to be called passing the study string");
    }

    QueryResult<Dataset> createDataset(long studyId, String name, String description, List<Long> files, Map<String, Object> attributes,
                                       QueryOptions options, String sessionId) throws CatalogException;

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

    DataInputStream grep(long fileId, String pattern, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream download(long fileId, int offset, int limit, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream head(long fileId, int lines, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Index variants or alignments.
     *
     * @param fileIdStr Comma separated list of file ids (directories or files)
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param type Type of the file(s) to be indexed (VCF or BAM)
     * @param params Object map containing the extra parameters for the indexation.
     * @param sessionId session id of the user asking for the index.
     * @return .
     * @throws CatalogException when the files or folders are not in catalog or the study does not match between them.
     * */
    QueryResult index(String fileIdStr, String studyStr, String type, Map<String, String> params, String sessionId) throws CatalogException;

    void setFileIndex(long fileId, FileIndex index, String sessionId) throws CatalogException;

    void setDiskUsage(long fileId, long size, String sessionId) throws CatalogException;

    void setModificationDate(long fileId, String date, String sessionId) throws CatalogException;

    void setUri(long fileId, String uri, String sessionId) throws CatalogException;

    // -------------- ACLs -------------------

    List<QueryResult<FileAclEntry>> updateAcl(String file, String studyStr, String memberId, File.FileAclParams fileAclParams,
                                                String sessionId) throws CatalogException;

}
