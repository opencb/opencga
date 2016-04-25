package org.opencb.opencga.catalog.managers.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.CatalogFileDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.models.Dataset;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Study;

import java.io.DataInputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface IFileManager extends ResourceManager<Long, File> {

    /*-------------*/
    /* URI METHODS */
    /*-------------*/
    URI getStudyUri(long studyId) throws CatalogException;

    URI getFileUri(Study study, File file) throws CatalogException;

    URI getFileUri(File file) throws CatalogException;

    URI getFileUri(long studyId, String relativeFilePath) throws CatalogException;

    @Deprecated
    URI getFileUri(URI studyUri, String relativeFilePath) throws CatalogException;

    /*-------------*/
    /* ID METHODS  */
    /*-------------*/
    String getUserId(long fileId) throws CatalogException;

    Long getStudyId(long fileId) throws CatalogException;

    Long getFileId(String fileId) throws CatalogException;

    boolean isExternal(File file) throws CatalogException;

    /*--------------*/
    /* CRUD METHODS */
    /*--------------*/
    QueryResult<File> create(long studyId, File.Type type, File.Format format, File.Bioformat bioformat, String path, String ownerId,
                             String creationDate, String description, File.FileStatus status, long diskUsage, long experimentId,
                             List<Long> sampleIds, long jobId, Map<String, Object> stats, Map<String, Object> attributes,
                             boolean parents, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> createFolder(long studyId, String path, File.FileStatus status, boolean parents, String description,
                                   QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> readAll(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> getParent(long fileId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> getParents(long fileId, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<File> rename(long fileId, String newName, String sessionId) throws CatalogException;

    QueryResult<File> unlink(long fileId, String sessionId) throws CatalogException;

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
        long studyId = query.getLong(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key());
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
        long studyId = query.getLong(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key());
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
        long studyId = query.getLong(CatalogFileDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("File[groupBy]: Study id not found in the query");
        }
        return groupBy(studyId, query, field, options, sessionId);
    }

    QueryResult<Dataset> createDataset(long studyId, String name, String description, List<Long> files, Map<String, Object> attributes,
                                       QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Dataset> readDataset(long dataSetId, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream grep(long fileId, String pattern, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream download(long fileId, int offset, int limit, QueryOptions options, String sessionId) throws CatalogException;

    DataInputStream head(long fileId, int lines, QueryOptions options, String sessionId) throws CatalogException;

}
