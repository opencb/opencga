package org.opencb.opencga.catalog.managers.api;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created by pfurio on 03/08/17.
 *
 * This interface will be implemented by all the managers falling within study (file, sample, cohort, individual...).
 */
public interface IEntryManager<I, R> extends ResourceManager<I, R> {

    /**
     * Create an entry in catalog.
     *
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entry entry that needs to be added in Catalog.
     * @param options QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return A QueryResult of the object created.
     * @throws CatalogException if any parameter from the entry is incorrect, the user does not have permissions...
     */
    QueryResult<R> create(String studyStr, R entry, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param entryStr Entry id in string format. Could be either the id or name generally.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException when more than one entry id is found.
     */
    AbstractManager.MyResourceId getId(String entryStr, @Nullable String studyStr, String sessionId) throws CatalogException;

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param entryStr Entry id in string format. Could be either the id or name generally.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException CatalogException.
     */
    AbstractManager.MyResourceIds getIds(String entryStr, @Nullable String studyStr, String sessionId) throws CatalogException;

    /**
     * Obtain the study id where the entry belongs to.
     *
     * @param entryId Entry id.
     * @return The study id where the entry belongs to.
     * @throws CatalogException If the entry id is not found.
     */
    Long getStudyId(long entryId) throws CatalogException;

    /** Obtain an entry iterator to iterate over the matching entries.
     *
     * @param studyId study id.
     * @param query Query object.
     * @param options QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return An iterator.
     * @throws CatalogException if there is any internal error.
     */
    default DBIterator<R> iterator(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return iterator(String.valueOf(studyId), query, options, sessionId);
    }

    /** Obtain an entry iterator to iterate over the matching entries.
     *
     * @param studyStr study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query Query object.
     * @param options QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return An iterator.
     * @throws CatalogException if there is any internal error.
     */
    DBIterator<R> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Search of entries in catalog.
     *
     * @param studyStr study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query    Query object.
     * @param options  QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return The list of entries matching the query.
     * @throws CatalogException catalogException.
     */
    QueryResult<R> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Count matching entries in catalog.
     *
     * @param studyStr study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query    Query object.
     * @param sessionId Session id of the user logged in.
     * @return A QueryResult with the total number of entries matching the query.
     * @throws CatalogException catalogException.
     */
    QueryResult<R> count(String studyStr, Query query, String sessionId) throws CatalogException;

    /**
     * Delete entries from Catalog.
     *
     * @param entries Comma separated list of ids corresponding to the objects to delete.
     * @param studyStr study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param params Map containing additional parameters that might be needed for the delete.
     * @param sessionId Session id of the user logged in.
     * @return A list with the deleted objects.
     * @throws CatalogException CatalogException
     * @throws IOException IOException.
     */
    List<QueryResult<R>> delete(String entries, @Nullable String studyStr, ObjectMap params, String sessionId)
            throws CatalogException, IOException;

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param studyId    Study id.
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param sessionId  Session id of the user logged in.
     * @return           A QueryResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    QueryResult rank(long studyId, Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException;

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param sessionId  Session id of the user logged in.
     * @return           A QueryResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    default QueryResult rank(Query query, String field, int numResults, boolean asc, String sessionId) throws CatalogException {
        long studyId = query.getLong(SampleDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Sample[rank]: Study id not found in the query");
        }
        return rank(studyId, query, field, numResults, asc, sessionId);
    }

    /**
     * Groups the matching entries by some fields.
     *
     * @param studyStr  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query Query object.
     * @param options QueryOptions object.
     * @param fields A field or a comma separated list of fields by which the results will be grouped in.
     * @param sessionId Session id of the user logged in.
     * @return A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    default QueryResult groupBy(@Nullable String studyStr, Query query, QueryOptions options, String fields, String sessionId)
            throws CatalogException {
        if (StringUtils.isEmpty(fields)) {
            throw new CatalogException("Empty fields parameter.");
        }
        return groupBy(studyStr, query, Arrays.asList(fields.split(",")), options, sessionId);
    }

    /**
     * Groups the matching entries by some fields.
     *
     * @param studyStr  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query Query object.
     * @param options QueryOptions object.
     * @param fields A field or a comma separated list of fields by which the results will be grouped in.
     * @param sessionId Session id of the user logged in.
     * @return A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException;

}
