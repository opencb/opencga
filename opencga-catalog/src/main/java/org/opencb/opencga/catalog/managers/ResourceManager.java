package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.PrivateStudyUid;
import org.opencb.opencga.core.models.Study;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by pfurio on 07/08/17.
 */
public abstract class ResourceManager<R extends PrivateStudyUid> extends AbstractManager {

    ResourceManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                    DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
    }

    /**
     * Obtains the resource java bean containing the requested entries.
     *
     * @param entryStr  Entry id in string format.
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested entries.
     * @throws CatalogException when more than one entry is found.
     */
    public AbstractManager.MyResource<R> getUid(String entryStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, QueryOptions.empty());
        R entry = smartResolutor(study.getUid(), entryStr, userId);
        return new MyResource<>(userId, study, entry);
    }

    /**
     * Obtains the resource java beans containing the requested entries.
     *
     * @param entryStr  Entry id in string format.
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java beans containing the requested entries.
     * @throws CatalogException when more than one entry is found.
     */
    public AbstractManager.MyResources<R> getUids(String entryStr, @Nullable String studyStr, String sessionId) throws CatalogException {
        return getUids(Arrays.asList(entryStr.split(",")), studyStr, false, sessionId);
    }

    /**
     * Obtains the resource java beans containing the requested entries.
     *
     * @param entryStr  List of entry ids in string format.
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java beans containing the requested entries.
     * @throws CatalogException when more than one entry is found.
     */
    public AbstractManager.MyResources<R> getUids(List<String> entryStr, @Nullable String studyStr, String sessionId)
            throws CatalogException {
        return getUids(entryStr, studyStr, false, sessionId);
    }

    /**
     * Obtains the resource java beans containing the requested entries.
     *
     * @param entryList  List of entry ids in string format.
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param silent boolean to accept partial or complete results.
     * @param sessionId Session id of the user logged.
     * @return the resource java beans containing the requested entries.
     * @throws CatalogException when more than one entry is found.
     */
    public AbstractManager.MyResources<R> getUids(List<String> entryList, @Nullable String studyStr, boolean silent, String sessionId)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(sessionId);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId, QueryOptions.empty());
        List<R> finalEntryList = new ArrayList<>(entryList.size());
        for (String entryStr : entryList) {
            try {
                R entry = smartResolutor(study.getUid(), entryStr, userId);
                finalEntryList.add(entry);
            } catch (CatalogException e) {
                if (!silent) {
                    throw e;
                }
            }
        }
        return new MyResources<>(userId, study, finalEntryList);
    }

    abstract R smartResolutor(long studyUid, String entry, String user) throws CatalogException;

    /**
     * Create an entry in catalog.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entry     entry that needs to be added in Catalog.
     * @param options   QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return A QueryResult of the object created.
     * @throws CatalogException if any parameter from the entry is incorrect, the user does not have permissions...
     */
    public abstract QueryResult<R> create(String studyStr, R entry, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Update an entry from catalog.
     *
     * @param studyStr   Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entryStr   Entry id in string format. Could be either the id or name generally.
     * @param parameters Map with parameters and values from the entry to be updated.
     * @param options    QueryOptions object.
     * @param sessionId  Session id of the user logged in.
     * @return A QueryResult with the object updated.
     * @throws CatalogException if there is any internal error, the user does not have proper permissions or a parameter passed does not
     *                          exist or is not allowed to be updated.
     */
    public abstract QueryResult<R> update(String studyStr, String entryStr, ObjectMap parameters, QueryOptions options, String sessionId)
            throws CatalogException;

    /**
     * Fetch the R object.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entryStr  Entry id to be fetched.
     * @param options   QueryOptions object, like "include", "exclude", "limit" and "skip".
     * @param sessionId sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException.
     */
    public QueryResult<R> get(String studyStr, String entryStr, QueryOptions options, String sessionId) throws CatalogException {
        Query query = new Query();
        MyResource resources = getUid(entryStr, studyStr, sessionId);
        query.put("uid", resources.getResource().getUid());
        return get(studyStr, query, options, sessionId);
    }

    /**
     * Fetch all the R objects matching the query.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entryList Comma separated list of entries to be fetched.
     * @param query     Query object.
     * @param options   QueryOptions object, like "include", "exclude", "limit" and "skip".
     * @param sessionId sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException.
     */
    public List<QueryResult<R>> get(String studyStr, List<String> entryList, Query query, QueryOptions options, String sessionId)
            throws CatalogException {
        List<QueryResult<R>> resultList = new ArrayList<>(entryList.size());
        query = ParamUtils.defaultObject(query, Query::new);

        MyResources<R> resource = getUids(entryList, studyStr, sessionId);
        List<Long> resourceIds = resource.getResourceList().stream().map(R::getUid).collect(Collectors.toList());
        for (int i = 0; i < resourceIds.size(); i++) {
            Long entityId = resourceIds.get(i);
            Query queryCopy = new Query(query);
            queryCopy.put("uid", entityId);
            QueryResult<R> rQueryResult = get(studyStr, queryCopy, options, sessionId);
            rQueryResult.setId(entryList.get(i));
            resultList.add(rQueryResult);
        }

        return resultList;
    }

    public List<QueryResult<R>> get(String studyStr, List<String> entryList, Query query, QueryOptions options, boolean silent,
                                    String sessionId)
            throws CatalogException {
        List<QueryResult<R>> resultList = new ArrayList<>(entryList.size());
        query = ParamUtils.defaultObject(query, Query::new);

        MyResources<R> resource = getUids(entryList, studyStr, silent, sessionId);
        List<R> resourceIds = resource.getResourceList();
        for (int i = 0; i < resourceIds.size(); i++) {
            R entity = resourceIds.get(i);
            Query queryCopy = new Query(query);
            queryCopy.put("uid", entity.getUid());
            try {
                QueryResult<R> rQueryResult = get(studyStr, queryCopy, options, sessionId);
                rQueryResult.setId(entryList.get(i));
                resultList.add(rQueryResult);
            } catch (CatalogException e) {
                if (silent) {
                    resultList.add(new QueryResult<>(entryList.get(i), 0, 0, 0, "", e.toString(), new ArrayList<>(0)));
                } else {
                    throw e;
                }
            }
        }
        return resultList;
    }

    /**
     * Fetch all the R objects matching the query.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param options   QueryOptions object, like "include", "exclude", "limit" and "skip".
     * @param sessionId sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException.
     */
    public abstract QueryResult<R> get(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Obtain an entry iterator to iterate over the matching entries.
     *
     * @param studyId   study id.
     * @param query     Query object.
     * @param options   QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return An iterator.
     * @throws CatalogException if there is any internal error.
     */
    public DBIterator<R> iterator(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException {
        return iterator(String.valueOf(studyId), query, options, sessionId);
    }

    /**
     * Obtain an entry iterator to iterate over the matching entries.
     *
     * @param studyStr  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param options   QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return An iterator.
     * @throws CatalogException if there is any internal error.
     */
    public abstract DBIterator<R> iterator(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Search of entries in catalog.
     *
     * @param studyStr  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param options   QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return The list of entries matching the query.
     * @throws CatalogException catalogException.
     */
    public abstract QueryResult<R> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Count matching entries in catalog.
     *
     * @param studyStr  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param sessionId Session id of the user logged in.
     * @return A QueryResult with the total number of entries matching the query.
     * @throws CatalogException catalogException.
     */
    public abstract QueryResult<R> count(String studyStr, Query query, String sessionId) throws CatalogException;

    /**
     * Delete all entries matching the query.
     *
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query Query object.
     * @param params Map containing additional parameters to be considered for the deletion.
     * @param sessionId Session id of the user logged in.
     * @return A WriteResult object containing the number of matching elements, deleted and elements that could not be deleted.
     */
    public abstract WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId);

    /**
     * Ranks the elements queried, groups them by the field(s) given and return it sorted.
     *
     * @param studyStr   study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query      Query object containing the query that will be executed.
     * @param field      A field or a comma separated list of fields by which the results will be grouped in.
     * @param numResults Maximum number of results to be reported.
     * @param asc        Order in which the results will be reported.
     * @param sessionId  Session id of the user logged in.
     * @return A QueryResult object containing each of the fields in field and the count of them matching the query.
     * @throws CatalogException CatalogException
     */
    public abstract QueryResult rank(String studyStr, Query query, String field, int numResults, boolean asc, String sessionId)
            throws CatalogException;

    /**
     * Groups the matching entries by some fields.
     *
     * @param studyStr  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param fields    A field or a comma separated list of fields by which the results will be grouped in.
     * @param options   QueryOptions object.
     * @param sessionId Session id of the user logged in.
     * @return A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    public QueryResult groupBy(@Nullable String studyStr, Query query, String fields, QueryOptions options, String sessionId)
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
     * @param query     Query object.
     * @param options   QueryOptions object.
     * @param fields    A field or a comma separated list of fields by which the results will be grouped in.
     * @param sessionId Session id of the user logged in.
     * @return A QueryResult object containing the results of the query grouped by the fields.
     * @throws CatalogException CatalogException
     */
    public abstract QueryResult groupBy(@Nullable String studyStr, Query query, List<String> fields, QueryOptions options, String sessionId)
            throws CatalogException;

}
