package org.opencb.opencga.catalog.managers;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.datastore.core.result.WriteResult;
import org.opencb.opencga.catalog.audit.AuditManager;
import org.opencb.opencga.catalog.audit.AuditRecord;
import org.opencb.opencga.catalog.auth.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.db.DBAdaptorFactory;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.opencb.opencga.catalog.models.InternalGetQueryResult;
import org.opencb.opencga.catalog.utils.ParamUtils;
import org.opencb.opencga.catalog.utils.UUIDUtils;
import org.opencb.opencga.core.config.Configuration;
import org.opencb.opencga.core.models.IPrivateStudyUid;
import org.opencb.opencga.core.models.Study;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.opencb.opencga.catalog.audit.AuditRecord.ERROR;
import static org.opencb.opencga.catalog.audit.AuditRecord.SUCCESS;

/**
 * Created by pfurio on 07/08/17.
 */
public abstract class ResourceManager<R extends IPrivateStudyUid> extends AbstractManager {

    ResourceManager(AuthorizationManager authorizationManager, AuditManager auditManager, CatalogManager catalogManager,
                    DBAdaptorFactory catalogDBAdaptorFactory, CatalogIOManagerFactory ioManagerFactory, Configuration configuration) {
        super(authorizationManager, auditManager, catalogManager, catalogDBAdaptorFactory, ioManagerFactory, configuration);
    }

    abstract AuditRecord.Entity getEntity();

    QueryResult<R> internalGet(long studyUid, String entry, QueryOptions options, String user) throws CatalogException {
        return internalGet(studyUid, entry, null, options, user);
    }

    abstract QueryResult<R> internalGet(long studyUid, String entry, @Nullable Query query, QueryOptions options, String user)
            throws CatalogException;

    InternalGetQueryResult<R> internalGet(long studyUid, List<String> entryList, QueryOptions options, String user, boolean silent)
            throws CatalogException {
        return internalGet(studyUid, entryList, null, options, user, silent);
    }

    abstract InternalGetQueryResult<R> internalGet(long studyUid, List<String> entryList, @Nullable Query query, QueryOptions options,
                                                   String user, boolean silent) throws CatalogException;

    /**
     * Create an entry in catalog.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entry     entry that needs to be added in Catalog.
     * @param options   QueryOptions object.
     * @param token Session id of the user logged in.
     * @return A QueryResult of the object created.
     * @throws CatalogException if any parameter from the entry is incorrect, the user does not have permissions...
     */
    public abstract QueryResult<R> create(String studyStr, R entry, QueryOptions options, String token) throws CatalogException;

    /**
     * Fetch the R object.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entryStr  Entry id to be fetched.
     * @param options   QueryOptions object, like "include", "exclude", "limit" and "skip".
     * @param token sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException.
     */
    public QueryResult<R> get(String studyStr, String entryStr, QueryOptions options, String token) throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyStr, userId);
        return internalGet(study.getUid(), entryStr, options, userId);
    }

    /**
     * Fetch all the R objects matching the query.
     *
     * @param studyStr  Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param entryList Comma separated list of entries to be fetched.
     * @param options   QueryOptions object, like "include", "exclude", "limit" and "skip".
     * @param token sessionId
     * @return All matching elements.
     * @throws CatalogException CatalogException.
     */
    public List<QueryResult<R>> get(String studyStr, List<String> entryList, QueryOptions options, String token) throws CatalogException {
        return get(studyStr, entryList, new Query(), options, false, token);
    }

    public List<QueryResult<R>> get(String studyStr, List<String> entryList, QueryOptions options, boolean silent, String token)
            throws CatalogException {
        return get(studyStr, entryList, new Query(), options, silent, token);
    }

    public List<QueryResult<R>> get(String studyId, List<String> entryList, Query query, QueryOptions options, boolean silent, String token)
            throws CatalogException {
        String userId = catalogManager.getUserManager().getUserId(token);
        Study study = catalogManager.getStudyManager().resolveId(studyId, userId);

        query = ParamUtils.defaultObject(query, Query::new);
        options = ParamUtils.defaultObject(options, QueryOptions::new);

        ObjectMap auditParams = new ObjectMap()
                .append("studyId", studyId)
                .append("entryList", entryList)
                .append("query", new Query(query))
                .append("options", new QueryOptions(options))
                .append("silent", silent)
                .append("token", token);
        String operationUuid = UUIDUtils.generateOpenCGAUUID(UUIDUtils.Entity.AUDIT);

        try {
            List<QueryResult<R>> resultList = new ArrayList<>(entryList.size());

            InternalGetQueryResult<R> responseResult = internalGet(study.getUid(), entryList, query, options, userId, silent);

            Map<String, InternalGetQueryResult.Missing> missingMap = new HashMap<>();
            if (responseResult.getMissing() != null) {
                missingMap = responseResult.getMissing().stream()
                        .collect(Collectors.toMap(InternalGetQueryResult.Missing::getId, Function.identity()));
            }

            List<List<R>> versionedResults = responseResult.getVersionedResults();
            for (int i = 0; i < versionedResults.size(); i++) {
                String entryId = entryList.get(i);
                if (versionedResults.get(i).isEmpty()) {
                    // Missing
                    resultList.add(new QueryResult<>(entryId, responseResult.getDbTime(), 0, 0, "", missingMap.get(entryId).getErrorMsg(),
                            Collections.emptyList()));
                } else {
                    int size = versionedResults.get(i).size();
                    resultList.add(new QueryResult<>(entryId, responseResult.getDbTime(), size, size, "", "", versionedResults.get(i)));
                }
            }

            for (QueryResult<R> queryResult : resultList) {
                auditManager.auditInfo(userId, operationUuid, queryResult.first().getId(), queryResult.first().getUuid(), study.getId(),
                        study.getUuid(), auditParams, getEntity(), SUCCESS);
            }

            return resultList;
        } catch (CatalogException e) {
            for (String entryId : entryList) {
                auditManager.auditInfo(userId, operationUuid, entryId, "", study.getId(), study.getUuid(), auditParams, getEntity(), ERROR);
            }
            throw e;
        }
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
     * @param studyId  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param options   QueryOptions object.
     * @param token Session id of the user logged in.
     * @return The list of entries matching the query.
     * @throws CatalogException catalogException.
     */
    public abstract QueryResult<R> search(String studyId, Query query, QueryOptions options, String token) throws CatalogException;

    /**
     * Count matching entries in catalog.
     *
     * @param studyId  study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query     Query object.
     * @param token Session id of the user logged in.
     * @return A QueryResult with the total number of entries matching the query.
     * @throws CatalogException catalogException.
     */
    public abstract QueryResult<R> count(String studyId, Query query, String token) throws CatalogException;

    /**
     * Delete all entries matching the query.
     *
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param query Query object.
     * @param params Map containing additional parameters to be considered for the deletion.
     * @param sessionId Session id of the user logged in.
     * @throws CatalogException if the study or the user do not exist.
     * @return A WriteResult object containing the number of matching elements, deleted and elements that could not be deleted.
     */
    public abstract WriteResult delete(String studyStr, Query query, ObjectMap params, String sessionId) throws CatalogException;

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
