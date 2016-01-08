package org.opencb.opencga.catalog.db.mongodb2;

import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api2.CatalogJobDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.models.Job;
import org.opencb.opencga.catalog.models.Tool;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

/**
 * Created by pfurio on 08/01/16.
 */
public class CatalogMongoJobDBAdaptor extends AbstractCatalogMongoDBAdaptor implements CatalogJobDBAdaptor {

    public CatalogMongoJobDBAdaptor(Logger logger) {
        super(logger);
    }

    @Override
    public boolean jobExists(int jobId) {
        return false;
    }

    @Override
    public QueryResult<Job> createJob(int studyId, Job job, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Job> deleteJob(int jobId) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Job> getJob(int jobId, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Job> getAllJobs(QueryOptions query, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Job> getAllJobsInStudy(int studyId, QueryOptions options) throws CatalogDBException {
        return null;
    }

    @Override
    public String getJobStatus(int jobId, String sessionId) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<ObjectMap> incJobVisits(int jobId) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Job> modifyJob(int jobId, ObjectMap parameters) throws CatalogDBException {
        return null;
    }

    @Override
    public int getStudyIdByJobId(int jobId) throws CatalogDBException {
        return 0;
    }

    @Override
    public QueryResult<Tool> createTool(String userId, Tool tool) throws CatalogDBException {
        return null;
    }

    @Override
    public QueryResult<Tool> getTool(int id) throws CatalogDBException {
        return null;
    }

    @Override
    public int getToolId(String userId, String toolAlias) throws CatalogDBException {
        return 0;
    }

    @Override
    public QueryResult<Tool> getAllTools(QueryOptions queryOptions) throws CatalogDBException {
        return null;
    }

    @Override
    public boolean experimentExists(int experimentId) {
        return false;
    }

    @Override
    public QueryResult<Long> count(Query query) {
        return null;
    }

    @Override
    public QueryResult distinct(Query query, String field) {
        return null;
    }

    @Override
    public QueryResult stats(Query query) {
        return null;
    }

    @Override
    public QueryResult<Job> get(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult nativeGet(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult<Job> update(Query query, ObjectMap parameters) {
        return null;
    }

    @Override
    public QueryResult<Integer> delete(Query query) {
        return null;
    }

    @Override
    public Iterator<Job> iterator(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public Iterator nativeIterator(Query query, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult rank(Query query, String field, int numResults, boolean asc) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, String field, QueryOptions options) {
        return null;
    }

    @Override
    public QueryResult groupBy(Query query, List<String> fields, QueryOptions options) {
        return null;
    }

    @Override
    public void forEach(Query query, Consumer<? super Object> action, QueryOptions options) {

    }
}
