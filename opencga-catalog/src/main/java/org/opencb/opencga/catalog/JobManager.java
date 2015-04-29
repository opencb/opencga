package org.opencb.opencga.catalog;

import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.catalog.CatalogException;
import org.opencb.opencga.catalog.api.IJobManager;
import org.opencb.opencga.catalog.authentication.AuthenticationManager;
import org.opencb.opencga.catalog.authorization.AuthorizationManager;
import org.opencb.opencga.catalog.beans.Job;
import org.opencb.opencga.catalog.db.api.*;
import org.opencb.opencga.catalog.io.CatalogIOManager;
import org.opencb.opencga.catalog.io.CatalogIOManagerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;

/**
 * @author Jacobo Coll <jacobo167@gmail.com>
 */
public class JobManager implements IJobManager {

    final protected AuthorizationManager authorizationManager;
    final protected CatalogUserDBAdaptor userDBAdaptor;
    final protected CatalogStudyDBAdaptor studyDBAdaptor;
    final protected CatalogFileDBAdaptor fileDBAdaptor;
    final protected CatalogSamplesDBAdaptor sampleDBAdaptor;
    final protected CatalogJobDBAdaptor jobDBAdaptor;
    final protected CatalogIOManagerFactory catalogIOManagerFactory;

    protected static Logger logger = LoggerFactory.getLogger(JobManager.class);

    public JobManager(AuthorizationManager authorizationManager, CatalogDBAdaptor catalogDBAdaptor, CatalogIOManagerFactory ioManagerFactory) {
        this.authorizationManager = authorizationManager;
        this.userDBAdaptor = catalogDBAdaptor.getCatalogUserDBAdaptor();
        this.studyDBAdaptor = catalogDBAdaptor.getCatalogStudyDBAdaptor();
        this.fileDBAdaptor = catalogDBAdaptor.getCatalogFileDBAdaptor();
        this.sampleDBAdaptor = catalogDBAdaptor.getCatalogSamplesDBAdaptor();
        this.jobDBAdaptor = catalogDBAdaptor.getCatalogJobDBAdaptor();
        this.catalogIOManagerFactory = ioManagerFactory;
    }
    @Override
    public Integer getStudyId(int jobId) {
        return null;
    }

    @Override
    public QueryResult<Job> visit(int jobId) {
        return null;
    }

    @Override
    public QueryResult<Job> create(QueryOptions params, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<Job> read(Integer id, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<Job> readAll(QueryOptions query, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<Job> update(Integer id, ObjectMap parameters, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }

    @Override
    public QueryResult<Job> delete(Integer id, QueryOptions options, String sessionId) throws CatalogException {
        return null;
    }
}
