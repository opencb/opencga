package org.opencb.opencga.catalog.db.api;

import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.catalog.exceptions.CatalogAuthorizationException;
import org.opencb.opencga.catalog.exceptions.CatalogDBException;
import org.opencb.opencga.catalog.exceptions.CatalogParameterException;
import org.opencb.opencga.core.response.OpenCGAResult;

import java.util.List;

/**
 * Interface that needs to be implemented by all DBAdaptors that depend from Study.
 *
 * @param <T> Entry parameter.
 */
public interface CoreDBAdaptor<T> extends DBAdaptor<T> {

    OpenCGAResult<T> get(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;

    OpenCGAResult nativeGet(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;

    DBIterator<T> iterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;

    DBIterator nativeIterator(long studyUid, Query query, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;

    OpenCGAResult<Long> count(Query query, String user) throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<T> groupBy(Query query, List<String> fields, QueryOptions options, String user)
            throws CatalogDBException, CatalogAuthorizationException, CatalogParameterException;

    OpenCGAResult<?> distinct(long studyUid, String field, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;

    OpenCGAResult<?> distinct(long studyUid, List<String> fields, Query query, String userId)
            throws CatalogDBException, CatalogParameterException, CatalogAuthorizationException;
}
