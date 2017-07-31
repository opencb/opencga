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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.catalog.db.api.DBIterator;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.AbstractManager;
import org.opencb.opencga.catalog.models.Annotation;
import org.opencb.opencga.catalog.models.File;
import org.opencb.opencga.catalog.models.Individual;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.catalog.models.acls.permissions.SampleAclEntry;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
@Deprecated
public interface ISampleManager extends ResourceManager<Long, Sample>, IAnnotationSetManager {

    Long getStudyId(long sampleId) throws CatalogException;

    /**
     * Obtains the numeric sample id given a string.
     *
     * @param userId User id of the user asking for the sample id.
     * @param sampleStr Sample id in string format. Could be one of [id | user@aliasProject:aliasStudy:sampleName
     *                | user@aliasStudy:sampleName | aliasStudy:sampleName | sampleName].
     * @return the numeric sample id.
     * @throws CatalogException when more than one sample id is found or the study or project ids cannot be resolved.
     */
    Long getId(String userId, String sampleStr) throws CatalogException;

    /**
     * Obtains the list of sampleIds corresponding to the comma separated list of sample strings given in sampleStr.
     *
     * @param userId User demanding the action.
     * @param sampleStr Comma separated list of sample ids.
     * @return A list of sample ids.
     * @throws CatalogException CatalogException.
     */
    default List<Long> getIds(String userId, String sampleStr) throws CatalogException {
        List<Long> sampleIds = new ArrayList<>();
        for (String sampleId : sampleStr.split(",")) {
            sampleIds.add(getId(userId, sampleId));
        }
        return sampleIds;
    }

    @Deprecated
    Long getId(String fileId) throws CatalogException;

    QueryResult<Sample> create(String studyStr, Sample sample, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Sample> create(String studyStr, String name, String source, String description, String type, boolean somatic,
                               Individual individual, Map<String, Object> attributes, QueryOptions options, String sessionId)
            throws CatalogException;

    @Deprecated
    QueryResult<Sample> create(String studyStr, String name, String source, String description, Map<String, Object> attributes,
                               QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param sampleStr Sample id in string format. Could be either the id or alias.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException when more than one sample id is found.
     */
    AbstractManager.MyResourceId getId(String sampleStr, @Nullable String studyStr, String sessionId) throws CatalogException;

    /**
     * Obtains the resource java bean containing the requested ids.
     *
     * @param sampleStr Sample id in string format. Could be either the id or alias.
     * @param studyStr Study id in string format. Could be one of [id|user@aliasProject:aliasStudy|aliasProject:aliasStudy|aliasStudy].
     * @param sessionId Session id of the user logged.
     * @return the resource java bean containing the requested ids.
     * @throws CatalogException CatalogException.
     */
    AbstractManager.MyResourceIds getIds(String sampleStr, @Nullable String studyStr, String sessionId) throws CatalogException;


    /**
     * Delete entries from Catalog.
     *
     * @param ids       Comma separated list of ids corresponding to the objects to delete
     * @param studyStr  Study string.
     * @param options   Deleting options.
     * @param sessionId sessionId
     * @return A list with the deleted objects
     * @throws CatalogException CatalogException
     * @throws IOException IOException.
     */
    List<QueryResult<Sample>> delete(String ids, @Nullable String studyStr, QueryOptions options, String sessionId)
            throws CatalogException, IOException;

    void checkCanDeleteSamples(AbstractManager.MyResourceIds resources) throws CatalogException;

    QueryResult<Annotation> load(File file) throws CatalogException;

    QueryResult<Sample> get(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    DBIterator<Sample> iterator(long studyId, Query query, QueryOptions options, String sessionId) throws CatalogException;

    /**
     * Multi-study search of samples in catalog.
     *
     * @param studyStr Study string that can point to several studies of the same project.
     * @param query    Query object.
     * @param options  QueryOptions object.
     * @param sessionId Session id.
     * @return The list of samples matching the query.
     * @throws CatalogException catalogException.
     */
    QueryResult<Sample> search(String studyStr, Query query, QueryOptions options, String sessionId) throws CatalogException;

    QueryResult<Sample> count(String studyStr, Query query, String sessionId) throws CatalogException;

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
        long studyId = query.getLong(SampleDBAdaptor.QueryParams.STUDY_ID.key());
        if (studyId == 0L) {
            throw new CatalogException("Sample[rank]: Study id not found in the query");
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

    List<QueryResult<SampleAclEntry>> updateAcl(String sample, String studyStr, String memberId, Sample.SampleAclParams sampleAclParams,
                                                String sessionId) throws CatalogException;
}
