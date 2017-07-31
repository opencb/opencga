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

package org.opencb.opencga.storage.core.manager.variant;

import org.apache.commons.lang.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.opencga.catalog.db.api.SampleDBAdaptor;
import org.opencb.opencga.catalog.exceptions.CatalogException;
import org.opencb.opencga.catalog.managers.CatalogManager;
import org.opencb.opencga.catalog.models.Sample;
import org.opencb.opencga.storage.core.manager.CatalogUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;

import java.util.List;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.*;

/**
 * Created on 28/02/17.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantCatalogQueryUtils extends CatalogUtils {

    public static final String SAMPLE_FILTER_DESC =
            "Selects some samples using metadata information from Catalog. e.g. age>20;ontologies=hpo:123,hpo:456;name=smith";
    public static final QueryParam SAMPLE_FILTER = QueryParam.create("sampleFilter", SAMPLE_FILTER_DESC, QueryParam.Type.TEXT_ARRAY);
//    public static final QueryParam SAMPLE_FILTER_GENOTYPE = QueryParam.create("sampleFilterGenotype", "", QueryParam.Type.TEXT_ARRAY);

    public VariantCatalogQueryUtils(CatalogManager catalogManager) {
        super(catalogManager);
    }

    /**
     * Transforms a high level Query to a query fully understandable by storage.
     * @param query     High level query. Will be modified by the method.
     * @param sessionId User's session id
     * @return          Modified input query (same instance)
     * @throws CatalogException if there is any catalog error
     */
    public Query parseQuery(Query query, String sessionId) throws CatalogException {
        if (query == null) {
            // Nothing to do!
            return null;
        }
        List<Long> studies = getStudies(query, VariantQueryParam.STUDIES, sessionId);
        String defaultStudyStr;
        long defaultStudyId = -1;
        if (studies.size() == 1) {
            defaultStudyId = studies.get(0);
            defaultStudyStr = String.valueOf(studies.get(0));
        } else {
            defaultStudyStr = null;
        }

        transformFilter(query, VariantQueryParam.STUDIES, value -> catalogManager.getStudyId(value, sessionId));
        transformFilter(query, VariantQueryParam.RETURNED_STUDIES, value -> catalogManager.getStudyId(value, sessionId));
        transformFilter(query, VariantQueryParam.COHORTS, value ->
                catalogManager.getCohortManager().getId(value, defaultStudyStr, sessionId).getResourceId());
        transformFilter(query, VariantQueryParam.FILES, value ->
                catalogManager.getFileId(value, defaultStudyStr, sessionId));
        transformFilter(query, VariantQueryParam.RETURNED_FILES, value ->
                catalogManager.getFileId(value, defaultStudyStr, sessionId));
        // TODO: Parse returned sample filter and add genotype filter

        if (isValidParam(query, SAMPLE_FILTER)) {
            String sampleAnnotation = query.getString(SAMPLE_FILTER.key());
            Query sampleQuery = parseSampleAnnotationQuery(sampleAnnotation, SampleDBAdaptor.QueryParams::getParam);
            sampleQuery.append(SampleDBAdaptor.QueryParams.STUDY_ID.key(), defaultStudyId);
            QueryOptions options = new QueryOptions(QueryOptions.INCLUDE, SampleDBAdaptor.QueryParams.ID);
            List<Long> sampleIds = catalogManager.getAllSamples(defaultStudyId, sampleQuery, options, sessionId).getResult().stream()
                    .map(Sample::getId)
                    .collect(Collectors.toList());

            if (sampleIds.isEmpty()) {
                throw new VariantQueryException("Could not found samples with this annotation: " + sampleAnnotation);
            }

            String genotype = query.getString("sampleAnnotationGenotype");
//            String genotype = query.getString(VariantDBAdaptor.VariantQueryParams.GENOTYPE.key());
            if (StringUtils.isNotBlank(genotype)) {
                StringBuilder sb = new StringBuilder();
                for (Long sampleId : sampleIds) {
                    sb.append(sampleId).append(IS)
                            .append(genotype)
                            .append(AND); // TODO: Should this be an AND (;) or an OR (,)?
                }
                query.append(VariantQueryParam.GENOTYPE.key(), sb.toString());
                if (!isValidParam(query, VariantQueryParam.RETURNED_SAMPLES)) {
                    query.append(VariantQueryParam.RETURNED_SAMPLES.key(), sampleIds);
                }
            } else {
                query.append(VariantQueryParam.SAMPLES.key(), sampleIds);
            }
        }

        return query;
    }
}
