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

package org.opencb.opencga.app.cli.internal.executors;

import org.apache.commons.lang3.StringUtils;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.app.cli.internal.options.VariantCommandOptions;
import org.opencb.opencga.client.config.ClientConfiguration;
import org.opencb.opencga.analysis.variant.manager.VariantCatalogQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantField;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryParam;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory;
import org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import static org.opencb.opencga.storage.core.variant.io.VariantWriterFactory.VariantOutputFormat.VCF;

/**
 * Created by imedina on 30/12/15.
 */
public class VariantQueryCommandUtils extends org.opencb.opencga.storage.app.cli.client.executors.VariantQueryCommandUtils {

    private static Logger logger = LoggerFactory.getLogger(VariantQueryCommandUtils.class);

    public static Query parseQuery(VariantCommandOptions.VariantQueryCommandOptions queryVariantsOptions, Map<Long, String> studyIds, ClientConfiguration clientConfiguration)
            throws Exception {
        return parseQuery(queryVariantsOptions, studyIds.values(), clientConfiguration);
    }

    public static Query parseQuery(VariantCommandOptions.AbstractVariantQueryCommandOptions queryVariantsOptions, Collection<String> studies, ClientConfiguration clientConfiguration)
            throws IOException {

        if ("TEXT".equalsIgnoreCase(queryVariantsOptions.commonOptions.outputFormat)) {
            queryVariantsOptions.commonOptions.outputFormat = VCF.name();
        }

        VariantOutputFormat of = VariantWriterFactory.toOutputFormat(queryVariantsOptions.commonOptions.outputFormat, queryVariantsOptions.outputFileName);
        Query query = parseGenericVariantQuery(
                queryVariantsOptions.genericVariantQueryOptions, queryVariantsOptions.study, studies,
                queryVariantsOptions.numericOptions.count, of);

        addParam(query, VariantCatalogQueryUtils.SAMPLE_ANNOTATION, queryVariantsOptions.sampleAnnotation);
        addParam(query, VariantCatalogQueryUtils.PROJECT, queryVariantsOptions.project);
        addParam(query, VariantCatalogQueryUtils.FAMILY, queryVariantsOptions.family);
        addParam(query, VariantCatalogQueryUtils.FAMILY_DISORDER, queryVariantsOptions.familyPhenotype);
        addParam(query, VariantCatalogQueryUtils.FAMILY_SEGREGATION, queryVariantsOptions.familySegregation);
        addParam(query, VariantCatalogQueryUtils.FAMILY_MEMBERS, queryVariantsOptions.familyMembers);
        addParam(query, VariantCatalogQueryUtils.FAMILY_PROBAND, queryVariantsOptions.familyProband);
        addParam(query, VariantCatalogQueryUtils.PANEL, queryVariantsOptions.panel);
        addParam(query, VariantCatalogQueryUtils.SAVED_FILTER, queryVariantsOptions.savedFilter);

        if (!VariantQueryUtils.isValidParam(query, VariantQueryParam.UNKNOWN_GENOTYPE)
                && clientConfiguration != null
                && clientConfiguration.getVariant() != null
                && clientConfiguration.getVariant().getUnknownGenotype() != null
                ) {
            query.put(VariantQueryParam.UNKNOWN_GENOTYPE.key(), clientConfiguration.getVariant().getUnknownGenotype());
        }

        return query;
    }

    public static QueryOptions parseQueryOptions(VariantCommandOptions.AbstractVariantQueryCommandOptions queryVariantsOptions) {
        QueryOptions queryOptions = new QueryOptions();

        if (StringUtils.isNotEmpty(queryVariantsOptions.dataModelOptions.include)) {
            queryOptions.add(QueryOptions.INCLUDE, queryVariantsOptions.dataModelOptions.include);
        }

        if (StringUtils.isNotEmpty(queryVariantsOptions.dataModelOptions.exclude)) {
            queryOptions.add(QueryOptions.EXCLUDE, queryVariantsOptions.dataModelOptions.exclude);
        }

        if (queryVariantsOptions.numericOptions.skip > 0) {
            queryOptions.add(QueryOptions.SKIP, queryVariantsOptions.numericOptions.skip);
        }

        if (queryVariantsOptions.numericOptions.limit > 0) {
            queryOptions.add(QueryOptions.LIMIT, queryVariantsOptions.numericOptions.limit);
        }

        if (queryVariantsOptions.numericOptions.count) {
            queryOptions.add(QueryOptions.COUNT, true);
        }

        if (queryVariantsOptions.genericVariantQueryOptions.summary) {
            queryOptions.add(VariantField.SUMMARY, true);
        }

        if (queryVariantsOptions.genericVariantQueryOptions.sort) {
            queryOptions.put(QueryOptions.SORT, true);
        }

        queryOptions.putAll(queryVariantsOptions.commonOptions.params);

        return queryOptions;
    }

}
