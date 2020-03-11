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

import com.beust.jcommander.ParameterException;
import org.apache.commons.lang3.StringUtils;
import org.opencb.biodata.formats.variant.vcf4.VcfUtils;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
        addParam(query, VariantCatalogQueryUtils.FAMILY_SEGREGATION, queryVariantsOptions.modeOfInheritance);
        addParam(query, VariantCatalogQueryUtils.FAMILY_MEMBERS, queryVariantsOptions.familyMembers);
        addParam(query, VariantCatalogQueryUtils.FAMILY_PROBAND, queryVariantsOptions.familyProband);

        if (!VariantQueryUtils.isValidParam(query, VariantQueryParam.INCLUDE_FORMAT)
                && !VariantQueryUtils.isValidParam(query, VariantQueryParam.INCLUDE_GENOTYPE)
                && clientConfiguration != null
                && queryVariantsOptions.study != null) {
            List<String> formats = getFormats(queryVariantsOptions.study, clientConfiguration);
            if (formats != VcfUtils.DEFAULT_SAMPLE_FORMAT) {
                query.put(VariantQueryParam.INCLUDE_FORMAT.key(), formats);
            }
        }
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

    public static ParameterException variantFormatNotSupported(String outputFormat) {
        logger.error("Format '{}' not supported", outputFormat);
        return new ParameterException("Format '" + outputFormat + "' not supported");
    }


    private static List<String> getFormats(String study, ClientConfiguration clientConfiguration) {

        List<String> formats = new ArrayList<>();
//        List<String> formatTypes = new ArrayList<>();
//        List<Integer> formatArities = new ArrayList<>();
//        List<String> formatDescriptions = new ArrayList<>();

        if (clientConfiguration.getVariant() != null && clientConfiguration.getVariant().getIncludeFormats() != null) {
            String studyConfigAlias = null;
            if (clientConfiguration.getVariant().getIncludeFormats().get(study) != null) {
                studyConfigAlias = study;
            } else {
                // Search for the study alias
                if (clientConfiguration.getAlias() != null) {
                    for (Map.Entry<String, String> stringStringEntry : clientConfiguration.getAlias().entrySet()) {
                        if (stringStringEntry.getValue().contains(study)) {
                            studyConfigAlias = stringStringEntry.getKey();
                            logger.debug("Updating study name by alias (key) when including formats: from " + study + " to " + studyConfigAlias);
                            break;
                        }
                    }
                }
            }

            // create format arrays (names, types, arities, descriptions)
            String formatFields = clientConfiguration.getVariant().getIncludeFormats().get(studyConfigAlias);
            if (formatFields != null) {
                String[] fields = formatFields.split(",");
                for (String field : fields) {
                    String[] subfields = field.split(":");
                    if (subfields.length == 4) {
                        formats.add(subfields[0]);
//                        formatTypes.add(subfields[1]);
//                        if (StringUtils.isEmpty(subfields[2]) || !StringUtils.isNumeric(subfields[2])) {
//                            formatArities.add(1);
//                            logger.debug("Invalid arity for format " + subfields[0] + ", updating arity to 1");
//                        } else {
//                            formatArities.add(Integer.parseInt(subfields[2]));
//                        }
//                        formatDescriptions.add(subfields[3]);
                    } else {
                        // We do not need the extra information fields for "GT", "AD", "DP", "GQ", "PL".
                        formats.add(subfields[0]);
//                        formatTypes.add("");
//                        formatArities.add(0);
//                        formatDescriptions.add("");
                    }
                }
            } else {
                logger.debug("No formats found for: {}, setting default format: {}", study, VcfUtils.DEFAULT_SAMPLE_FORMAT);
                formats = VcfUtils.DEFAULT_SAMPLE_FORMAT;
            }
        } else {
            logger.debug("No formats found for: {}, setting default format: {}", study, VcfUtils.DEFAULT_SAMPLE_FORMAT);
            formats = VcfUtils.DEFAULT_SAMPLE_FORMAT;
        }
        return formats;
    }

}
