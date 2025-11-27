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

package org.opencb.opencga.storage.core.variant.adaptors;

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AdditionalAttribute;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.DataResult;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.metadata.models.StudyMetadata;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.query.ParsedVariantQuery;
import org.opencb.opencga.storage.core.variant.query.VariantQueryParser;
import org.opencb.opencga.storage.core.variant.query.VariantQueryResult;
import org.opencb.opencga.storage.core.variant.query.projection.VariantQueryProjectionParser;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author Ignacio Medina <igmecas@gmail.com>
 * @author Jacobo Coll <jacobo167@gmail.com>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface VariantDBAdaptor extends VariantIterable, AutoCloseable {

    String NATIVE = "native";
    String QUIET = "quiet";

    /**
     * Fetch all variants resulting of executing the query in the database. Returned fields are taken from
     * the 'include' and 'exclude' fields at options.
     *
     * @param variants Iterator with variants to filter
     * @param query    Query to be executed in the database to filter variants
     * @param options  Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A DataResult with the result of the query
     */
    default VariantQueryResult<Variant> get(Iterator<?> variants, Query query, QueryOptions options) {
        ParsedVariantQuery variantQuery = new VariantQueryParser(null, getMetadataManager()).parseQuery(query, options, true);
        try (VariantDBIterator iterator = iterator(variants, query, options)) {
            return iterator.toDataResult(variantQuery);
        } catch (Exception e) {
            throw VariantQueryException.internalException(e);
        }
    }

    @Deprecated
    default VariantDBIterator iterator(Query query, QueryOptions options) {
        return iterator(new VariantQueryParser(null, getMetadataManager()).parseQuery(query, options, true));
    }

    VariantDBIterator iterator(ParsedVariantQuery query);

    /**
     * Fetch all variants resulting of executing the query in the database. Returned fields are taken from
     * the 'include' and 'exclude' fields at options.
     *
     * @param query   Query to be executed in the database to filter variants
     * @return A DataResult with the result of the query
     */
    VariantQueryResult<Variant> get(ParsedVariantQuery query);

    /**
     * Fetch all variants resulting of executing the query in the database. Returned fields are taken from
     * the 'include' and 'exclude' fields at options.
     *
     * @param query   Query to be executed in the database to filter variants
     * @param options Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A DataResult with the result of the query
     */
    @Deprecated
    default VariantQueryResult<Variant> get(Query query, QueryOptions options) {
        return get(new VariantQueryParser(null, getMetadataManager()).parseQuery(query, options, true));
    }

    /**
     * Return all the variants in the same phase set for a given sample in a given variant.
     *
     * @param variant The main variant. See {@link Variant#toString()}
     * @param studyName Study of the sample
     * @param sampleName Sample name
     * @param options Other options
     * @param windowsSize Windows size for searching the phased variants.
     * @return A DataResult with the result of the query
     */
    VariantQueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options, int windowsSize);

    DataResult<VariantAnnotation> getAnnotation(String name, Query query, QueryOptions options);

    default DataResult<Long> count() {
        return count(new ParsedVariantQuery());
    }

    @Deprecated
    default DataResult<Long> count(Query query) {
        return count(new VariantQueryParser(null, getMetadataManager()).parseQuery(query, QueryOptions.empty(), true));
    }

    /**
     * Performs a distinct operation of the given field over the returned results.
     *
     * @param query Query to be executed in the database to filter variants
     * @return A DataResult with the all the distinct values
     */
    DataResult<Long> count(ParsedVariantQuery query);

    /**
     * Performs a distinct operation of the given field over the returned results.
     *
     * @param query Query to be executed in the database to filter variants
     * @param field Field to be distinct, it must be a valid QueryParams id
     * @return A DataResult with the all the distinct values
     */
    @Deprecated
    DataResult distinct(Query query, String field);

    /**
     * This methods calculates the number of variants at different equally-sized genome chunks. This can be renderer
     * as a histogram of the number of variants across a genomic region.
     *
     * @param query              Query to be executed in the database to filter variants
     * @param region             Region where to calculate the variant frequency
     * @param regionIntervalSize Size of the interval window, by default it is adjusted to return 200 chunks
     * @return Frequencies of queried variants
     */
    @Deprecated
    DataResult getFrequency(ParsedVariantQuery query, Region region, int regionIntervalSize);

    /**
     * This method ranks different entities with the most or the least number of variants. These entities
     * can be 'gene' or 'consequence_type' among others.
     *
     * @param query      Query to be executed in the database to filter variants
     * @param field      The entity to rank
     * @param numResults The max number of results to return
     * @param asc        Whether we want the top or the bottom part of the rank
     * @return A DataResult with a list of the entities and the number of elements
     */
    @Deprecated
    DataResult rank(Query query, String field, int numResults, boolean asc);

    @Deprecated
    DataResult groupBy(Query query, String field, QueryOptions options);

    @Deprecated
    DataResult groupBy(Query query, List<String> fields, QueryOptions options);

    /**
     * Returns all the possible samples to be returned by an specific query.
     *
     * @param query     Query to execute
     * @param options   Query Options
     * @return  Map key: StudyId, value: list of sampleIds
     */
    default Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options) {
        return VariantQueryProjectionParser.getIncludeSampleIds(query, options, getMetadataManager());
    }

    @Deprecated
    DataResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, long timestamp, QueryOptions queryOptions);

    @Deprecated
    DataResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyMetadata studyMetadata, long timestamp,
                            QueryOptions options);

    @Deprecated
    DataResult updateAnnotations(List<VariantAnnotation> variantAnnotations, long timestamp, QueryOptions queryOptions);


    @Deprecated
    DataResult updateCustomAnnotations(Query query, String name, AdditionalAttribute attribute, long timeStamp, QueryOptions options);

    VariantStorageMetadataManager getMetadataManager();

    void setVariantStorageMetadataManager(VariantStorageMetadataManager variantStorageMetadataManager);

    void close() throws IOException;
}
