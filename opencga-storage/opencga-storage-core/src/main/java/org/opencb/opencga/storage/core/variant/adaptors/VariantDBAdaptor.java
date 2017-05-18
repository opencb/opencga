/*
 * Copyright 2015-2016 OpenCB
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
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.core.results.VariantQueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author Ignacio Medina <igmecas@gmail.com>
 * @author Jacobo Coll <jacobo167@gmail.com>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface VariantDBAdaptor extends VariantIterable, AutoCloseable {

    /**
     * This method inserts Variants into the given Study. If the Study already exists then it just adds the new Sample
     * genotypes, also new variants are inserted. If it is a new Study then Sample genotypes are added to the new Study.
     *
     * @param variants  List of variants in OpenCB data model to be inserted
     * @param studyName Name or alias of the study
     * @param options   Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with the number of inserted variants
     */
    @Deprecated
    default QueryResult insert(List<Variant> variants, String studyName, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    /**
     * Delete all the variants from the database resulting of executing the query.
     *
     * @param query   Query to be executed in the database
     * @param options Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with the number of deleted variants
     */
    @Deprecated
    QueryResult delete(Query query, QueryOptions options);

    /**
     * Delete all the given samples belonging to the study from the database.
     *
     * @param studyName   The study name where samples belong to
     * @param sampleNames Sample names to be deleted, these must belong to the study
     * @param options     Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with a list with all the samples deleted
     */
    @Deprecated
    QueryResult deleteSamples(String studyName, List<String> sampleNames, QueryOptions options);

    /**
     * Delete the given file from the database with all the samples it has.
     *
     * @param studyName The study where the file belong
     * @param fileName  The file name to be deleted, it must belong to the study
     * @param options   Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with the file deleted
     */
    @Deprecated
    QueryResult deleteFile(String studyName, String fileName, QueryOptions options);

    /**
     * Delete the given study from the database.
     *
     * @param studyName The study name to delete
     * @param options   Query modifiers, accepted values are: purge
     * @return A QueryResult with the study deleted
     */
    @Deprecated
    QueryResult deleteStudy(String studyName, QueryOptions options);


    /**
     * Fetch all variants resulting of executing the query in the database. Returned fields are taken from
     * the 'include' and 'exclude' fields at options.
     *
     * @param query   Query to be executed in the database to filter variants
     * @param options Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with the result of the query
     */
    VariantQueryResult<Variant> get(Query query, QueryOptions options);

    /**
     * Fetch all variants resulting of executing all the queries in the database. Returned fields are taken from
     * the 'include' and 'exclude' fields at options.
     *
     * @param queries List of queries to be executed in the database to filter variants
     * @param options Query modifiers, accepted values are: include, exclude, limit, skip, sort and count.
     * @return A list of QueryResult with the result of the queries
     */
    List<VariantQueryResult<Variant>> get(List<Query> queries, QueryOptions options);

    /**
     * Return all the variants in the same phase set for a given sample in a given variant.
     *
     * @param variant The main variant. See {@link Variant#toString()}
     * @param studyName Study of the sample
     * @param sampleName Sample name
     * @param options Other options
     * @param windowsSize Windows size for searching the phased variants.
     * @return A QueryResult with the result of the query
     */
    VariantQueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options, int windowsSize);

    /**
     * Performs a distinct operation of the given field over the returned results.
     *
     * @param query Query to be executed in the database to filter variants
     * @return A QueryResult with the all the distinct values
     */
    QueryResult<Long> count(Query query);

    /**
     * Performs a distinct operation of the given field over the returned results.
     *
     * @param query Query to be executed in the database to filter variants
     * @param field Field to be distinct, it must be a valid QueryParams id
     * @return A QueryResult with the all the distinct values
     */
    QueryResult distinct(Query query, String field);

    /**
     * This methods calculates the number of variants at different equally-sized genome chunks. This can be renderer
     * as a histogram of the number of variants across a genomic region.
     *
     * @param query              Query to be executed in the database to filter variants
     * @param region             Region where to calculate the variant frequency
     * @param regionIntervalSize Size of the interval window, by default it is adjusted to return 200 chunks
     * @return Frequencies of queried variants
     */
    QueryResult getFrequency(Query query, Region region, int regionIntervalSize);

    /**
     * This method ranks different entities with the most or the least number of variants. These entities
     * can be 'gene' or 'consequence_type' among others.
     *
     * @param query      Query to be executed in the database to filter variants
     * @param field      The entity to rank
     * @param numResults The max number of results to return
     * @param asc        Whether we want the top or the bottom part of the rank
     * @return A QueryResult with a list of the entities and the number of elements
     */
    QueryResult rank(Query query, String field, int numResults, boolean asc);

    QueryResult groupBy(Query query, String field, QueryOptions options);

    QueryResult groupBy(Query query, List<String> fields, QueryOptions options);

    default List<Integer> getReturnedStudies(Query query, QueryOptions options) {
        return VariantQueryUtils.getReturnedStudies(query, options, getStudyConfigurationManager());
    }
    /**
     * Returns all the possible samples to be returned by an specific query.
     *
     * @param query     Query to execute
     * @param options   Query Options
     * @return  Map key: StudyId, value: list of sampleIds
     */
    default Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options) {
        return VariantQueryUtils.getReturnedSamples(query, options, getStudyConfigurationManager());
    }

    @Deprecated
    default QueryResult addStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions) {
        return updateStats(variantStatsWrappers, studyName, queryOptions);
    }

    QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions);

    QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration, QueryOptions options);

    QueryResult deleteStats(String studyName, String cohortName, QueryOptions options);

    QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions);

    /**
     * Update custom annotation for all the variants with in a given region.
     *
     * @param query       Region to update
     * @param name        Custom annotation name.
     * @param attribute   Custom annotation for the region
     * @param options     Other options
     * @return            Result of the insertion
     */
    QueryResult updateCustomAnnotations(Query query, String name, AdditionalAttribute attribute, QueryOptions options);

    VariantSourceDBAdaptor getVariantSourceDBAdaptor();

    StudyConfigurationManager getStudyConfigurationManager();

    void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager);

    void close() throws IOException;
}
