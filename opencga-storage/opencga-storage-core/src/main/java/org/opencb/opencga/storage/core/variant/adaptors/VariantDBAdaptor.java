/*
 * Copyright 2015 OpenCB
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

import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.Query;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryParam;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;

import java.util.*;
import java.util.function.Consumer;

import static org.opencb.datastore.core.QueryParam.Type.*;

/**
 * @author Ignacio Medina <igmecas@gmail.com>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface VariantDBAdaptor extends Iterable<Variant> {

    enum VariantQueryParams implements QueryParam {
        ID ("ids", TEXT_ARRAY, ""),
        REGION ("region", TEXT_ARRAY, ""),
        CHROMOSOME ("chromosome", TEXT_ARRAY, ""),
        GENE ("gene", TEXT_ARRAY, ""),
        TYPE ("type", TEXT_ARRAY, ""),
        REFERENCE ("reference", TEXT_ARRAY, ""),
        ALTERNATE ("alternate", TEXT_ARRAY, ""),
        //EFFECT ("TEXT_ARRAY", null, ""),
        STUDIES ("studies", TEXT_ARRAY, ""),
        FILES ("files", TEXT_ARRAY, ""),
        FILE_ID ("fileId", TEXT_ARRAY, ""),
        MAF ("maf", TEXT_ARRAY, ""),
        MGF ("mgf", TEXT_ARRAY, ""),
        MISSING_ALLELES ("missingAlleles", TEXT_ARRAY, ""),
        MISSING_GENOTYPES ("missingGenotypes", TEXT_ARRAY, ""),
        ANNOTATION_EXISTS ("annotationExists", TEXT_ARRAY, ""),
        GENOTYPE ("genotype", TEXT_ARRAY, ""),
        ANNOT_CONSEQUENCE_TYPE ("annot-ct", TEXT_ARRAY, ""),
        ANNOT_XREF ("annot-xref", TEXT_ARRAY, ""),
        ANNOT_BIOTYPE ("annot-biotype", TEXT_ARRAY, ""),
        POLYPHEN ("polyphen", TEXT_ARRAY, ""),
        SIFT ("sift", TEXT_ARRAY, ""),
        PROTEIN_SUBSTITUTION ("protein_substitution", TEXT_ARRAY, ""),
        CONSERVED_REGION ("conserved_region", TEXT_ARRAY, ""),
        ALTERNATE_FREQUENCY ("alternate_frequency", TEXT_ARRAY, ""),
        REFERENCE_FREQUENCY ("reference_frequency", TEXT_ARRAY, ""),
        ;

        VariantQueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        private final String key;
        private Type type;
        private String description;

        @Override public String key() {return key;}
        @Override public String description() {return description;}
        @Override public Type type() {return type;}
    }

    /**
     * This method set a data writer object for data serialization. When used no data will be return in
     * QueryResult object.
     */
    void setDataWriter(DataWriter dataWriter);

    /**
     * This method inserts Variants into the given Study. If the Study already exists then it just adds the new Sample
     * genotypes, also new variants are inserted. If it is a new Study then Sample genotypes are added to the new Study.
     * @param variants List of variants in OpenCB data model to be inserted
     * @param studyName Name or alias of the study
     * @param options Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return QueryResult with the number of inserted variants
     */
    QueryResult insert(List<Variant> variants, String studyName, QueryOptions options);

    QueryResult delete(Query query, QueryOptions options);

    QueryResult deleteSamples(String studyName, List<String> sampleNames, QueryOptions queryOptions);

    QueryResult deleteFile(String studyName, String fileName, QueryOptions queryOptions);

    QueryResult deleteStudy(String studyName, QueryOptions queryOptions);



    QueryResult get(Query query, QueryOptions options);

    List<QueryResult> get(List<Query> queries, QueryOptions options);

    QueryResult distinct(Query query, String field, QueryOptions options);

    @Override
    VariantDBIterator iterator();

    VariantDBIterator iterator(Query query, QueryOptions options);

    @Override
    void forEach(Consumer<Variant> action);

    void forEach(Query query, Consumer<Variant> action, QueryOptions options);



    QueryResult getFrequency(Query query, Region region, int regionIntervalSize, QueryOptions options);

    QueryResult rank(Query query, String field, int numResults, boolean asc, QueryOptions options);

    QueryResult groupBy(Query query, String field, QueryOptions options);

    QueryResult groupBy(Query query, List<String> fields, QueryOptions options);



    QueryResult addStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions);

    QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions);

    QueryResult deleteStats(String studyName, String cohortName, QueryOptions options);



    QueryResult addAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions);

    QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions);

    QueryResult deleteAnnotation(String studyName, int annotationId, QueryOptions queryOptions);


    boolean close();


    /**
     * Given a genomic region, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param options   Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    @Deprecated
    QueryResult<Variant> getAllVariants(QueryOptions options);

    @Deprecated
    QueryResult<Variant> getVariantById(String id, QueryOptions options);

    @Deprecated
    List<QueryResult<Variant>> getAllVariantsByIdList(List<String> idList, QueryOptions options);

    /**
     * Given a genomic region, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param region    The region where variants must be searched
     * @param options   Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    @Deprecated
    QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options);

    /**
     * @deprecated Use "getAllVariants" with VariantDBAdaptor.REGION filter instead.
     */
    @Deprecated
    List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options);

    @Deprecated
    QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options);

    @Deprecated
    QueryResult groupBy(String field, QueryOptions options);


    VariantSourceDBAdaptor getVariantSourceDBAdaptor();

//    @Override
//    VariantDBIterator iterator();

    @Deprecated
    VariantDBIterator iterator(QueryOptions options);

    @Deprecated
    QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, int studyId, QueryOptions queryOptions);

//    @Deprecated
//    QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions);


//    @Deprecated
//    QueryResult getAllVariantsByRegionAndStudies(Region region, List<String> studyIds, QueryOptions options);
//    @Deprecated
//    QueryResult getAllVariantsByGene(String geneName, QueryOptions options);
//    @Deprecated
//    QueryResult getMostAffectedGenes(int numGenes, QueryOptions options);
//    @Deprecated
//    QueryResult getLeastAffectedGenes(int numGenes, QueryOptions options);
//    @Deprecated
//    QueryResult getTopConsequenceTypes(int numConsequenceTypes, QueryOptions options);
//    @Deprecated
//    QueryResult getBottomConsequenceTypes(int numConsequenceTypes, QueryOptions options);

}
