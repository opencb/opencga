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

import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.commons.datastore.core.Query;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryParam;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.opencb.commons.datastore.core.QueryParam.Type.*;

/**
 * @author Ignacio Medina <igmecas@gmail.com>
 * @author Jacobo Coll <jacobo167@gmail.com>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface VariantDBAdaptor extends Iterable<Variant>, AutoCloseable {

    enum VariantQueryParams implements QueryParam {
        ID("ids", TEXT_ARRAY,
                "List of variant ids"),
        REGION("region", TEXT_ARRAY,
                "List of regions: {chr}:{start}-{end}"),
        CHROMOSOME("chromosome", TEXT_ARRAY,
                "List of chromosomes"),
        GENE("gene", TEXT_ARRAY,
                "List of genes"),
        TYPE("type", TEXT_ARRAY,
                "Variant type: [SNV, MNV, INDEL, SV, CNV]"),
        REFERENCE("reference", TEXT_ARRAY,
                "Reference allele"),
        ALTERNATE("alternate", TEXT_ARRAY,
                "Main alternate allele"),
        //EFFECT ("TEXT_ARRAY", null, ""),
        STUDIES("studies", TEXT_ARRAY, ""),
        RETURNED_STUDIES("returnedStudies", TEXT_ARRAY,
                "List of studies to be returned"),
        RETURNED_SAMPLES("returnedSamples", TEXT_ARRAY,
                "List of samples to be returned"),
        FILES("files", TEXT_ARRAY, ""),
        RETURNED_FILES("returnedFiles", TEXT_ARRAY,
                "List of files to be returned"),

        COHORTS("cohorts", TEXT_ARRAY,
                "Select variants with calculated stats for the selected cohorts"),
        STATS_MAF("maf", TEXT_ARRAY,
                "Minor Allele Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}"),
        STATS_MGF("mgf", TEXT_ARRAY,
                "Minor Genotype Frequency: [{study:}]{cohort}[<|>|<=|>=]{number}"),
        MISSING_ALLELES("missingAlleles", TEXT_ARRAY,
                "Number of missing alleles: [{study:}]{cohort}[<|>|<=|>=]{number}"),
        MISSING_GENOTYPES("missingGenotypes", TEXT_ARRAY,
                "Number of missing genotypes: [{study:}]{cohort}[<|>|<=|>=]{number}"),
        //[<study>:]<sample>:<genotype>[,<genotype>]*
        GENOTYPE("genotype", TEXT_ARRAY,
                "Samples with a specific genotype: {samp_1}:{gt_1}(,{gt_n})*(;{samp_n}:{gt_1}(,{gt_n})*)* e.g. HG0097:0/0;HG0098:0/1,1/1"),

        ANNOTATION_EXISTS("annotationExists", BOOLEAN,
                "Specify if the variant annotation must exists."),
        ANNOT_CONSEQUENCE_TYPE("annot-ct", TEXT_ARRAY,
                "Consequence type SO term list. e.g. SO:0000045,SO:0000046"),
        ANNOT_XREF("annot-xref", TEXT_ARRAY,
                "External references."),
        ANNOT_BIOTYPE("annot-biotype", TEXT_ARRAY,
                "Biotype"),
        ANNOT_POLYPHEN("polyphen", TEXT_ARRAY,
                "Polyphen, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. <=0.9 , =benign"),
        ANNOT_SIFT("sift", TEXT_ARRAY,
                "Sift, protein substitution score. [<|>|<=|>=]{number} or [~=|=|]{description} e.g. >0.1 , ~=tolerant"),
        ANNOT_PROTEIN_SUBSTITUTION("protein_substitution", TEXT_ARRAY,
                "Protein substitution score. {protein_score}[<|>|<=|>=]{number} or {protein_score}[~=|=|]{description} "
                        + "e.g. polyphen>0.1 , sift=tolerant"),
        ANNOT_CONSERVATION("conservation", TEXT_ARRAY,
                "Conservation score: {conservation_score}[<|>|<=|>=]{number}  e.g. phastCons>0.5,phylop<0.1,gerp>0.1"),
        ANNOT_POPULATION_ALTERNATE_FREQUENCY("alternate_frequency", TEXT_ARRAY,
                "Alternate Population Frequency: {study}:{population}[<|>|<=|>=]{number}"),
        ANNOT_POPULATION_REFERENCE_FREQUENCY("reference_frequency", TEXT_ARRAY,
                "Reference Population Frequency: {study}:{population}[<|>|<=|>=]{number}"),
        ANNOT_POPULATION_MINOR_ALLELE_FREQUENCY("annot-population-maf", TEXT_ARRAY,
                "Population minor allele frequency: {study}:{population}[<|>|<=|>=]{number}"),
        ANNOT_TRANSCRIPTION_FLAGS("annot-transcription-flags", TEXT_ARRAY,
                "List of transcript annotation flags. e.g. CCDS, basic, cds_end_NF, mRNA_end_NF, cds_start_NF, mRNA_start_NF, seleno"),
        ANNOT_GENE_TRAITS_ID("annot-gene-trait-id", TEXT_ARRAY,
                "List of gene trait association id. e.g. \"umls:C0007222\" , \"OMIM:269600\""),
        ANNOT_GENE_TRAITS_NAME("annot-gene-trait-name", TEXT_ARRAY,
                "List of gene trait association names. e.g. \"Cardiovascular Diseases\""),
        ANNOT_HPO("annot-hpo", TEXT_ARRAY,
                "List of HPO terms. e.g. \"HP:0000545\""),
        ANNOT_PROTEIN_KEYWORDS("annot-protein-keywords", TEXT_ARRAY,
                "List of protein variant annotation keywords"),
        ANNOT_DRUG("annot-drug", TEXT_ARRAY,
                "List of drug names"),
        ANNOT_FUNCTIONAL_SCORE("annot-functional-score", TEXT_ARRAY,
                "Functional score: {functional_score}[<|>|<=|>=]{number}  e.g. cadd_scaled>5.2 , cadd_raw<=0.3"),


        UNKNOWN_GENOTYPE("unknownGenotype", TEXT, "Returned genotype for unknown genotypes. Common values: [0/0, 0|0, ./.]");

        VariantQueryParams(String key, Type type, String description) {
            this.key = key;
            this.type = type;
            this.description = description;
        }

        private final String key;
        private Type type;
        private String description;

        @Override
        public String key() {
            return key;
        }

        @Override
        public String description() {
            return description;
        }

        @Override
        public Type type() {
            return type;
        }
    }

    /**
     * This method sets a data writer object for data serialization. When used no data will be return in
     * QueryResult object but written into the writer.
     *
     * @param dataWriter Deprecated param
     */
    @Deprecated
    void setDataWriter(DataWriter dataWriter);

    /**
     * This method inserts Variants into the given Study. If the Study already exists then it just adds the new Sample
     * genotypes, also new variants are inserted. If it is a new Study then Sample genotypes are added to the new Study.
     *
     * @param variants  List of variants in OpenCB data model to be inserted
     * @param studyName Name or alias of the study
     * @param options   Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with the number of inserted variants
     */
    QueryResult insert(List<Variant> variants, String studyName, QueryOptions options);

    /**
     * Delete all the variants from the database resulting of executing the query.
     *
     * @param query   Query to be executed in the database
     * @param options Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with the number of deleted variants
     */
    QueryResult delete(Query query, QueryOptions options);

    /**
     * Delete all the given samples belonging to the study from the database.
     *
     * @param studyName   The study name where samples belong to
     * @param sampleNames Sample names to be deleted, these must belong to the study
     * @param options     Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with a list with all the samples deleted
     */
    QueryResult deleteSamples(String studyName, List<String> sampleNames, QueryOptions options);

    /**
     * Delete the given file from the database with all the samples it has.
     *
     * @param studyName The study where the file belong
     * @param fileName  The file name to be deleted, it must belong to the study
     * @param options   Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with the file deleted
     */
    QueryResult deleteFile(String studyName, String fileName, QueryOptions options);

    /**
     * Delete the given study from the database.
     *
     * @param studyName The study name to delete
     * @param options   Query modifiers, accepted values are: purge
     * @return A QueryResult with the study deleted
     */
    QueryResult deleteStudy(String studyName, QueryOptions options);


    /**
     * Fetch all variants resulting of executing the query in the database. Returned fields are taken from
     * the 'include' and 'exclude' fields at options.
     *
     * @param query   Query to be executed in the database to filter variants
     * @param options Query modifiers, accepted values are: include, exclude, limit, skip, sort and count
     * @return A QueryResult with the result of the query
     */
    QueryResult<Variant> get(Query query, QueryOptions options);

    /**
     * Fetch all variants resulting of executing all the queries in the database. Returned fields are taken from
     * the 'include' and 'exclude' fields at options.
     *
     * @param queries List of queries to be executed in the database to filter variants
     * @param options Query modifiers, accepted values are: include, exclude, limit, skip, sort and count.
     * @return A list of QueryResult with the result of the queries
     */
    List<QueryResult<Variant>> get(List<Query> queries, QueryOptions options);

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
    QueryResult<Variant> getPhased(String variant, String studyName, String sampleName, QueryOptions options, int windowsSize);

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

    @Override
    VariantDBIterator iterator();

    VariantDBIterator iterator(Query query, QueryOptions options);

    @Override
    void forEach(Consumer<? super Variant> action);

    void forEach(Query query, Consumer<? super Variant> action, QueryOptions options);


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

    List<Integer> getReturnedStudies(Query query, QueryOptions options);
    /**
     * Returns all the possible samples to be returned by an specific query.
     *
     * @param query     Query to execute
     * @param options   Query Options
     * @return  Map key: StudyId, value: list of sampleIds
     */
    Map<Integer, List<Integer>> getReturnedSamples(Query query, QueryOptions options);

    QueryResult addStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions);

    QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, String studyName, QueryOptions queryOptions);

    QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration, QueryOptions options);

    QueryResult deleteStats(String studyName, String cohortName, QueryOptions options);


    default void preUpdateAnnotations() throws IOException {
    }

    QueryResult addAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions);

    QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions);

    QueryResult deleteAnnotation(String annotationId, Query query, QueryOptions queryOptions);


    void close() throws IOException;


    /**
     * Given a genomic region, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param options Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    @Deprecated
    default QueryResult<Variant> getAllVariants(QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default QueryResult<Variant> getVariantById(String id, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default List<QueryResult<Variant>> getAllVariantsByIdList(List<String> idList, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    /**
     * Given a genomic region, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param region  The region where variants must be searched
     * @param options Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    @Deprecated
    default QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default QueryResult groupBy(String field, QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    default VariantSourceDBAdaptor getVariantSourceDBAdaptor() {
        throw new UnsupportedOperationException();
    }

    StudyConfigurationManager getStudyConfigurationManager();

    void setStudyConfigurationManager(StudyConfigurationManager studyConfigurationManager);

    @Deprecated
    default VariantDBIterator iterator(QueryOptions options) {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    default QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, int studyId, QueryOptions queryOptions) {
        throw new UnsupportedOperationException();
    }

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
