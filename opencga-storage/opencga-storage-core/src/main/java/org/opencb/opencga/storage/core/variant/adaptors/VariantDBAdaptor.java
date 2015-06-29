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

import java.util.*;

import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;

/**
 * @author Ignacio Medina <igmecas@gmail.com>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface VariantDBAdaptor extends Iterable<Variant> {

    String ID = "ids";
    String REGION = "region";
    String CHROMOSOME = "chromosome";
    String GENE = "gene";
    String TYPE = "type";
    String REFERENCE = "reference";
    String ALTERNATE = "alternate";
    String EFFECT = "effect";
    String STUDIES = "studies";
    String FILES = "files";
    String FILE_ID = "fileId";
    String MAF = "maf";
    String MGF = "mgf";
    String MISSING_ALLELES = "missingAlleles";
    String MISSING_GENOTYPES = "missingGenotypes";
    String ANNOTATION_EXISTS = "annotationExists";
    String GENOTYPE = "genotype";
    String ANNOT_CONSEQUENCE_TYPE = "annot-ct";
    String ANNOT_XREF = "annot-xref";
    String ANNOT_BIOTYPE = "annot-biotype";
    String POLYPHEN = "polyphen";
    String SIFT = "sift";
    String PROTEIN_SUBSTITUTION = "protein_substitution";
    String CONSERVED_REGION = "conserved_region";
    String ALTERNATE_FREQUENCY = "alternate_frequency";
    String REFERENCE_FREQUENCY = "reference_frequency";
    String MERGE = "merge";
    String SORT = "sort";

    class QueryParams {
        public static final Set<String> acceptedValues;
        static {
            acceptedValues = new HashSet<>();
            acceptedValues.add(ID);
            acceptedValues.add(REGION);
            acceptedValues.add(CHROMOSOME);
            acceptedValues.add(GENE);
            acceptedValues.add(TYPE);
            acceptedValues.add(REFERENCE);
            acceptedValues.add(ALTERNATE);
            acceptedValues.add(EFFECT);
            acceptedValues.add(STUDIES);
            acceptedValues.add(FILES);
            acceptedValues.add(FILE_ID);
            acceptedValues.add(MAF);
            acceptedValues.add(MGF);
            acceptedValues.add(MISSING_ALLELES);
            acceptedValues.add(MISSING_GENOTYPES);
            acceptedValues.add(ANNOTATION_EXISTS);
            acceptedValues.add(GENOTYPE);
            acceptedValues.add(ANNOT_CONSEQUENCE_TYPE);
            acceptedValues.add(ANNOT_XREF);
            acceptedValues.add(ANNOT_BIOTYPE);
            acceptedValues.add(POLYPHEN);
            acceptedValues.add(SIFT);
            acceptedValues.add(PROTEIN_SUBSTITUTION);
            acceptedValues.add(CONSERVED_REGION);
            acceptedValues.add(MERGE);
        }

        //TODO: Think about this
        public static QueryOptions checkQueryOptions(QueryOptions options) throws Exception {
            QueryOptions filteredQueryOptions = new QueryOptions(options);
            Iterator<Map.Entry<String, Object>> iterator = filteredQueryOptions.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Object> entry = iterator.next();
                if (acceptedValues.contains(entry.getKey())) {
                    if (entry.getValue() == null || entry.toString().isEmpty()) {
                        iterator.remove();
                    } else {
                        //TODO: check type
                    }
                } else {
                    iterator.remove();
                    System.out.println("Unknown query param " + entry.getKey());
                }
            }
            return filteredQueryOptions;
        }
    }

    /**
     * This method set a data writer object for data serialization. When used no data will be return in
     * QueryResult object.
     */
    void setDataWriter(DataWriter dataWriter);

    /**
     * Given a genomic region, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param options   Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    QueryResult<Variant> getAllVariants(QueryOptions options);


    QueryResult<Variant> getVariantById(String id, QueryOptions options);

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
    QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options);

    /**
     * @deprecated Use "getAllVariants" with VariantDBAdaptor.REGION filter instead.
     */
    @Deprecated
    List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options);


    QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options);

    QueryResult groupBy(String field, QueryOptions options);


    VariantSourceDBAdaptor getVariantSourceDBAdaptor();

    @Override
    VariantDBIterator iterator();

    VariantDBIterator iterator(QueryOptions options);

    //QueryResult insert(List<Variant> variants, StudyConfiguration studyConfiguration, QueryOptions queryOptions);

    //QueryResult addStats(List<VariantStatsWrapper> variantStatsWrappers, int studyId, QueryOptions queryOptions);

    QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, int studyId, QueryOptions queryOptions);

    //QueryResult addAnnotation(List<VariantAnnotation> variantAnnotation, QueryOptions queryOptions);

    QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions);

    //QueryResult deleteStudy(int studyId, QueryOptions queryOptions);

    //QueryResult deleteSamples(List<Integer> sampleIds, int studyId, QueryOptions queryOptions);

    //QueryResult deleteSamples(List<String> sampleIds, StudyConfiguration studyConfiguration, QueryOptions queryOptions);

    //QueryResult deleteStats(int studyId, int cohortId, QueryOptions queryOptions);

    //QueryResult deleteAnnotation(int annotationId, int studyId, QueryOptions queryOptions);

    boolean close();


    /**
     * Given a genomic region and studies IDs, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param region    The region where variants must be searched
     * @param studyIds   The identifier of the studies where variants are classified
     * @param options   Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    @Deprecated
    QueryResult getAllVariantsByRegionAndStudies(Region region, List<String> studyIds, QueryOptions options);


    @Deprecated
    QueryResult getAllVariantsByGene(String geneName, QueryOptions options);


    @Deprecated
    QueryResult getMostAffectedGenes(int numGenes, QueryOptions options);
    @Deprecated
    QueryResult getLeastAffectedGenes(int numGenes, QueryOptions options);


    @Deprecated
    QueryResult getTopConsequenceTypes(int numConsequenceTypes, QueryOptions options);
    @Deprecated
    QueryResult getBottomConsequenceTypes(int numConsequenceTypes, QueryOptions options);
    
//    QueryResult getStatsByVariant(Variant variant, QueryOptions options);

//    QueryResult getStatsByVariantList(List<Variant> variant, QueryOptions options);

//    QueryResult getSimpleStatsByVariant(Variant variant, QueryOptions options);


//    QueryResult getEffectsByVariant(Variant variant, QueryOptions options);


//    QueryResult getAllBySampleList(List<Sample> samples, QueryOptions options);


    //    QueryResult getAllBySample(Sample sample, QueryOptions options);
//
//    @Deprecated
//    List<VariantInfo> getRecords(Map<String, String> options);
//
//    @Deprecated
//    List<VariantStats> getRecordsStats(Map<String, String> options);
//
//    @Deprecated
//    List<VariantEffect> getEffect(Map<String, String> options);
//
//    @Deprecated

//    VariantAnalysisInfo getAnalysisInfo(Map<String, String> options);

}
