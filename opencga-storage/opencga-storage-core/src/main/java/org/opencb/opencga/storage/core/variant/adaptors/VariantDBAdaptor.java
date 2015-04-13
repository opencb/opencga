package org.opencb.opencga.storage.core.variant.adaptors;

import java.util.*;

import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.ObjectMap;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;

/**
 * @author Ignacio Medina <igmecas@gmail.com>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface VariantDBAdaptor extends Iterable<Variant> {

    public static final String ID = "id";
    public static final String REGION = "region";
    public static final String CHROMOSOME = "chromosome";
    public static final String GENE = "gene";
    public static final String TYPE = "type";
    public static final String REFERENCE = "reference";
    public static final String ALTERNATE = "alternate";
    public static final String EFFECT = "effect";
    public static final String STUDIES = "studies";
    public static final String FILES = "files";
    public static final String FILE_ID = "fileId";
    public static final String MAF = "maf";
    public static final String MGF = "mgf";
    public static final String MISSING_ALLELES = "missingAlleles";
    public static final String MISSING_GENOTYPES = "missingGenotypes";
    public static final String ANNOTATION_EXISTS = "annotationExists";
    public static final String GENOTYPE = "genotype";
    public static final String ANNOT_CONSEQUENCE_TYPE = "annot-ct";
    public static final String ANNOT_XREF = "annot-xref";
    public static final String ANNOT_BIOTYPE = "annot-biotype";
    public static final String POLYPHEN = "polyphen";
    public static final String SIFT = "sift";
    public static final String PROTEIN_SUBSTITUTION = "protein_substitution";
    public static final String CONSERVED_REGION = "conserved_region";
    public static final String MERGE = "merge";

    static public class QueryParams {
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
    public void setDataWriter(DataWriter dataWriter);

    /**
     * Given a genomic region, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param options   Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    public QueryResult<Variant> getAllVariants(QueryOptions options);


    public QueryResult<Variant> getVariantById(String id, QueryOptions options);

    public List<QueryResult<Variant>> getAllVariantsByIdList(List<String> idList, QueryOptions options);

    /**
     * Given a genomic region, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param region    The region where variants must be searched
     * @param options   Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    public QueryResult<Variant> getAllVariantsByRegion(Region region, QueryOptions options);

    /**
     * @deprecated Use "getAllVariants" with VariantDBAdaptor.REGION filter instead.
     */
    @Deprecated
    public List<QueryResult<Variant>> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options);


    QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options);

    QueryResult groupBy(String field, QueryOptions options);


    public VariantSourceDBAdaptor getVariantSourceDBAdaptor();

    @Override
    public VariantDBIterator iterator();

    public VariantDBIterator iterator(QueryOptions options);

    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions);

    public QueryResult updateStats(List<VariantStatsWrapper> variantStatsWrappers, StudyConfiguration studyConfiguration, QueryOptions queryOptions);

    public boolean close();


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
