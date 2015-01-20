package org.opencb.opencga.storage.core.variant.adaptors;

import java.net.UnknownHostException;
import java.nio.file.Path;
import java.util.List;
import org.opencb.biodata.models.feature.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.annotation.VariantAnnotation;
import org.opencb.commons.io.DataWriter;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

/**
 * @author Ignacio Medina <igmecas@gmail.com>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface VariantDBAdaptor extends Iterable<Variant> {

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
    public QueryResult getAllVariants(QueryOptions options);


    public QueryResult getVariantById(String id, QueryOptions options);

    public List<QueryResult> getAllVariantsByIdList(List<String> idList, QueryOptions options);

    /**
     * Given a genomic region, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param region    The region where variants must be searched
     * @param options   Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    public QueryResult getAllVariantsByRegion(Region region, QueryOptions options);

    public List<QueryResult> getAllVariantsByRegionList(List<Region> regionList, QueryOptions options);

    
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

    
    QueryResult getVariantFrequencyByRegion(Region region, QueryOptions options);

    QueryResult groupBy(String field, QueryOptions options);


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


//    QueryResult getAllBySample(Sample sample, QueryOptions options);
//
//    QueryResult getAllBySampleList(List<Sample> samples, QueryOptions options);


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

    public VariantSourceDBAdaptor getVariantSourceDBAdaptor();

    @Override
    public VariantDBIterator iterator();

    public VariantDBIterator iterator(QueryOptions options);

    public QueryResult updateAnnotations(List<VariantAnnotation> variantAnnotations, QueryOptions queryOptions);

    public boolean close();

}
