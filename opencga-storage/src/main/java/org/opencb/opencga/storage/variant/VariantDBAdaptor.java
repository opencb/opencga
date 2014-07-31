package org.opencb.opencga.storage.variant;

import java.util.List;
import org.opencb.biodata.models.feature.Region;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;

/**
 * @author Alejandro Aleman Ramos <aaleman@cipf.es>
 * @author Cristina Yenyxe Gonzalez Garcia <cgonzalez@cipf.es>
 */
public interface VariantDBAdaptor {

    /**
     * Given a genomic region, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param region    The region where variants must be searched
     * @param options   Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    QueryResult getAllVariantsByRegion(Region region, QueryOptions options);

    List<QueryResult> getAllVariantsByRegionList(List<Region> regions, QueryOptions options);

    
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
    QueryResult getAllVariantsByRegionAndStudies(Region region, List<String> studyIds, QueryOptions options);

    
    QueryResult getVariantsHistogramByRegion(Region region, QueryOptions options);

    
    QueryResult getAllVariantsByGene(String geneName, QueryOptions options);
    
    QueryResult getMostAffectedGenes(int numGenes, QueryOptions options);
    
    QueryResult getLeastAffectedGenes(int numGenes, QueryOptions options);

    
    QueryResult getVariantById(String id, QueryOptions options);
            
    List<QueryResult> getVariantsByIdList(List<String> ids, QueryOptions options);
    
    
    QueryResult getTopConsequenceTypes(int numConsequenceTypes, QueryOptions options);
    
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

    public boolean close();

}
