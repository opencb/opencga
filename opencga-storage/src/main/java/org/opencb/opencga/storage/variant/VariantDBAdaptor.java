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
     * Given a genomic region and a study name, it retrieves a set of variants and, optionally, all the information
     * about their samples, effects and statistics. These optional arguments are specified in the "options" dictionary,
     * with the keys (values must be set to true): "samples", "effects" and "stats", respectively.
     *
     * @param region    The region where variants must be searched
     * @param studyId   The identifier of the study where variants are filed
     * @param options   Optional arguments
     * @return A QueryResult containing a set of variants and other optional information
     */
    QueryResult getAllVariantsByRegionAndStudy(Region region, String studyId, QueryOptions options);

    List<QueryResult> getAllVariantsByRegionListAndStudy(List<Region> regions, String studyName, QueryOptions options);

    
//    QueryResult getVariantsHistogramByRegion(Region region, String studyName, boolean histogramLogarithm, int histogramMax);

    
    QueryResult getAllVariantsByGene(String geneName, QueryOptions options);
    
    QueryResult getMostAffectedGenes(int numGenes, QueryOptions options);
    
    QueryResult getLeastAffectedGenes(int numGenes, QueryOptions options);

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
