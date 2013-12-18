package org.opencb.opencga.storage.variant;

import org.opencb.commons.bioformats.feature.Region;
import org.opencb.commons.bioformats.variant.Variant;
import org.opencb.commons.bioformats.variant.json.VariantAnalysisInfo;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;
import org.opencb.commons.containers.QueryResult;
import org.opencb.commons.containers.map.QueryOptions;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: aaleman
 * Date: 10/30/13
 * Time: 12:14 PM
 * To change this template use File | Settings | File Templates.
 */
public interface VariantQueryBuilder {

    QueryResult getAllVariantsByRegion(Region region, QueryOptions options);

//    QueryResult getAllVariantsByRegionList(List<Region> region, QueryOptions options);

    QueryResult getVariantsHistogramByRegion(Region region, boolean histogramLogarithm, int histogramMax);



    QueryResult getStatsByVariant(Variant variant, QueryOptions options);

//    QueryResult getStatsByVariantList(List<Variant> variant, QueryOptions options);

    QueryResult getSimpleStatsByVariant(Variant variant, QueryOptions options);



//    QueryResult getAllBySample(Sample sample, QueryOptions options);
//
//    QueryResult getAllBySampleList(List<Sample> samples, QueryOptions options);


    @Deprecated
    List<VariantInfo> getRecords(Map<String, String> options);

    @Deprecated
    List<VariantStats> getRecordsStats(Map<String, String> options);

    @Deprecated
    List<VariantEffect> getEffect(Map<String, String> options);

    @Deprecated
    VariantAnalysisInfo getAnalysisInfo(Map<String, String> options);
}
