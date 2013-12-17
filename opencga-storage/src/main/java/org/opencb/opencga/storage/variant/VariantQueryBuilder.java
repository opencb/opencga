package org.opencb.opencga.storage.variant;

import org.opencb.commons.bioformats.variant.json.VariantAnalysisInfo;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.bioformats.variant.utils.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.utils.stats.VariantStats;

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
    List<VariantInfo> getRecords(Map<String, String> options);

    List<VariantStats> getRecordsStats(Map<String, String> options);

    List<VariantEffect> getEffect(Map<String, String> options);

    VariantAnalysisInfo getAnalysisInfo(Map<String, String> options);
}
