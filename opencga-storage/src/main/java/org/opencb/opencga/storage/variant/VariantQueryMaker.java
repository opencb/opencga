package org.opencb.opencga.storage.variant;

import org.opencb.commons.bioformats.variant.json.VariantAnalysisInfo;
import org.opencb.commons.bioformats.variant.json.VariantInfo;
import org.opencb.commons.bioformats.variant.vcf4.effect.VariantEffect;
import org.opencb.commons.bioformats.variant.vcf4.stats.VcfVariantStat;

import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: aaleman
 * Date: 10/30/13
 * Time: 12:14 PM
 * To change this template use File | Settings | File Templates.
 */
public interface VariantQueryMaker {
    List<VariantInfo> getRecords(Map<String, String> options);

    List<VcfVariantStat> getRecordsStats(Map<String, String> options);

    List<VariantEffect> getEffect(Map<String, String> options);

    VariantAnalysisInfo getAnalysisInfo(Map<String, String> options);
}
