package org.opencb.opencga.storage.hadoop.variant.index.stats;

import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.avro.VariantStats;
import org.opencb.biodata.tools.variant.converter.Converter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.AbstractPhoenixConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created on 07/07/16.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HBaseToVariantStatsConverter extends AbstractPhoenixConverter implements Converter<Result, VariantStats> {

    private final Logger logger = LoggerFactory.getLogger(HBaseToVariantStatsConverter.class);
    private final GenomeHelper genomeHelper;

    public HBaseToVariantStatsConverter(GenomeHelper genomeHelper) {
        this.genomeHelper = genomeHelper;
    }

    @Override
    public VariantStats convert(Result result) {
        return null;
    }


    @Override
    protected GenomeHelper getGenomeHelper() {
        return genomeHelper;
    }
}
