package org.opencb.opencga.storage.hadoop.variant.score;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantScore;
import org.opencb.biodata.tools.commons.Converter;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.converters.AbstractPhoenixConverter;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;

import java.util.Arrays;

public class VariantScoreToHBaseConverter extends AbstractPhoenixConverter implements Converter<Pair<Variant, VariantScore>, Put> {

    private final PhoenixHelper.Column column;

    public VariantScoreToHBaseConverter(byte[] columnFamily, int studyId, int scoreId) {
        super(columnFamily);
        column = VariantPhoenixSchema.getVariantScoreColumn(studyId, scoreId);
    }

    @Override
    public Put convert(Pair<Variant, VariantScore> pair) {
        Put put = new Put(VariantPhoenixKeyFactory.generateVariantRowKey(pair.getKey()));
        add(put, column, Arrays.asList(pair.getValue().getScore(), pair.getValue().getPValue()));
        HadoopVariantSearchIndexUtils.addNotSyncStatus(put, columnFamily);
        return put;
    }
}
