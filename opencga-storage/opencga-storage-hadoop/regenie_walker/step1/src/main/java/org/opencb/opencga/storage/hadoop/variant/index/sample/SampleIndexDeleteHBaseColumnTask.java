package org.opencb.opencga.storage.hadoop.variant.index.sample;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Pair;
import org.opencb.opencga.storage.hadoop.utils.DeleteHBaseColumnDriver;

import java.util.ArrayList;
import java.util.List;

public class SampleIndexDeleteHBaseColumnTask extends DeleteHBaseColumnDriver.DeleteHBaseColumnTask {

    public static final String SAMPLE_IDS_TO_DELETE_FROM_SAMPLE_INDEX = "sampleIdsToDeleteFromSampleIndex";

    @Override
    public List<Pair<byte[], byte[]>> getRegionsToDelete(Configuration configuration) {
        int[] sampleIds = configuration.getInts(SAMPLE_IDS_TO_DELETE_FROM_SAMPLE_INDEX);
        List<Pair<byte[], byte[]>> regions = new ArrayList<>();
        for (int sampleId : sampleIds) {
            regions.add(new org.apache.hadoop.hbase.util.Pair<>(
                    SampleIndexSchema.toRowKey(sampleId),
                    SampleIndexSchema.toRowKey(sampleId + 1)));
        }
        return regions;
    }
}
