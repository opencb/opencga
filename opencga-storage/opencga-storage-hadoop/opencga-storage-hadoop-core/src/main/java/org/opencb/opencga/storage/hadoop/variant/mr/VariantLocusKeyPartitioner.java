package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.opencb.opencga.storage.core.utils.GenomeSplitFactory;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

public class VariantLocusKeyPartitioner<V> extends Partitioner<VariantLocusKey, V> implements Configurable {

    private final TreeMap<VariantLocusKey, Integer> regionSplitsMap = new TreeMap<>();
    private Configuration conf;

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        try {
            Job job = Job.getInstance(conf);
            int numReduceTasks = job.getNumReduceTasks();
            setup(numReduceTasks);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public TreeMap<VariantLocusKey, Integer> setup(int numPartitions) {
        List<VariantLocusKey> splits = GenomeSplitFactory.generateBootPreSplitsHuman(
                numPartitions, VariantLocusKey::new, VariantLocusKey::compareTo, false);
        regionSplitsMap.put(new VariantLocusKey("0", 0), 0);
        for (int i = 0; i < splits.size(); i++) {
            regionSplitsMap.put(splits.get(i), regionSplitsMap.size());
        }
        return regionSplitsMap;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public int getPartition(VariantLocusKey variantLocusKey, V v, int numPartitions) {
        return regionSplitsMap.floorEntry(variantLocusKey).getValue();
    }

}
