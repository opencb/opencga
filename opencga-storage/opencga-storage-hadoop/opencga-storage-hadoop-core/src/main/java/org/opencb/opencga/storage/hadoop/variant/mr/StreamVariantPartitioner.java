package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

public class StreamVariantPartitioner extends Partitioner<ImmutableBytesWritable, Text> implements Configurable {

    private TreeMap<String, Integer> regionSplitsMap = new TreeMap<>();
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

    public TreeMap<String, Integer> setup(int numPartitions) {
        List<String> splits = GenomeHelper.generateBootPreSplitsHuman(
                numPartitions, StreamVariantMapper::buildOutputKeyPrefix, String::compareTo, false);
        regionSplitsMap.put(StreamVariantMapper.buildOutputKeyPrefix("0", 0), 0);
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
    public int getPartition(ImmutableBytesWritable key, Text text, int numPartitions) {
        int start = key.getOffset() + StreamVariantReducer.STDOUT_KEY_BYTES.length;
        byte[] bytes = key.get();
        // Find last '|'
        int idx = 0;
        for (int i = key.getLength() + key.getOffset() - 1; i >= 0; i--) {
            if (bytes[i] == '|') {
                idx = i;
                break;
            }
        }
        String chrPos = Bytes.toString(bytes, start, idx - start);
        return regionSplitsMap.floorEntry(chrPos).getValue();
    }

}
