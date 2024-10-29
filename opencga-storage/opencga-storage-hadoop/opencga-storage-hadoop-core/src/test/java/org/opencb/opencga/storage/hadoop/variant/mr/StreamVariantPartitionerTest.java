package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import static org.junit.Assert.assertEquals;

@Category(ShortTests.class)
public class StreamVariantPartitionerTest {

    public static final int NUM_PARTITIONS = 10;
    private StreamVariantPartitioner partitioner;

    @Before
    public void setUp() {
        partitioner = new StreamVariantPartitioner();
        partitioner.setup(NUM_PARTITIONS);
    }

    @Test
    public void partitionerTest() {
        assertEquals(0, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:00|0000000001|")), null, NUM_PARTITIONS));
        assertEquals(0, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:01|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(0, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:02|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(1, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:03|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(2, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:04|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(2, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:05|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(3, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:06|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(3, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:07|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(4, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:08|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(4, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:09|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(5, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:10|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(5, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:11|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(6, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:12|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(6, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:13|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(7, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:14|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(7, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:15|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(7, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:16|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:17|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:17_random_contig|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:18|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:19|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:20|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:21|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:22|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:X|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:Y|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:MT|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:Z|0000000000|")), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new ImmutableBytesWritable(Bytes.toBytes("o:Z_random_contig|0000000000|")), null, NUM_PARTITIONS));
    }

}