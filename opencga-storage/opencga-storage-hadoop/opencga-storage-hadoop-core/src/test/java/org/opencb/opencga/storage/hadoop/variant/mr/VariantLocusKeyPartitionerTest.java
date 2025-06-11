package org.opencb.opencga.storage.hadoop.variant.mr;

import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.opencb.opencga.core.testclassification.duration.ShortTests;

import static org.junit.Assert.assertEquals;

@Category(ShortTests.class)
public class VariantLocusKeyPartitionerTest {

    public static final int NUM_PARTITIONS = 10;
    private VariantLocusKeyPartitioner<?> partitioner;

    @Before
    public void setUp() {
        partitioner = new VariantLocusKeyPartitioner<>();
        partitioner.setup(NUM_PARTITIONS);
    }

    @Test
    public void partitionerTest() {
        assertEquals(0, partitioner.getPartition(new VariantLocusKey("0",1), null, NUM_PARTITIONS));
        assertEquals(0, partitioner.getPartition(new VariantLocusKey("1",0), null, NUM_PARTITIONS));
        assertEquals(0, partitioner.getPartition(new VariantLocusKey("2",0), null, NUM_PARTITIONS));
        assertEquals(1, partitioner.getPartition(new VariantLocusKey("3",0), null, NUM_PARTITIONS));
        assertEquals(2, partitioner.getPartition(new VariantLocusKey("4",0), null, NUM_PARTITIONS));
        assertEquals(2, partitioner.getPartition(new VariantLocusKey("5",0), null, NUM_PARTITIONS));
        assertEquals(3, partitioner.getPartition(new VariantLocusKey("6",0), null, NUM_PARTITIONS));
        assertEquals(3, partitioner.getPartition(new VariantLocusKey("7",0), null, NUM_PARTITIONS));
        assertEquals(4, partitioner.getPartition(new VariantLocusKey("8",0), null, NUM_PARTITIONS));
        assertEquals(4, partitioner.getPartition(new VariantLocusKey("9",0), null, NUM_PARTITIONS));
        assertEquals(5, partitioner.getPartition(new VariantLocusKey("10",0), null, NUM_PARTITIONS));
        assertEquals(5, partitioner.getPartition(new VariantLocusKey("11",0), null, NUM_PARTITIONS));
        assertEquals(6, partitioner.getPartition(new VariantLocusKey("12",0), null, NUM_PARTITIONS));
        assertEquals(6, partitioner.getPartition(new VariantLocusKey("13",0), null, NUM_PARTITIONS));
        assertEquals(7, partitioner.getPartition(new VariantLocusKey("14",0), null, NUM_PARTITIONS));
        assertEquals(7, partitioner.getPartition(new VariantLocusKey("15",0), null, NUM_PARTITIONS));
        assertEquals(7, partitioner.getPartition(new VariantLocusKey("16",0), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new VariantLocusKey("17",0), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new VariantLocusKey("17_random_contig",0), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new VariantLocusKey("18",0), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new VariantLocusKey("18",70880000), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new VariantLocusKey("19",0), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new VariantLocusKey("20",0), null, NUM_PARTITIONS));
        assertEquals(8, partitioner.getPartition(new VariantLocusKey("21",0), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new VariantLocusKey("22",0), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new VariantLocusKey("X",0), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new VariantLocusKey("Y",0), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new VariantLocusKey("MT",0), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new VariantLocusKey("Z",0), null, NUM_PARTITIONS));
        assertEquals(9, partitioner.getPartition(new VariantLocusKey("Z_random_contig",0), null, NUM_PARTITIONS));
    }

}