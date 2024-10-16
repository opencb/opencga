package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.opencb.commons.datastore.core.ObjectMap;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDeleteHBaseColumnTask.*;

public class ArchiveDeleteHBaseColumnTaskTest {

    private ArchiveDeleteHBaseColumnTask task;

    @Test
    public void testConfigureTask() {
        ObjectMap options = new ObjectMap();
        task = new ArchiveDeleteHBaseColumnTask();
        configureTask(options, Arrays.asList(1, 2, 3));
        assertEquals(Arrays.asList(0), options.getAsIntegerList(FILE_BATCHES_WITH_FILES_TO_DELETE_FROM_ARCHIVE_INDEX));
        List<Pair<byte[], byte[]>> regionsToDelete = task.getRegionsToDelete(toConf(options));
        assertEquals(1, regionsToDelete.size());

        assertArrayEquals(Bytes.toBytes("00000_"), regionsToDelete.get(0).getFirst());
        assertArrayEquals(Bytes.toBytes("00001_"), regionsToDelete.get(0).getSecond());
    }

    @Test
    public void testConfigureTaskMultiRegions() {
        ObjectMap options = new ObjectMap();
        task = new ArchiveDeleteHBaseColumnTask();
        configureTask(options, Arrays.asList(5300, 6053, 9032));
        assertEquals(Arrays.asList(5, 6, 9), options.getAsIntegerList(FILE_BATCHES_WITH_FILES_TO_DELETE_FROM_ARCHIVE_INDEX));
        List<Pair<byte[], byte[]>> regionsToDelete = task.getRegionsToDelete(toConf(options));
        assertEquals(3, regionsToDelete.size());

        assertArrayEquals(Bytes.toBytes("00005_"), regionsToDelete.get(0).getFirst());
        assertArrayEquals(Bytes.toBytes("00006_"), regionsToDelete.get(0).getSecond());
        assertArrayEquals(Bytes.toBytes("00006_"), regionsToDelete.get(1).getFirst());
        assertArrayEquals(Bytes.toBytes("00007_"), regionsToDelete.get(1).getSecond());
        assertArrayEquals(Bytes.toBytes("00009_"), regionsToDelete.get(2).getFirst());
        assertArrayEquals(Bytes.toBytes("00010_"), regionsToDelete.get(2).getSecond());
    }

    private static Configuration toConf(ObjectMap options) {
        Configuration conf = new Configuration();
        for (String key : options.keySet()) {
            conf.set(key, options.getString(key));
        }
        return conf;
    }

}