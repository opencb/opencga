package org.opencb.opencga.storage.hadoop.variant.archive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.hadoop.utils.DeleteHBaseColumnDriver;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ArchiveDeleteHBaseColumnTask extends DeleteHBaseColumnDriver.DeleteHBaseColumnTask {

    public static final String FILE_BATCHES_WITH_FILES_TO_DELETE_FROM_ARCHIVE_INDEX = "fileBatchesWithFilesToDeleteFromArchiveIndex";

    public static void configureTask(ObjectMap options, List<Integer> fileIds) {
        ArchiveRowKeyFactory keyFactory = new ArchiveRowKeyFactory(options);
        Set<Integer> fileBatchesSet = new HashSet<>();
        for (Integer fileId : fileIds) {
            fileBatchesSet.add(keyFactory.getFileBatch(fileId));
        }
        List<Integer> fileBatches = new ArrayList<>(fileBatchesSet);
        fileBatches.sort(Integer::compareTo);
        options.put(DeleteHBaseColumnDriver.DELETE_HBASE_COLUMN_TASK_CLASS, ArchiveDeleteHBaseColumnTask.class.getName());
        options.put(ArchiveDeleteHBaseColumnTask.FILE_BATCHES_WITH_FILES_TO_DELETE_FROM_ARCHIVE_INDEX, fileBatches);
    }

    @Override
    public List<Pair<byte[], byte[]>> getRegionsToDelete(Configuration configuration) {
        int[] fileBatches = configuration.getInts(FILE_BATCHES_WITH_FILES_TO_DELETE_FROM_ARCHIVE_INDEX);
        List<Pair<byte[], byte[]>> regions = new ArrayList<>();
        ArchiveRowKeyFactory archiveRowKeyFactory = new ArchiveRowKeyFactory(configuration);
        for (int fileBatch : fileBatches) {
            regions.add(new Pair<>(
                    Bytes.toBytes(archiveRowKeyFactory.generateBlockIdFromBatch(fileBatch)),
                    Bytes.toBytes(archiveRowKeyFactory.generateBlockIdFromBatch(fileBatch + 1))));
        }
        return regions;
    }
}
