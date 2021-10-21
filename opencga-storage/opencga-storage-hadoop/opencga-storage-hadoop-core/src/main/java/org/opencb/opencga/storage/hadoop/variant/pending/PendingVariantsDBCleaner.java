package org.opencb.opencga.storage.hadoop.variant.pending;

import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.opencb.opencga.storage.hadoop.utils.AbstractHBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.*;

/**
 * Clear a PendingVariants table.
 *
 * Forces a major_compact of cleared regions.
 * Should run a major compact to clear the HFiles, so next executions won't have to iterate over
 * all the DELETE tombstones of this region.
 *
 * Created on 13/02/19.
 *
 * @see <a href="https://hbase.apache.org/2.2/book.html#_delete">HBase delete</a>
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PendingVariantsDBCleaner extends AbstractHBaseDataWriter<byte[], Delete> {

    private final PendingVariantsDescriptor descriptor;
    private final Deque<HRegionLocation> regions = new LinkedList<>();
    private RegionLocator regionLocator;
    private final Logger logger = LoggerFactory.getLogger(PendingVariantsDBCleaner.class);

    public PendingVariantsDBCleaner(HBaseManager hBaseManager, String tableName, PendingVariantsDescriptor descriptor) {
        super(hBaseManager, tableName);
        this.descriptor = descriptor;
        descriptor.checkValidPendingTableName(tableName);
    }

    @Override
    public boolean pre() {
        try {
            descriptor.createTableIfNeeded(tableName, hBaseManager);
            regionLocator = hBaseManager.getConnection().getRegionLocator(TableName.valueOf(tableName));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    protected List<Delete> convert(List<byte[]> batch) throws IOException {
        List<Delete> deletes = new ArrayList<>(batch.size());
        for (byte[] rowKey : batch) {
            Delete delete = new Delete(rowKey);
            deletes.add(delete);
        }

        for (int i = 0; i < batch.size(); i += 100) {
            HRegionLocation region = regionLocator.getRegionLocation(batch.get(0));
            if (!regions.contains(region)) {
                regions.add(region);
            }
        }
        while (regions.size() > 10) {
            // If the regions list contains more than 10 elements, start running major_compacts.
            compactRegions(Collections.singletonList(regions.pollFirst()));
        }

        return deletes;
    }

    @Override
    public boolean post() {
        super.post();
        compactRegions(new ArrayList<>(regions));
        return true;
    }

    /**
     * Major compact given regions.
     *
     * @param regions List of regions to compact
     */
    private void compactRegions(List<HRegionLocation> regions) {
        try (Admin admin = hBaseManager.getConnection().getAdmin()) {
            for (HRegionLocation region : regions) {
                logger.info("Major compact region " + region.toString());
                admin.majorCompactRegion(region.getRegionInfo().getRegionName());
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
