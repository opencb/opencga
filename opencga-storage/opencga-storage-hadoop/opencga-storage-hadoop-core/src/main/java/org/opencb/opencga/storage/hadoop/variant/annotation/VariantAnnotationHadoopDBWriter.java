package org.opencb.opencga.storage.hadoop.variant.annotation;

import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.annotation.pending.AnnotationPendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDBCleaner;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationHadoopDBWriter extends HBaseDataWriter<Put> {

    private static final int PENDING_VARIANTS_BUFFER_SIZE = 100_000;

    private byte[] columnFamily;
    private List<byte[]> loadedVariants = new ArrayList<>(PENDING_VARIANTS_BUFFER_SIZE);
    private final PendingVariantsDBCleaner pendingVariantsCleaner;

    public VariantAnnotationHadoopDBWriter(HBaseManager hBaseManager, HBaseVariantTableNameGenerator nameGenerator, byte[] columnFamily) {
        super(hBaseManager, nameGenerator.getVariantTableName());
        this.columnFamily = columnFamily;

        pendingVariantsCleaner = new AnnotationPendingVariantsManager(hBaseManager, nameGenerator).cleaner();
    }

    @Override
    public boolean open() {
        super.open();
        return pendingVariantsCleaner.open();
    }

    @Override
    public boolean pre() {
        super.pre();
        try {
            VariantPhoenixHelper variantPhoenixHelper = new VariantPhoenixHelper(columnFamily, hBaseManager.getConf());
            Connection connection = variantPhoenixHelper.newJdbcConnection();
            String variantTable = super.tableName;

            variantPhoenixHelper.getPhoenixHelper().addMissingColumns(connection, variantTable,
                    VariantPhoenixHelper.getHumanPopulationFrequenciesColumns(), VariantPhoenixHelper.DEFAULT_TABLE_TYPE);

            variantPhoenixHelper.updateAnnotationColumns(connection, variantTable);
        } catch (SQLException | ClassNotFoundException e) {
            throw Throwables.propagate(e);
        }
        pendingVariantsCleaner.pre();
        return true;
    }

    @Override
    public boolean post() {
        super.post();
        cleanPendingVariants();
        pendingVariantsCleaner.post();
        return true;
    }

    @Override
    public boolean close() {
        super.close();
        return pendingVariantsCleaner.close();
    }

    @Override
    protected List<Put> convert(List<Put> puts) {
        if (loadedVariants.size() + puts.size() >= PENDING_VARIANTS_BUFFER_SIZE) {
            cleanPendingVariants();
        }
        for (Put put : puts) {
            HadoopVariantSearchIndexUtils.addNotSyncStatus(put, columnFamily);
            loadedVariants.add(put.getRow());
        }

        return puts;
    }

    private void cleanPendingVariants() {
        flush(); // Ensure own BufferedMutator is flushed
        pendingVariantsCleaner.write(loadedVariants);
        loadedVariants.clear();
    }
}
