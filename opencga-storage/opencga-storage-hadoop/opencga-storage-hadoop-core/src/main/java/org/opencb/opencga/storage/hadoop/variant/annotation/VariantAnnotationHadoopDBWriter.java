package org.opencb.opencga.storage.hadoop.variant.annotation;

import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.client.Put;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchemaManager;
import org.opencb.opencga.storage.hadoop.variant.annotation.pending.AnnotationPendingVariantsManager;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDBCleaner;
import org.opencb.opencga.storage.hadoop.variant.search.HadoopVariantSearchIndexUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantAnnotationHadoopDBWriter extends HBaseDataWriter<Put> {

    private static final int PENDING_VARIANTS_BUFFER_SIZE = 100_000;
    private final VariantPhoenixSchemaManager schemaManager;

    private final byte[] columnFamily;
    private final List<byte[]> loadedVariants = new ArrayList<>(PENDING_VARIANTS_BUFFER_SIZE);
    private final PendingVariantsDBCleaner pendingVariantsCleaner;


    public VariantAnnotationHadoopDBWriter(VariantHadoopDBAdaptor dbAdaptor) {
        super(dbAdaptor.getHBaseManager(), dbAdaptor.getTableNameGenerator().getVariantTableName());
        this.columnFamily = GenomeHelper.COLUMN_FAMILY_BYTES;

        pendingVariantsCleaner = new AnnotationPendingVariantsManager(hBaseManager, dbAdaptor.getTableNameGenerator()).cleaner();
        schemaManager = new VariantPhoenixSchemaManager(dbAdaptor);
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
            schemaManager.registerAnnotationColumns();
        } catch (StorageEngineException e) {
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
            HadoopVariantSearchIndexUtils.addNotSyncStatus(put);
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
