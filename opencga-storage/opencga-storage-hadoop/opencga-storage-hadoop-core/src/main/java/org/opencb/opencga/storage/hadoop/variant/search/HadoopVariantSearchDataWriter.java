package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.client.Mutation;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.search.VariantSearchUpdateDocument;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSolrInputDocumentDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsFileCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;


/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSearchDataWriter extends VariantSolrInputDocumentDataWriter {

    private final HBaseDataWriter<Mutation> writer;
    private final PendingVariantsFileCleaner cleaner;
    private final List<Variant> variantsToClean = new ArrayList<>();
    private final List<Mutation> rowsToUpdate = new ArrayList<>();
    private final long updateTs;

    private long hbasePutTimeMs = 0;
    private long cleanTimeMs = 0;

    private final Logger logger = LoggerFactory.getLogger(HadoopVariantSearchDataWriter.class);


    public HadoopVariantSearchDataWriter(VariantSearchManager variantSearchManager, SearchIndexMetadata indexMetadata,
                                         VariantHadoopDBAdaptor dbAdaptor, PendingVariantsFileCleaner cleaner, long updateStartTimestamp) {
        super(variantSearchManager, indexMetadata);
        this.writer = new HBaseDataWriter<>(dbAdaptor.getHBaseManager(), dbAdaptor.getVariantTable());
        this.cleaner = cleaner;
        this.updateTs = updateStartTimestamp;
    }

    /**
     * If an error occurs while writing to Solr, we reset the pending mutations and variants.
     */
    private void onError() {
        // Reset the lists to avoid adding more mutations
        logger.error("Error while writing to Solr. Resetting pending mutations and variants to clean.");
        cleaner.abort();
        variantsToClean.clear();
        rowsToUpdate.clear();
    }

    @Override
    public boolean write(List<VariantSearchUpdateDocument> batch) {
        try {
            super.write(batch);
        } catch (Exception e) {
            onError();
            throw e;
        }

        if (!batch.isEmpty()) {
            List<Mutation> mutations = new ArrayList<>(batch.size());
            List<Variant> variants = new ArrayList<>(batch.size());

            for (VariantSearchUpdateDocument updateDocument : batch) {
                // Save a simplified version of the variant to clean later
                variants.add(new Variant(updateDocument.getVariant().toString()));
                mutations.add(HadoopVariantSearchIndexUtils.updateSyncStatus(updateDocument, updateTs));
            }
            variantsToClean.addAll(variants);
            rowsToUpdate.addAll(mutations);
        }
        return true;
    }

    @Override
    protected void flush() {
        try {
            super.flush();
        } catch (Exception e) {
            onError();
            throw e;
        }

        StopWatch stopWatch = StopWatch.createStarted();
        writer.write(rowsToUpdate);
        writer.flush();
        hbasePutTimeMs += stopWatch.getTime();
        rowsToUpdate.clear();

        stopWatch.reset();
        stopWatch.start();
        cleaner.write(variantsToClean);
        variantsToClean.clear();
        cleanTimeMs += stopWatch.getTime();
    }

    @Override
    public boolean open() {
        super.open();
        writer.open();
        cleaner.open();
        return true;
    }

    @Override
    public boolean pre() {
        super.pre();
        writer.pre();
        cleaner.pre();
        return true;
    }

    @Override
    public boolean post() {
        try {
            super.post();
            flush();
        } catch (Exception e) {
            onError();
            throw e;
        }
        StopWatch stopWatch = StopWatch.createStarted();
        writer.post();
        hbasePutTimeMs += stopWatch.getTime();
        stopWatch.reset();
        stopWatch.start();
        cleaner.post();
        cleanTimeMs += stopWatch.getTime();
        return true;
    }

    @Override
    public boolean close() {
        try {
            super.close();
        } catch (Exception e) {
            onError();
            throw e;
        }
        writer.close();
        cleaner.close();
        return true;
    }

    @Override
    public void printStats() {
        super.printStats();
        logger.info(" - HBase flags update time: {}", TimeUtils.durationToString(hbasePutTimeMs));
        logger.info(" - Pending Variants Files Clean: {}", TimeUtils.durationToString(cleanTimeMs));
    }
}
