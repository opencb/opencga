package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDBCleaner;
import org.opencb.opencga.storage.hadoop.variant.prune.SecondaryIndexPrunePendingVariantsManager;

import java.io.IOException;
import java.util.*;


/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSearchDataDeleter implements DataWriter<Variant> {

    private final String collection;
    private final SolrClient solrClient;
    private final PendingVariantsDBCleaner cleaner;

    public HadoopVariantSearchDataDeleter(String collection, SolrClient solrClient, VariantHadoopDBAdaptor dbAdaptor) {
        this(collection, solrClient, new SecondaryIndexPrunePendingVariantsManager(dbAdaptor).cleaner());
    }

    public HadoopVariantSearchDataDeleter(String collection, SolrClient solrClient, PendingVariantsDBCleaner cleaner) {
        this.collection = collection;
        this.solrClient = solrClient;
        this.cleaner = cleaner;
    }

    @Override
    public boolean write(List<Variant> batch) {
        if (batch.isEmpty()) {
            return true;
        }

        List<byte[]> variantRows = new ArrayList<>(batch.size());
        List<String> variantIds = new ArrayList<>(batch.size());

        for (Variant variant : batch) {
            byte[] row = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
            variantRows.add(row);
            variantIds.add(variant.toString());
        }

        try {
            solrClient.deleteById(collection, variantIds);
            solrClient.commit(collection);
            cleaner.write(variantRows);
            cleaner.flush();
        } catch (SolrServerException | IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public boolean open() {
        cleaner.open();
        return true;
    }

    @Override
    public boolean pre() {
        cleaner.pre();
        return true;
    }

    @Override
    public boolean post() {
        cleaner.post();
        return true;
    }

    @Override
    public boolean close() {
        cleaner.close();
        return true;
    }
}
