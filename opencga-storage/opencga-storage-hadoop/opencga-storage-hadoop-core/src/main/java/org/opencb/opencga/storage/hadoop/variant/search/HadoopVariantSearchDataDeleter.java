package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchIdGenerator;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDBCleaner;

import java.io.IOException;
import java.util.*;


/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSearchDataDeleter implements DataWriter<Variant> {

    private final String collection;
    private final VariantSearchIdGenerator idGenerator;
    private final SolrClient solrClient;
    private final PendingVariantsDBCleaner cleaner;

    public HadoopVariantSearchDataDeleter(String collection, VariantSearchIdGenerator idGenerator,
                                          SolrClient solrClient, PendingVariantsDBCleaner cleaner) {
        this.collection = collection;
        this.idGenerator = idGenerator;
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
            variantIds.add(idGenerator.getId(variant));
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
