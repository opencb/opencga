package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.opencb.opencga.storage.core.variant.search.solr.SolrInputDocumentDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsDBCleaner;

import java.util.*;
import java.util.stream.Collectors;


/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSearchDataWriter extends SolrInputDocumentDataWriter {

    private final HBaseDataWriter<Mutation> writer;
    private final PendingVariantsDBCleaner cleaner;
    private final byte[] family = GenomeHelper.COLUMN_FAMILY_BYTES;
    protected final Map<String, Integer> studiesMap;

    public HadoopVariantSearchDataWriter(String collection, SolrClient solrClient, int insertBatchSize,
                                         VariantHadoopDBAdaptor dbAdaptor) {
        super(collection, solrClient, insertBatchSize);
        this.writer = new HBaseDataWriter<Mutation>(dbAdaptor.getHBaseManager(), dbAdaptor.getVariantTable()) {
            @Override
            protected BufferedMutatorParams buildBufferedMutatorParams() {
                // Set write buffer size to 10GB to ensure that will only be triggered manually on flush
                return super.buildBufferedMutatorParams().writeBufferSize(10L * 1024L * 1024L * 1024L);
            }
        };
        this.cleaner = new SecondaryIndexPendingVariantsManager(dbAdaptor).cleaner();
        this.studiesMap = new HashMap<>(dbAdaptor.getMetadataManager().getStudies());
        for (String study : new ArrayList<>(studiesMap.keySet())) {
            studiesMap.put(VariantSearchToVariantConverter.studyIdToSearchModel(study), studiesMap.get(study));
        }
    }

    @Override
    protected void add(List<SolrInputDocument> batch) throws Exception {
        super.add(batch);

        if (!batch.isEmpty()) {
            List<Mutation> mutations = new ArrayList<>(batch.size());
            List<byte[]> variantRows = new ArrayList<>(batch.size());
            Map<Collection<Object>, byte[]> studiesColumnMap = new HashMap<>();

            for (SolrInputDocument document : batch) {
                // For each variant, clear from pending and update INDEX_STUDIES

                Collection<Object> studies = document.getFieldValues("studies");
                byte[] bytes = studiesColumnMap.computeIfAbsent(studies, list -> {
                    Set<Integer> studyIds = list.stream().map(o -> studiesMap.get(o.toString())).collect(Collectors.toSet());
                    return PhoenixHelper.toBytes(studyIds, PIntegerArray.INSTANCE);
                });

                byte[] row = VariantPhoenixKeyFactory.generateVariantRowKey(new Variant(document.getFieldValue("id").toString()));
                variantRows.add(row);
                mutations.add(new Put(row)
                        .addColumn(family, VariantPhoenixSchema.VariantColumn.INDEX_STUDIES.bytes(), bytes));
            }

            writer.write(mutations);
            cleaner.write(variantRows);
        }
    }

    @Override
    protected void commit() throws Exception {
        super.commit();
        writer.flush();
        cleaner.flush();
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
        super.post();
        writer.post();
        cleaner.post();
        return true;
    }

    @Override
    public boolean close() {
        super.close();
        writer.close();
        cleaner.close();
        return true;
    }
}
