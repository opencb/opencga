package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.io.DataWriter;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.opencb.opencga.storage.core.variant.search.solr.SolrInputDocumentDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.search.pending.index.file.SecondaryIndexPendingVariantsFileBasedManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;


/**
 * Created on 19/04/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class HadoopVariantSearchDataWriter extends SolrInputDocumentDataWriter {

    private final HBaseDataWriter<Mutation> writer;
//    private final PendingVariantsDBCleaner cleaner;
    private final DataWriter<Variant> cleaner;
    private final List<Variant> variantsToClean = new ArrayList<>();
    private final List<Mutation> rowsToUpdate = new ArrayList<>();
    private final byte[] family = GenomeHelper.COLUMN_FAMILY_BYTES;
    protected final Map<String, Integer> studiesMap;

    private long hbasePutTimeMs = 0;
    private long cleanTimeMs = 0;

    private final Logger logger = LoggerFactory.getLogger(HadoopVariantSearchDataWriter.class);


    public HadoopVariantSearchDataWriter(String collection, SolrClient solrClient, int insertBatchSize,
                                         VariantHadoopDBAdaptor dbAdaptor) {
        super(collection, solrClient, insertBatchSize);
        this.writer = new HBaseDataWriter<>(dbAdaptor.getHBaseManager(), dbAdaptor.getVariantTable());
        try {
            this.cleaner = new SecondaryIndexPendingVariantsFileBasedManager(dbAdaptor.getVariantTable(), dbAdaptor.getConfiguration())
                    .cleaner();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
            List<Variant> variants = new ArrayList<>(batch.size());
            Map<Collection<Object>, byte[]> studiesColumnMap = new HashMap<>();

            for (SolrInputDocument document : batch) {
                // For each variant, clear from pending and update INDEX_STUDIES

                Collection<Object> studies = document.getFieldValues("studies");
                byte[] bytes = studiesColumnMap.computeIfAbsent(studies, list -> {
                    Set<Integer> studyIds = list.stream().map(o -> studiesMap.get(o.toString())).collect(Collectors.toSet());
                    return PhoenixHelper.toBytes(studyIds, PIntegerArray.INSTANCE);
                });

                Variant variant = new Variant(document.getFieldValue("attr_id").toString());
                variants.add(variant);
                byte[] row = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
                variantRows.add(row);
                mutations.add(new Put(row)
                        .addColumn(family, VariantPhoenixSchema.VariantColumn.INDEX_STUDIES.bytes(), bytes));
            }
            variantsToClean.addAll(variants);
            rowsToUpdate.addAll(mutations);

//            writer.write(mutations);
////            cleaner.write(variantRows);
//            cleaner.write(variants);
        }
    }

    @Override
    protected void commit() throws Exception {
        super.commit();
        StopWatch stopWatch = StopWatch.createStarted();
        writer.write(rowsToUpdate);
        writer.flush();
        hbasePutTimeMs += stopWatch.getTime();
        rowsToUpdate.clear();
        stopWatch.reset();

        stopWatch.start();
        cleaner.write(variantsToClean);
//        cleaner.flush();
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
        super.post();
        StopWatch stopWatch = StopWatch.createStarted();
        writer.post();
        hbasePutTimeMs += stopWatch.getTime();
        stopWatch.reset();
        stopWatch.start();
        cleaner.post();
        cleanTimeMs += stopWatch.getTime();
        logger.info("HBase flags update time: {}", TimeUtils.durationToString(hbasePutTimeMs));
        logger.info("Pending Variants Files Clean: {}", TimeUtils.durationToString(cleanTimeMs));
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
