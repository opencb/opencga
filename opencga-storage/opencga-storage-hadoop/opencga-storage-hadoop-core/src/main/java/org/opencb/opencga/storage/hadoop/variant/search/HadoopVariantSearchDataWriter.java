package org.opencb.opencga.storage.hadoop.variant.search;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.phoenix.schema.types.PIntegerArray;
import org.apache.solr.common.SolrInputDocument;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.storage.core.metadata.models.project.SearchIndexMetadata;
import org.opencb.opencga.storage.core.variant.search.VariantSearchToVariantConverter;
import org.opencb.opencga.storage.core.variant.search.solr.SolrInputDocumentDataWriter;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSearchManager;
import org.opencb.opencga.storage.core.variant.search.solr.VariantSolrInputDocumentDataWriter;
import org.opencb.opencga.storage.hadoop.utils.HBaseDataWriter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantPhoenixSchema;
import org.opencb.opencga.storage.hadoop.variant.pending.PendingVariantsFileCleaner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


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
    private final byte[] family = GenomeHelper.COLUMN_FAMILY_BYTES;
    protected final Map<String, Integer> studiesMap;

    private long hbasePutTimeMs = 0;
    private long cleanTimeMs = 0;

    private final Logger logger = LoggerFactory.getLogger(HadoopVariantSearchDataWriter.class);


    public HadoopVariantSearchDataWriter(VariantSearchManager variantSearchManager, SearchIndexMetadata indexMetadata,
                                         VariantHadoopDBAdaptor dbAdaptor, PendingVariantsFileCleaner cleaner) {
        super(variantSearchManager, indexMetadata);
        this.writer = new HBaseDataWriter<>(dbAdaptor.getHBaseManager(), dbAdaptor.getVariantTable());
        this.cleaner = cleaner;
        this.studiesMap = new HashMap<>(dbAdaptor.getMetadataManager().getStudies());
        for (String study : new ArrayList<>(studiesMap.keySet())) {
            studiesMap.put(VariantSearchToVariantConverter.studyIdToSearchModel(study), studiesMap.get(study));
        }
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
    public boolean write(List<Pair<SolrInputDocument, SolrInputDocument>> batch) {
        try {
            super.write(batch);
        } catch (Exception e) {
            onError();
            throw e;
        }

        if (!batch.isEmpty()) {
            List<Mutation> mutations = new ArrayList<>(batch.size());
            List<byte[]> variantRows = new ArrayList<>(batch.size());
            List<Variant> variants = new ArrayList<>(batch.size());
            Map<Collection<Object>, byte[]> studiesColumnMap = new HashMap<>();

            for (Pair<SolrInputDocument, SolrInputDocument> pair : batch) {
                // For each variant, clear from pending and update INDEX_STUDIES
                SolrInputDocument document = pair.getLeft();
                if (document == null) {
                    document = pair.getRight();
                }

                String attrId;
                Collection<Object> studies;
                if (SolrInputDocumentDataWriter.isSetValue(document.getField("attr_id"))) {
                    attrId = SolrInputDocumentDataWriter.readSetValue(document.getField("attr_id")).toString();
                    studies = SolrInputDocumentDataWriter.readSetValue(document.getField("studies"));
                } else {
                    attrId = document.getFieldValue("attr_id").toString();
                    studies = document.getFieldValues("studies");
                }

                byte[] bytes = studiesColumnMap.computeIfAbsent(studies, list -> {
                    Set<Integer> studyIds = list.stream().map(o -> studiesMap.get(o.toString())).collect(Collectors.toSet());
                    return PhoenixHelper.toBytes(studyIds, PIntegerArray.INSTANCE);
                });

                Variant variant = new Variant(attrId);
                variants.add(variant);
                byte[] row = VariantPhoenixKeyFactory.generateVariantRowKey(variant);
                variantRows.add(row);
                mutations.add(new Put(row)
                        .addColumn(family, VariantPhoenixSchema.VariantColumn.INDEX_STUDIES.bytes(), bytes));
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
        logger.info("HBase flags update time: {}", TimeUtils.durationToString(hbasePutTimeMs));
        logger.info("Pending Variants Files Clean: {}", TimeUtils.durationToString(cleanTimeMs));
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
}
