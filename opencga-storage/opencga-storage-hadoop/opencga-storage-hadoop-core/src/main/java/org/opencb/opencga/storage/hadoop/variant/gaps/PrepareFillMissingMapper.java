package org.opencb.opencga.storage.hadoop.variant.gaps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PInteger;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.AbstractVariantsTableDriver;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveRowKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantsTableMapReduceHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created on 14/02/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class PrepareFillMissingMapper extends TableMapper<ImmutableBytesWritable, Put> {

    public static final byte[] EMPTY_VALUE = new byte[0];
    public static final ImmutableBytesWritable EMPTY_IMMUTABLE_BYTES = new ImmutableBytesWritable(EMPTY_VALUE);
    private ArchiveRowKeyFactory rowKeyFactory;
    private Set<Integer> allFileBatches;
    private Map<Integer, Collection<Integer>> fileBatchesMap = new HashMap<>();
    private byte[] family;
    private PhoenixHelper.Column fillMissingColumn;
    private List<Integer> indexedFiles;
    private long timestamp;
    private boolean fillAllFiles;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        GenomeHelper helper = new GenomeHelper(context.getConfiguration());
        family = helper.getColumnFamily();
        rowKeyFactory = new ArchiveRowKeyFactory(context.getConfiguration());
        indexedFiles = getIndexedFiles(context.getConfiguration());
        allFileBatches = indexedFiles.stream().map(rowKeyFactory::getFileBatch).collect(Collectors.toSet());
        fillMissingColumn = VariantPhoenixHelper.getFillMissingColumn(helper.getStudyId());
        timestamp = context.getConfiguration().getLong(AbstractVariantsTableDriver.TIMESTAMP, 0);
        if (timestamp <= 0) {
            throw new IllegalArgumentException(AbstractVariantsTableDriver.TIMESTAMP + " not defined!");
        }
        fillAllFiles = FillGapsFromArchiveMapper.isOverwrite(context.getConfiguration());
    }
    private final Logger logger = LoggerFactory.getLogger(PrepareFillMissingMapper.class);

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        Variant variant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(value.getRow());
        byte[] column = FillMissingFromArchiveTask.getArchiveVariantColumn(variant);
        long sliceId = rowKeyFactory.getSliceId(variant.getStart());
        String chromosome = variant.getChromosome();
        byte[] lastFileBytes = value.getValue(family, fillMissingColumn.bytes());
        Collection<Integer> fileBatches;
        boolean newVariant;
        if (lastFileBytes == null || lastFileBytes.length == 0 || fillAllFiles) {
            fileBatches = this.allFileBatches;
            newVariant = true;
//            logger.info("FILL ALL for variant " + variant + " -> " + fileBatches);
        } else {
            Integer lastFile = (Integer) PInteger.INSTANCE.toObject(lastFileBytes);
            fileBatches = fileBatchesMap.computeIfAbsent(lastFile, this::buildFileBatches);
            newVariant = false;
//            logger.info("FILL some for variant " + variant + " -> " + fileBatches);
        }
        for (Integer fileBatch : fileBatches) {
            Put put = new Put(Bytes.toBytes(rowKeyFactory.generateBlockIdFromSliceAndBatch(fileBatch, chromosome, sliceId)), timestamp);
            put.addColumn(family, column, lastFileBytes);
            context.write(EMPTY_IMMUTABLE_BYTES, put);
            if (!newVariant) {
                context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "UPDATE_VARIANT_BATCH_" + fileBatch).increment(1);
            }
        }
        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "PUT").increment(fileBatches.size());
        if (newVariant) {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "NEW_VARIANT").increment(1);
        } else {
            context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "UPDATE_VARIANT").increment(1);
        }
        context.getCounter(VariantsTableMapReduceHelper.COUNTER_GROUP_NAME, "VARIANTS").increment(1);
    }

    private Collection<Integer> buildFileBatches(Integer lastFile) {
        Set<Integer> batches = new HashSet<>();
        // Add only file batches from files beyond the last file
        int indexOf = indexedFiles.indexOf(lastFile);
        if (indexOf == -1) {
            logger.warn("Last file updated '" + lastFile + "' is not indexed!");
        }
        for (int i = indexOf + 1; i < indexedFiles.size(); i++) {
            Integer indexedFile = indexedFiles.get(i);
            batches.add(rowKeyFactory.getFileBatch(indexedFile));
        }
        return batches;
    }

    public static List<Integer> getIndexedFiles(Configuration configuration) {
        return Arrays.stream(configuration.getStrings("indexedFiles")).map(Integer::parseInt).collect(Collectors.toList());
    }

    public static void setIndexedFiles(Configuration configuration, Collection<?> indexedFiles) {
        configuration.set("indexedFiles", indexedFiles.stream().map(Object::toString).collect(Collectors.joining(",")));
    }
}
