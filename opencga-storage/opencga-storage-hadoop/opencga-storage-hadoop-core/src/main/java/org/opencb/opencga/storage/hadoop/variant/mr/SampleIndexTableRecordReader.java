package org.opencb.opencga.storage.hadoop.variant.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableRecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.Query;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixKeyFactory;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.metadata.HBaseVariantStorageMetadataDBAdaptorFactory;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

import static org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator.getDBNameFromVariantsTableName;

/**
 * Iterate over an HBase table with multiple GET operations to specific RowKeys given from {@link SampleIndexDBAdaptor}.
 * return (ImmutableBytesWritable, Result) pairs.
 *
 * Implementation note: {@link TableRecordReader} is not an interface. Has to overwrite all methods to ensure
 * that {@link org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl} is not used.
 */
public class SampleIndexTableRecordReader extends TableRecordReader {

    private final SampleIndexDBAdaptor sampleIndexDBAdaptor;
    private final StudyConfiguration studyConfiguration;
    private final Map<String, List<String>> samples;
    private final VariantQueryUtils.QueryOperation operation;
    private final HBaseManager hBaseManager;
    private final StudyConfigurationManager scm;

    private Table table;
    private Scan scan;
    private List<Region> regions;
    private VariantDBIterator iterator;
    private List<Result> results;
    private ListIterator<Result> resultsIterator;

    private ImmutableBytesWritable currentKey = new ImmutableBytesWritable();
    private Result currentValue;

    private Logger logger = LoggerFactory.getLogger(SampleIndexTableRecordReader.class);

    public SampleIndexTableRecordReader(Configuration conf) {
        GenomeHelper helper = new GenomeHelper(conf);
        hBaseManager = new HBaseManager(conf);
        String variantsTableName = VariantTableHelper.getVariantsTable(conf);
        HBaseVariantTableNameGenerator tableNameGenerator =
                new HBaseVariantTableNameGenerator(getDBNameFromVariantsTableName(variantsTableName), conf);
        scm = new StudyConfigurationManager(new HBaseVariantStorageMetadataDBAdaptorFactory(
                hBaseManager,
                tableNameGenerator.getMetaTableName(),
                conf));

        sampleIndexDBAdaptor = new SampleIndexDBAdaptor(helper, hBaseManager, tableNameGenerator, scm);

        Query query = VariantMapReduceUtil.getQueryFromConfig(conf);
        SampleIndexQuery sampleIndexQuery = SampleIndexQuery.extractSampleIndexQuery(query, scm);
        studyConfiguration = scm.getStudyConfiguration(sampleIndexQuery.getStudy(), null).first();
        operation = sampleIndexQuery.getQueryOperation();
        samples = sampleIndexQuery.getSamplesMap();

    }

    /**
     * Restart from survivable exceptions by creating a new scanner.
     *
     * This method is never used!
     *
     * @param firstRow  The first row to start at.
     * @throws IOException When restarting fails.
     */
    @Override
    public void restart(byte[] firstRow) throws IOException {
        // This method should not be called
        throw new UnsupportedOperationException();
    }

    /**
     * @param table the {@link Table} to scan.
     */
    @Override
    public void setTable(Table table) {
        this.table = table;
    }

    /**
     * Sets the scan defining the actual details like columns etc.
     *
     * @param scan  The scan to set.
     */
    @Override
    public void setScan(Scan scan) {
        this.scan = scan;
    }

    /**
     * Closes the split.
     *
     * @see org.apache.hadoop.mapreduce.RecordReader#close()
     */
    @Override
    public void close() {
        try {
            hBaseManager.close();
        } catch (IOException e) {
            logger.error("Error closing hBaseManager", e);
        }
        try {
            scm.close();
        } catch (IOException e) {
            logger.error("Error closing StudyConfigurationManager", e);
        }
        try {
            if (iterator != null) {
                iterator.close();
            }
        } catch (Exception e) {
            logger.error("Error closing SampleIndexIterator", e);
        }
    }

    /**
     * Returns the current key.
     *
     * @return The current key.
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentKey()
     */
    @Override
    public ImmutableBytesWritable getCurrentKey() {
        return currentKey;
    }

    /**
     * Returns the current value.
     *
     * @return The current value.
     * @throws IOException When the value is faulty.
     * @throws InterruptedException When the job is aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#getCurrentValue()
     */
    @Override
    public Result getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    /**
     * Initializes the reader.
     *
     * @param inputsplit  The split to work with.
     * @param context  The current task context.
     * @throws IOException When setting up the reader fails.
     * @throws InterruptedException When the job is aborted.
     */
    @Override
    public void initialize(InputSplit inputsplit, TaskAttemptContext context) throws IOException, InterruptedException {

        List<String> allChromosomes = new ArrayList<>(studyConfiguration.getVariantHeaderLines("contig").keySet());
        if (allChromosomes.isEmpty()) {
            // Contigs not found!
            allChromosomes = Arrays.asList("1", "10", "11", "12", "13", "14", "15", "16", "17", "18", "19",
                    "2", "20", "21", "22", "3", "4", "5", "6", "7", "8", "9", "M", "MT", "X", "Y");
        }
        allChromosomes.replaceAll(Region::normalizeChromosome);
        allChromosomes.sort(String::compareTo);

        byte[] firstRow = scan.getStartRow();
        byte[] lastRow = scan.getStopRow();

        String startChr;
        Integer start;
        if (firstRow == null || firstRow.length == 0) {
            startChr = allChromosomes.get(0);
            start = 0;
        } else {
            Variant startVariant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(firstRow);
            startChr = startVariant.getChromosome();
            start = startVariant.getStart();
        }
        String stopChr;
        Integer end;
        if (lastRow == null || lastRow.length == 0) {
            stopChr = allChromosomes.get(allChromosomes.size() - 1);
            end = Integer.MAX_VALUE;
        } else {
            Variant stopVariant = VariantPhoenixKeyFactory.extractVariantFromVariantRowKey(lastRow);
            stopChr = stopVariant.getChromosome();
            end = stopVariant.getStart();
        }

        regions = new ArrayList<>();
        if (stopChr.equals(startChr)) {
            regions.add(new Region(startChr, start, end));
        } else {
            for (int i = allChromosomes.indexOf(startChr); i <= allChromosomes.indexOf(stopChr); i++) {
                String chromosome = allChromosomes.get(i);
                regions.add(new Region(chromosome, chromosome.equals(startChr) ? start : 0,
                        chromosome.equals(stopChr) ? end : Integer.MAX_VALUE));
            }
        }
        iterator = sampleIndexDBAdaptor.iterator(regions, studyConfiguration.getStudyName(), samples, operation);
        loadMoreResults();
    }

    /**
     * Load a new batch of results.
     *
     * @throws IOException When reading the record failed.
     */
    protected void loadMoreResults() throws IOException {
        int batchSize = scan.getCaching();
        List<Get> gets = new ArrayList<>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            if (!iterator.hasNext()) {
                break;
            }
            Variant next = iterator.next();
            Get get = new Get(VariantPhoenixKeyFactory.generateVariantRowKey(next));
            get.setFilter(scan.getFilter());
            get.setTimeRange(scan.getTimeRange().getMin(), scan.getTimeRange().getMax());
            get.setMaxVersions(scan.getMaxVersions());
            for (Map.Entry<byte[], NavigableSet<byte[]>> entry : scan.getFamilyMap().entrySet()) {
                for (byte[] column : entry.getValue()) {
                    get.addColumn(entry.getKey(), column);
                }
            }
            gets.add(get);
        }
        if (gets.isEmpty()) {
            results = Collections.emptyList();
        } else {
            results = Arrays.asList(table.get(gets));
        }
        resultsIterator = results.listIterator();
    }

    /**
     * Positions the record reader to the next record.
     *
     * @return <code>true</code> if there was another record.
     * @throws IOException When reading the record failed.
     * @throws InterruptedException When the job was aborted.
     * @see org.apache.hadoop.mapreduce.RecordReader#nextKeyValue()
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (resultsIterator.hasNext()) {
            currentValue = resultsIterator.next();
            currentKey.set(currentValue.getRow());
        } else {
            loadMoreResults();
            if (!resultsIterator.hasNext()) {
                return false;
            } else {
                currentValue = resultsIterator.next();
                currentKey.set(currentValue.getRow());
            }
        }
        return true;
    }

    /**
     * The current progress of the record reader through its data.
     *
     * @return A number between 0.0 and 1.0, the fraction of the data read.
     * @see org.apache.hadoop.mapreduce.RecordReader#getProgress()
     */
    @Override
    public float getProgress() {
        return 0;
    }
}
