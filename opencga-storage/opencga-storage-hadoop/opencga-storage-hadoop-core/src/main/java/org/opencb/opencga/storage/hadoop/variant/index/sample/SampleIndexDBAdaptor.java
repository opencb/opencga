package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created on 14/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexDBAdaptor {


    private final HBaseManager hBaseManager;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final StudyConfigurationManager scm;
    private final byte[] family;
    private static Logger logger = LoggerFactory.getLogger(SampleIndexDBAdaptor.class);

    public SampleIndexDBAdaptor(GenomeHelper helper, HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                StudyConfigurationManager scm) {
        this.hBaseManager = hBaseManager;
        this.tableNameGenerator = tableNameGenerator;
        this.scm = scm;
        family = helper.getColumnFamily();
    }

    public SampleIndexVariantDBIterator get(List<Region> regions, String study, String sample, List<String> gts) {

        Integer studyId;
        if (StringUtils.isEmpty(study)) {
            Map<String, Integer> studies = scm.getStudies(null);
            if (studies.size() == 1) {
                studyId = studies.values().iterator().next();
            } else {
                throw VariantQueryException.studyNotFound(study, studies.keySet());
            }
        } else {
            studyId = scm.getStudyId(study, null);
        }

        String tableName = tableNameGenerator.getSampleIndexTableName(studyId);

        try {
            return hBaseManager.act(tableName, table -> {
                return new SampleIndexVariantDBIterator(table, regions, studyId, sample, gts);
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    public class SampleIndexVariantDBIterator extends VariantDBIterator {

        private final Iterator<Variant> iterator;
        private int count = 0;

        SampleIndexVariantDBIterator(Table table, List<Region> regions, Integer studyId, String sample, List<String> gts) {
            if (CollectionUtils.isEmpty(regions)) {
                // If no regions are defined, get a list of one null element to initialize the stream.
                regions = Collections.singletonList(null);
            } else {
                regions = VariantQueryUtils.mergeRegions(regions);
            }

            Iterator<Iterator<Variant>> iterators = regions.stream()
                    .map(region -> {
                        // One scan per region
                        Scan scan = parse(region, studyId, sample, gts);
                        SampleIndexConverter converter = new SampleIndexConverter(region);
                        try {
                            ResultScanner scanner = table.getScanner(scan);
                            addCloseable(scanner);
                            Iterator<Result> resultIterator = scanner.iterator();
                            Iterator<Iterator<Variant>> transform = Iterators.transform(resultIterator,
                                    result -> converter.convert(result).iterator());
                            return Iterators.concat(transform);
                        } catch (IOException e) {
                            throw VariantQueryException.internalException(e);
                        }
                    }).iterator();
            iterator = Iterators.concat(iterators);
        }

        @Override
        public boolean hasNext() {
            return fetch(iterator::hasNext);
        }

        @Override
        public Variant next() {
            Variant variant = fetch(iterator::next);
            count++;
            return variant;
        }

        public int getCount() {
            return count;
        }
    }

    private Scan parse(Region region, int study, String sample, List<String> gts) {

        Scan scan = new Scan();
        int sampleId = toSampleId(study, sample);
        if (region != null) {
            scan.setStartRow(SampleIndexConverter.toRowKey(sampleId, region.getChromosome(), region.getStart()));
            scan.setStopRow(SampleIndexConverter.toRowKey(sampleId, region.getChromosome(),
                    region.getEnd() + (region.getEnd() == Integer.MAX_VALUE ? 0 : SampleIndexDBLoader.BATCH_SIZE)));
        } else {
            scan.setRowPrefixFilter(SampleIndexConverter.toRowKey(sampleId));
        }
        for (String gt : gts) {
            scan.addColumn(family, Bytes.toBytes(gt));
        }


        logger.info("StartRow = " + Bytes.toStringBinary(scan.getStartRow()) + " == "
                + SampleIndexConverter.rowKeyToString(scan.getStartRow()));
        logger.info("StopRow = " + Bytes.toStringBinary(scan.getStopRow()) + " == "
                + SampleIndexConverter.rowKeyToString(scan.getStopRow()));
        logger.info("columns = " + scan.getFamilyMap().getOrDefault(family, Collections.emptyNavigableSet())
                .stream().map(Bytes::toString).collect(Collectors.joining(",")));
        logger.info("MaxResultSize = " + scan.getMaxResultSize());
        logger.info("Filters = " + scan.getFilter());
        logger.info("Batch = " + scan.getBatch());
        logger.info("Caching = " + scan.getCaching());

//        try {
//            System.out.println("scan = " + scan.toJSON() + " " + rowKeyToString(scan.getStartRow()) + " -> + "
// + rowKeyToString(scan.getStopRow()));
//        } catch (IOException e) {
//            throw VariantQueryException.internalException(e);
//        }

        return scan;
    }


    private int toSampleId(int studyId, String sample) {
        StudyConfiguration sc = scm.getStudyConfiguration(studyId, new QueryOptions(StudyConfigurationManager.READ_ONLY, true)
                .append(StudyConfigurationManager.CACHED, true)).first();
        if (sc == null) {
            throw VariantQueryException.studyNotFound(studyId, scm.getStudies(null).keySet());
        }
        return scm.getSampleId(sample, sc);
    }


}
