package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.Iterators;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.GenotypeClass;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryUtils.QueryOperation;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.IntersectMultiVariantKeyIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.UnionMultiVariantKeyIterator;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.IndexUtils;
import org.opencb.opencga.storage.hadoop.variant.index.sample.SampleIndexQuery.SingleSampleIndexQuery;
import org.opencb.opencga.storage.hadoop.variant.utils.HBaseVariantTableNameGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.adaptors.phoenix.VariantSqlQueryParser.DEFAULT_LOADED_GENOTYPES;
import static org.opencb.opencga.storage.hadoop.variant.index.IndexUtils.EMPTY_MASK;

/**
 * Created on 14/05/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SampleIndexDBAdaptor {

    private final HBaseManager hBaseManager;
    private final HBaseVariantTableNameGenerator tableNameGenerator;
    private final VariantStorageMetadataManager metadataManager;
    private final byte[] family;
    private static Logger logger = LoggerFactory.getLogger(SampleIndexDBAdaptor.class);

    public SampleIndexDBAdaptor(GenomeHelper helper, HBaseManager hBaseManager, HBaseVariantTableNameGenerator tableNameGenerator,
                                VariantStorageMetadataManager metadataManager) {
        this.hBaseManager = hBaseManager;
        this.tableNameGenerator = tableNameGenerator;
        this.metadataManager = metadataManager;
        family = helper.getColumnFamily();
    }

    public VariantDBIterator iterator(SampleIndexQuery query) {
        String study = query.getStudy();
        Map<String, List<String>> samples = query.getSamplesMap();

        if (samples.isEmpty()) {
            throw new VariantQueryException("At least one sample expected to query SampleIndex!");
        }
        List<String> allGts = getAllLoadedGenotypes(study);
        QueryOperation operation = query.getQueryOperation();

        if (samples.size() == 1) {
            String sample = samples.entrySet().iterator().next().getKey();
            List<String> gts = query.getSamplesMap().get(sample);
            List<String> filteredGts = GenotypeClass.filter(gts, allGts);

            if (!gts.isEmpty() && filteredGts.isEmpty()) {
                // If empty, should find none. Return empty iterator
                return VariantDBIterator.emptyIterator();
            } else {
                return internalIterator(query.forSample(sample, filteredGts));
            }
        }

        List<VariantDBIterator> iterators = new ArrayList<>(samples.size());
        List<VariantDBIterator> negatedIterators = new ArrayList<>(samples.size());

        for (Map.Entry<String, List<String>> entry : samples.entrySet()) {
            String sample = entry.getKey();
            List<String> gts = GenotypeClass.filter(entry.getValue(), allGts);
            if (!entry.getValue().isEmpty() && gts.isEmpty()) {
                // If empty, should find none. Add empty iterator for this sample
                iterators.add(VariantDBIterator.emptyIterator());
            } else if (gts.stream().allMatch(SampleIndexDBLoader::validGenotype)) {
                iterators.add(internalIterator(query.forSample(sample, gts)));
            } else {
                if (operation.equals(QueryOperation.OR)) {
                    throw new IllegalArgumentException("Unable to query by REF or MISS genotypes!");
                }
                List<String> queryGts = new ArrayList<>(allGts);
                queryGts.removeAll(gts);

                // Skip if GTs to query is empty!
                // Otherwise, it will return ALL genotypes instead of none
                if (!queryGts.isEmpty()) {
                    negatedIterators.add(internalIterator(query.forSample(sample, queryGts)));
                }
            }
        }
        if (operation.equals(QueryOperation.OR)) {
            logger.info("Union of " + iterators.size() + " sample indexes");
            return new UnionMultiVariantKeyIterator(iterators);
        } else {
            logger.info("Intersection of " + iterators.size() + " sample indexes plus " + negatedIterators.size() + " negated indexes");
            return new IntersectMultiVariantKeyIterator(iterators, negatedIterators);
        }

    }

    /**
     * Partially processed iterator. Internal usage only.
     *
     * @param query SingleSampleIndexQuery
     * @return SingleSampleIndexVariantDBIterator
     */
    private SingleSampleIndexVariantDBIterator internalIterator(SingleSampleIndexQuery query) {
        String tableName = tableNameGenerator.getSampleIndexTableName(toStudyId(query.getStudy()));

        try {
            return hBaseManager.act(tableName, table -> {
                return new SingleSampleIndexVariantDBIterator(table, query, family, this);
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected Map<String, List<Variant>> rawQuery(int study, int sample, String chromosome, int position) throws IOException {
        String tableName = tableNameGenerator.getSampleIndexTableName(study);

        return hBaseManager.act(tableName, table -> {
            Get get = new Get(HBaseToSampleIndexConverter.toRowKey(sample, chromosome, position));
            HBaseToSampleIndexConverter converter = new HBaseToSampleIndexConverter(family);
            try {
                Result result = table.get(get);
                if (result != null) {
                    return converter.convertToMap(result);
                } else {
                    return Collections.emptyMap();
                }
            } catch (IOException e) {
                throw VariantQueryException.internalException(e);
            }
        });
    }

    public Iterator<Map<String, List<Variant>>> rawIterator(int study, int sample) throws IOException {
        String tableName = tableNameGenerator.getSampleIndexTableName(study);

        return hBaseManager.act(tableName, table -> {

            Scan scan = new Scan();
            scan.setRowPrefixFilter(HBaseToSampleIndexConverter.toRowKey(sample));
            HBaseToSampleIndexConverter converter = new HBaseToSampleIndexConverter(family);
            try {
                ResultScanner scanner = table.getScanner(scan);
                Iterator<Result> resultIterator = scanner.iterator();
                Iterator<Map<String, List<Variant>>> iterator = Iterators.transform(resultIterator, converter::convertToMap);
                return iterator;
            } catch (IOException e) {
                throw VariantQueryException.internalException(e);
            }
        });
    }

    public boolean isFastCount(SampleIndexQuery query) {
        return query.getSamplesMap().size() == 1 && query.emptyAnnotationIndex() && query.emptyFileIndex();
    }

    public long count(List<Region> regions, String study, String sample, List<String> gts) {
        return count(new SampleIndexQuery(regions, study, Collections.singletonMap(sample, gts), null), sample);
    }

    public long count(SampleIndexQuery query, String sample) {
        List<Region> regionsList;
        if (CollectionUtils.isEmpty(query.getRegions())) {
            // If no regions are defined, get a list of one null element to initialize the stream.
            regionsList = Collections.singletonList(null);
        } else {
            regionsList = VariantQueryUtils.mergeRegions(query.getRegions());
        }

        List<String> allGts = getAllLoadedGenotypes(query.getStudy());
        List<String> gts = query.getSamplesMap().get(sample);
        if (CollectionUtils.isEmpty(gts)) {
            gts = allGts;
        } else {
            gts = GenotypeClass.filter(gts, allGts);
        }

        String tableName = tableNameGenerator.getSampleIndexTableName(toStudyId(query.getStudy()));

        List<String> finalGts = gts;
        try {
            return hBaseManager.act(tableName, table -> {
                long count = 0;
                for (Region region : regionsList) {
                    // Split region in countable regions
                    List<Region> subRegions = region == null ? Collections.singletonList((Region) null) : splitRegion(region);
                    for (Region subRegion : subRegions) {
                        SingleSampleIndexQuery sampleIndexQuery = query.forSample(sample, finalGts);
                        HBaseToSampleIndexConverter converter = new HBaseToSampleIndexConverter(sampleIndexQuery, subRegion, family);
                        if (query.emptyAnnotationIndex() && query.emptyFileIndex()
                                && (query.getVariantTypes() == null || query.getVariantTypes().size() == VariantType.values().length)
                                && (subRegion == null || startsAtBatch(subRegion) && endsAtBatch(subRegion))) {
                            Scan scan = parse(sampleIndexQuery, subRegion, true);
                            try {
                                ResultScanner scanner = table.getScanner(scan);
                                Result result = scanner.next();
                                while (result != null) {
                                    count += converter.convertToCount(result);
                                    result = scanner.next();
                                }
                            } catch (IOException e) {
                                throw VariantQueryException.internalException(e);
                            }
                        } else {
                            Scan scan = parse(sampleIndexQuery, subRegion, false);
                            try {
                                ResultScanner scanner = table.getScanner(scan);
                                Result result = scanner.next();
                                while (result != null) {
                                    count += converter.convert(result).size();
                                    result = scanner.next();
                                }
                            } catch (IOException e) {
                                throw VariantQueryException.internalException(e);
                            }
                        }
                    }
                }
                return count;
            });
        } catch (IOException e) {
            throw VariantQueryException.internalException(e);
        }
    }

    protected int toStudyId(String study) {
        int studyId;
        if (StringUtils.isEmpty(study)) {
            Map<String, Integer> studies = metadataManager.getStudies(null);
            if (studies.size() == 1) {
                studyId = studies.values().iterator().next();
            } else {
                throw VariantQueryException.studyNotFound(study, studies.keySet());
            }
        } else {
            studyId = metadataManager.getStudyId(study);
        }
        return studyId;
    }

    protected List<String> getAllLoadedGenotypes(String study) {
        List<String> allGts = metadataManager.getStudyMetadata(study)
                .getAttributes()
                .getAsStringList(VariantStorageEngine.Options.LOADED_GENOTYPES.key());
        if (allGts == null || allGts.isEmpty()) {
            allGts = DEFAULT_LOADED_GENOTYPES;
        }
        return allGts;
    }

    /**
     * Split region into regions that match with batches at SampleIndexTable.
     *
     * @param region Region to split
     * @return List of regions.
     */
    protected static List<Region> splitRegion(Region region) {
        List<Region> regions;
        if (region.getEnd() - region.getStart() < SampleIndexDBLoader.BATCH_SIZE) {
            // Less than one batch. Do not split region
            regions = Collections.singletonList(region);
        } else if (region.getStart() / SampleIndexDBLoader.BATCH_SIZE + 1 == region.getEnd() / SampleIndexDBLoader.BATCH_SIZE
                && !startsAtBatch(region)
                && !endsAtBatch(region)) {
            // Consecutive partial batches. Do not split region
            regions = Collections.singletonList(region);
        } else {
            regions = new ArrayList<>(3);
            if (!startsAtBatch(region)) {
                int splitPoint = region.getStart() - region.getStart() % SampleIndexDBLoader.BATCH_SIZE + SampleIndexDBLoader.BATCH_SIZE;
                regions.add(new Region(region.getChromosome(), region.getStart(), splitPoint - 1));
                region.setStart(splitPoint);
            }
            regions.add(region);
            if (!endsAtBatch(region)) {
                int splitPoint = region.getEnd() - region.getEnd() % SampleIndexDBLoader.BATCH_SIZE;
                regions.add(new Region(region.getChromosome(), splitPoint, region.getEnd()));
                region.setEnd(splitPoint - 1);
            }
        }
        return regions;
    }

    protected static boolean startsAtBatch(Region region) {
        return region.getStart() % SampleIndexDBLoader.BATCH_SIZE == 0;
    }

    protected static boolean endsAtBatch(Region region) {
        return region.getEnd() + 1 % SampleIndexDBLoader.BATCH_SIZE == 0;
    }

    public Scan parse(SingleSampleIndexQuery query, Region region, boolean count) {

        Scan scan = new Scan();
        int studyId = toStudyId(query.getStudy());
        int sampleId = toSampleId(studyId, query.getSample());
        if (region != null) {
            scan.setStartRow(HBaseToSampleIndexConverter.toRowKey(sampleId, region.getChromosome(), region.getStart()));
            scan.setStopRow(HBaseToSampleIndexConverter.toRowKey(sampleId, region.getChromosome(),
                    region.getEnd() + (region.getEnd() == Integer.MAX_VALUE ? 0 : SampleIndexDBLoader.BATCH_SIZE)));
        } else {
            scan.setStartRow(HBaseToSampleIndexConverter.toRowKey(sampleId));
            scan.setStopRow(HBaseToSampleIndexConverter.toRowKey(sampleId + 1));
        }
        // If genotypes are not defined, return ALL columns
        for (String gt : query.getGenotypes()) {
            if (count) {
                scan.addColumn(family, HBaseToSampleIndexConverter.toGenotypeCountColumn(gt));
            } else {
                if (query.getMendelianError()) {
                    scan.addColumn(family, HBaseToSampleIndexConverter.toMendelianErrorColumn());
                } else {
                    scan.addColumn(family, HBaseToSampleIndexConverter.toGenotypeColumn(gt));
                }
                if (query.getAnnotationIndexMask() != EMPTY_MASK) {
                    scan.addColumn(family, HBaseToSampleIndexConverter.toAnnotationIndexColumn(gt));
                }
                if (query.getFileIndexMask() != EMPTY_MASK) {
                    scan.addColumn(family, HBaseToSampleIndexConverter.toFileIndexColumn(gt));
                }
            }
        }


        logger.info("StartRow = " + Bytes.toStringBinary(scan.getStartRow()) + " == "
                + HBaseToSampleIndexConverter.rowKeyToString(scan.getStartRow()));
        logger.info("StopRow = " + Bytes.toStringBinary(scan.getStopRow()) + " == "
                + HBaseToSampleIndexConverter.rowKeyToString(scan.getStopRow()));
        logger.info("columns = " + scan.getFamilyMap().getOrDefault(family, Collections.emptyNavigableSet())
                .stream().map(Bytes::toString).collect(Collectors.joining(",")));
        logger.info("MaxResultSize = " + scan.getMaxResultSize());
//        logger.info("Filters = " + scan.getFilter());
//        logger.info("Batch = " + scan.getBatch());
//        logger.info("Caching = " + scan.getCaching());
        logger.info("AnnotationIndex = " + IndexUtils.maskToString(query.getAnnotationIndexMask(), (byte) 0xFF));
        logger.info("FileIndex       = " + IndexUtils.maskToString(query.getFileIndexMask(), query.getFileIndex()));

//        try {
//            System.out.println("scan = " + scan.toJSON() + " " + rowKeyToString(scan.getStartRow()) + " -> + "
// + rowKeyToString(scan.getStopRow()));
//        } catch (IOException e) {
//            throw VariantQueryException.internalException(e);
//        }

        return scan;
    }

    private int toSampleId(int studyId, String sample) {
        return metadataManager.getSampleId(studyId, sample);
    }


}
