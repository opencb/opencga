package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.Iterators;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.query.SingleSampleIndexQuery;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class RawSingleSampleIndexVariantDBIterator implements Iterator<SampleVariantIndexEntry> {

    private final Iterator<SampleVariantIndexEntry> iterator;
    protected int count = 0;

    public RawSingleSampleIndexVariantDBIterator(Table table, SingleSampleIndexQuery query, SampleIndexDBAdaptor dbAdaptor) {
        List<Region> regions;
        if (CollectionUtils.isEmpty(query.getRegions())) {
            // If no regions are defined, get a list of one null element to initialize the stream.
            regions = Collections.singletonList(null);
        } else {
            regions = VariantQueryUtils.mergeRegions(query.getRegions());
        }

        Iterator<Iterator<SampleVariantIndexEntry>> iterators = regions.stream()
                .map(region -> {
                    // One scan per region
                    Scan scan = dbAdaptor.parseIncludeAll(query, region);
                    HBaseToSampleIndexConverter converter = new HBaseToSampleIndexConverter(dbAdaptor.getConfiguration());
                    RawSampleIndexEntryFilter filter = new RawSampleIndexEntryFilter(query, region);
                    try {
                        ResultScanner scanner = table.getScanner(scan);
//                        addCloseable(scanner);
                        Iterator<Result> resultIterator = scanner.iterator();
                        Iterator<Iterator<SampleVariantIndexEntry>> transform = Iterators.transform(resultIterator,
                                result -> {
                                    SampleIndexEntry sampleIndexEntry = converter.convert(result);
                                    return filter.filter(sampleIndexEntry).iterator();
                                });
                        return Iterators.concat(transform);
                    } catch (IOException e) {
                        throw VariantQueryException.internalException(e);
                    }
                }).iterator();
        iterator = Iterators.concat(iterators);
    }

    private RawSingleSampleIndexVariantDBIterator(Iterator<SampleVariantIndexEntry> iterator) {
        this.iterator = iterator;
    }

    public static RawSingleSampleIndexVariantDBIterator emptyIterator() {
        return new RawSingleSampleIndexVariantDBIterator(Collections.emptyIterator());
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public SampleVariantIndexEntry next() {
        return iterator.next();
    }
}

