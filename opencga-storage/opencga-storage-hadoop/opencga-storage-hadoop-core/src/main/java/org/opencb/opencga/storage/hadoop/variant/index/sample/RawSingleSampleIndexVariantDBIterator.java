package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.Iterators;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.utils.iterators.CloseableIterator;
import org.opencb.opencga.storage.hadoop.variant.index.query.LocusQuery;
import org.opencb.opencga.storage.hadoop.variant.index.query.SingleSampleIndexQuery;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class RawSingleSampleIndexVariantDBIterator extends CloseableIterator<SampleVariantIndexEntry> {

    private final Iterator<SampleVariantIndexEntry> iterator;
    protected int count = 0;

    public RawSingleSampleIndexVariantDBIterator(Table table, SingleSampleIndexQuery query, SampleIndexSchema schema,
                                                 SampleIndexDBAdaptor dbAdaptor) {
        Collection<LocusQuery> locusQueries;
        if (CollectionUtils.isEmpty(query.getLocusQueries())) {
            // If no locusQueries are defined, get a list of one null element to initialize the stream.
            locusQueries = Collections.singletonList(null);
        } else {
            locusQueries = query.getLocusQueries();
        }

        Iterator<Iterator<SampleVariantIndexEntry>> iterators = locusQueries.stream()
                .map(locusQuery -> {
                    // One scan per locus query
                    Scan scan = dbAdaptor.parseIncludeAll(query, locusQuery);
                    HBaseToSampleIndexConverter converter = new HBaseToSampleIndexConverter(schema);
                    RawSampleIndexEntryFilter filter = new RawSampleIndexEntryFilter(query, locusQuery);
                    try {
                        ResultScanner scanner = table.getScanner(scan);
                        addCloseable(scanner);
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

