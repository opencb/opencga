package org.opencb.opencga.storage.hadoop.variant.index.sample;

import com.google.common.collect.Iterators;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.CollectionUtils;
import org.opencb.biodata.models.core.Region;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.variant.adaptors.iterators.VariantDBIterator;
import org.opencb.opencga.storage.core.variant.adaptors.VariantQueryException;
import org.opencb.opencga.storage.core.variant.query.VariantQueryUtils;
import org.opencb.opencga.storage.hadoop.variant.index.query.SingleSampleIndexQuery;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Created on 03/07/18.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class SingleSampleIndexVariantDBIterator extends VariantDBIterator {

    private final Iterator<Variant> iterator;
    protected int count = 0;

    public SingleSampleIndexVariantDBIterator(Table table, SingleSampleIndexQuery query, byte[] family, SampleIndexDBAdaptor dbAdaptor) {
        List<Region> regions;
        if (CollectionUtils.isEmpty(query.getRegions())) {
            // If no regions are defined, get a list of one null element to initialize the stream.
            regions = Collections.singletonList(null);
        } else {
            regions = VariantQueryUtils.mergeRegions(query.getRegions());
        }

        Iterator<Iterator<Variant>> iterators = regions.stream()
                .map(region -> {
                    // One scan per region
                    Scan scan = dbAdaptor.parse(query, region);
                    HBaseToSampleIndexConverter converter = new HBaseToSampleIndexConverter(dbAdaptor.getConfiguration());
                    SampleIndexEntryFilter filter = dbAdaptor.buildSampleIndexEntryFilter(query, region);
                    try {
                        ResultScanner scanner = table.getScanner(scan);
                        addCloseable(scanner);
                        Iterator<Result> resultIterator = scanner.iterator();
                        Iterator<Iterator<Variant>> transform = Iterators.transform(resultIterator,
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

    @Override
    public int getCount() {
        return count;
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

}
