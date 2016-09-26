package org.opencb.opencga.storage.hadoop.variant.index;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created on 23/11/15.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class VariantHBaseScanIterator extends VariantDBIterator {

    private final Logger logger = LoggerFactory.getLogger(VariantHBaseScanIterator.class);
    private final ResultScanner resultScanner;
    private final GenomeHelper genomeHelper;
    private final Iterator<Result> iterator;
    private final HBaseToVariantConverter converter;
    private long limit = Long.MAX_VALUE;
    private long count = 0;

    public VariantHBaseScanIterator(ResultScanner resultScanner, VariantTableHelper variantTableHelper, QueryOptions options)
            throws IOException {
        this.resultScanner = resultScanner;
        this.genomeHelper = variantTableHelper;
        iterator = resultScanner.iterator();
        converter = new HBaseToVariantConverter(variantTableHelper);
        setLimit(options.getLong("limit"));
    }

    public VariantHBaseScanIterator(ResultScanner resultScanner, GenomeHelper genomeHelper, StudyConfigurationManager scm,
                                    QueryOptions options) throws IOException {
        this.resultScanner = resultScanner;
        this.genomeHelper = genomeHelper;
        iterator = resultScanner.iterator();
        converter = new HBaseToVariantConverter(genomeHelper, scm)
                .setMutableSamplesPosition(false)
                .setStudyNameAsStudyId(true);
        setLimit(options.getLong("limit"));
    }

    @Override
    public boolean hasNext() {
        return count < limit && fetch(iterator::hasNext);
    }

    @Override
    public Variant next() {
        if (count >= limit) {
            throw new NoSuchElementException("Limit reached");
        }
        count++;
        Result next = fetch(iterator::next);
        return convert(() -> converter.convert(next));
    }

    @Override
    public void close() {
        logger.debug("Close variant iterator. Fetch = {}ms, Convert = {}ms",
                getTimeFetching() / 1000000.0, getTimeConverting() / 1000000.0);
        resultScanner.close();
    }

    public long getLimit() {
        return limit;
    }

    protected void setLimit(long limit) {
        this.limit = limit <= 0 ? Long.MAX_VALUE : limit;
    }
}
