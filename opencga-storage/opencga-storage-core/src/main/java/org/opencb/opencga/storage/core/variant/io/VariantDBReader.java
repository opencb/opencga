package org.opencb.opencga.storage.core.variant.io;

import org.opencb.biodata.formats.variant.io.VariantReader;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.cellbase.mongodb.db.VariationMongoDBAdaptor;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Created by jmmut on 3/03/15.
 */
public class VariantDBReader implements VariantReader {
    private VariantSource variantSource;
    private VariantDBAdaptor variantDBAdaptor;
    private QueryOptions options;
    private VariantDBIterator iterator;
    protected static Logger logger = LoggerFactory.getLogger(VariantDBReader.class);

    public VariantDBReader(VariantSource variantSource, VariantDBAdaptor variantDBAdaptor, QueryOptions options) {
        this.variantSource = variantSource;
        this.variantDBAdaptor = variantDBAdaptor;
        this.options = options;
    }

    public VariantDBReader() {
    }

    @Override
    public List<String> getSampleNames() {
        return variantSource.getSamples();
    }

    @Override
    public String getHeader() {
        return null;
    }

    @Override
    public boolean open() {
        QueryOptions iteratorQueryOptions = new QueryOptions();
        if(options != null) { //Parse query options
            iteratorQueryOptions = options;
        }

        // TODO rethink this way to refer to the Variant fields (through DBObjectToVariantConverter)
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "sourceEntries");
        iteratorQueryOptions.add("include", include);

        iterator = variantDBAdaptor.iterator(iteratorQueryOptions);
        return iterator != null;
    }

    @Override
    public boolean close() {
        return false;
    }

    @Override
    public boolean pre() {
        return false;
    }

    @Override
    public boolean post() {
        return false;
    }

    @Override
    public List<Variant> read() {
        return read(1);
    }

    @Override
    public List<Variant> read(int batchSize) {

        long start = System.currentTimeMillis();
        List<Variant> variants = new ArrayList<>(batchSize);
        while (variants.size() < batchSize && iterator.hasNext()) {
            variants.add(iterator.next());
        }
        logger.info("another batch of {} elements read. time: {}ms", variants.size(), System.currentTimeMillis() - start);
        logger.debug("time splitted: fetch = {}ms, convert = {}ms", iterator.getTimeFetching(), iterator.getTimeConverting());
        
        iterator.setTimeConverting(0);
        iterator.setTimeFetching(0);
        
        return variants;
    }
}
