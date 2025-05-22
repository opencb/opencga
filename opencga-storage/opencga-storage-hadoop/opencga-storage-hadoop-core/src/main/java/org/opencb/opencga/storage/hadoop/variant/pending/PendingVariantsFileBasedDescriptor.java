package org.opencb.opencga.storage.hadoop.variant.pending;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.VariantStorageMetadataManager;
import org.opencb.opencga.storage.hadoop.variant.mr.VariantLocusKey;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Created on 13/02/19.
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public interface PendingVariantsFileBasedDescriptor extends PendingVariantsDescriptor<Variant> {

    String name();

    /**
     * Configure the scan to read from the variants table.
     *
     * @param scan Scan to configure
     * @param metadataManager Metadata manager
     * @return The same scan object
     */
    Scan configureScan(Scan scan, VariantStorageMetadataManager metadataManager);

    Function<Result, Variant> getPendingEvaluatorMapper(VariantStorageMetadataManager metadataManager, boolean overwrite);

    @Override
    default PendingVariantsDescriptor.Type getType() {
        return PendingVariantsDescriptor.Type.FILE;
    }

    int getFileBatchSize();

    default List<String> buildFileName(String chromosome, int start, int end) {
        List<String> fileNames = new ArrayList<>();
        fileNames.add(buildFileName(chromosome, start));
        start += 1_000_000;
        while (start < end) {
            fileNames.add(buildFileName(chromosome, start));
            start += 1_000_000;
        }
        return fileNames;
    }

    default String buildFileName(String chromosome, int position) {
        int batchSize = getFileBatchSize();
        // Round the position to the nearest batch
        position = (position / batchSize) * batchSize;

        String encodedChr;
        try {
            encodedChr = URLEncoder.encode(chromosome, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // This should never happen
            throw new RuntimeException(e);
        }
        return String.format("%s.%s.%010d.json.gz", name(), encodedChr, position);
    }

    default VariantLocusKey getLocusFromFileName(String name) {
        String[] split = name.split("\\.");
        String encodedChr = split[1];
        String chromosome;
        try {
            chromosome = URLDecoder.decode(encodedChr, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            // This should never happen
            throw new RuntimeException(e);
        }
        int position = Integer.parseInt(split[2]);
        VariantLocusKey locusKey = new VariantLocusKey(chromosome, position);
        return locusKey;
    }

    default boolean isPendingVariantsFile(String name) {
        return name.startsWith(name() + ".");
    }
}
