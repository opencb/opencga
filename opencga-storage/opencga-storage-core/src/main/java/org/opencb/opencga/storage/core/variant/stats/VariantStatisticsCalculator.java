package org.opencb.opencga.storage.core.variant.stats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jmmut on 28/01/15.
 */
public class VariantStatisticsCalculator {

    private static final String BATCH_SIZE = "batchSize";
    private ObjectMapper jsonObjectMapper;
    protected static Logger logger = LoggerFactory.getLogger(VariantStatisticsCalculator.class);

    public VariantStatisticsCalculator() {
        this.jsonObjectMapper = new ObjectMapper(new JsonFactory());
    }

    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, QueryOptions options) throws IOException {

        /** Open output stream **/

        OutputStream outputStream;
        Path filePath = Paths.get(output);
        outputStream = new FileOutputStream(filePath.toFile());
        logger.info("writing stats to {}", filePath);
        if(options != null && options.getBoolean("gzip", true)) {
            outputStream = new GZIPOutputStream(outputStream);
        }

        /** Initialize Json serializer**/
        ObjectWriter writer = jsonObjectMapper.writerWithType(VariantStats.class);

        /** Getting iterator from OpenCGA Variant database. **/
        QueryOptions iteratorQueryOptions = new QueryOptions();

//        int batchSize = 100;  // future optimization, threads, etc

        if(options != null) { //Parse query options
            iteratorQueryOptions = options;
//            batchSize = options.getInt(BATCH_SIZE, batchSize);
        }

        logger.info("starting stats calculation");
        long start = System.currentTimeMillis();

        Iterator<Variant> iterator = variantDBAdaptor.iterator(iteratorQueryOptions);
        while(iterator.hasNext()) {
            Variant variant = iterator.next();
            for (VariantSourceEntry file : variant.getSourceEntries().values()) {
                VariantStats variantStats = new VariantStats(variant);
                file.setStats(variantStats.calculate(file.getSamplesData(), file.getAttributes(), null));
                outputStream.write(writer.writeValueAsString(variantStats).getBytes());
            }
        }
        logger.info("finishing stats calculation, time: {}ms", System.currentTimeMillis() - start);

        outputStream.close();
        return output;
    }

    public void loadStats(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException {

    }
}
