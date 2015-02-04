package org.opencb.opencga.storage.core.variant.stats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Iterator;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Created by jmmut on 28/01/15.
 */
public class VariantStatisticsCalculator {

    private static final String BATCH_SIZE = "batchSize";
    private final JsonFactory jsonFactory;
    private ObjectMapper jsonObjectMapper;
    protected static Logger logger = LoggerFactory.getLogger(VariantStatisticsCalculator.class);

    public VariantStatisticsCalculator() {
        jsonFactory = new JsonFactory();
        this.jsonObjectMapper = new ObjectMapper(jsonFactory);

        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);

    }

    public URI createStats(VariantDBAdaptor variantDBAdaptor, @NotNull URI output, QueryOptions options) throws IOException {

        /** Open output stream **/
        OutputStream outputStream;
        Path filePath = Paths.get(output.getPath() + ".stats.json.gz");
        outputStream = new FileOutputStream(filePath.toFile());
        logger.info("writing stats to {}", filePath);
        if(options != null && options.getBoolean("gzip", true)) {
            outputStream = new GZIPOutputStream(outputStream);
        }

        /** Initialize Json serializer**/
        ObjectWriter writer = jsonObjectMapper.writerWithType(VariantStatsWrapper.class);

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
                VariantStatsWrapper variantStatsWrapper = new VariantStatsWrapper(variant.getChromosome(), variant.getStart(), variantStats);
                outputStream.write(writer.writeValueAsString(variantStatsWrapper).getBytes());
            }
        }
        logger.info("finishing stats calculation, time: {}ms", System.currentTimeMillis() - start);

        outputStream.close();
        return filePath.toUri();
    }

    public void loadStats(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException {


        /** Open input stream **/
        Path input = Paths.get(uri.getPath());
        InputStream inputStream;
        inputStream = new FileInputStream(input.toFile());
        inputStream = new GZIPInputStream(inputStream);

        /** Initialize Json parse **/
        JsonParser parser = jsonFactory.createParser(inputStream);

        while (parser.nextToken() != null) {
            VariantStatsWrapper variantStatsWrapper = parser.readValueAs(VariantStatsWrapper.class);

            VariantSource variantSource = options.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);

            variantDBAdaptor.updateStats(Collections.singletonList(variantStatsWrapper), options);

        }
    }
}
