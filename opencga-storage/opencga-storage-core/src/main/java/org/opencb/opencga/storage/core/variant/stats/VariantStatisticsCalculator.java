package org.opencb.opencga.storage.core.variant.stats;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.VariantSourceEntry;
import org.opencb.biodata.models.variant.stats.VariantSourceStats;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.datastore.core.QueryOptions;
import org.opencb.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.variant.VariantStorageManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.json.VariantStatsJsonMixin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
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
    private String VARIANT_STATS_SUFFIX = ".variants.stats.json.gz";
    private String SOURCE_STATS_SUFFIX = ".source.stats.json.gz";

    public VariantStatisticsCalculator() {
        jsonFactory = new JsonFactory();
        jsonObjectMapper = new ObjectMapper(jsonFactory);
        jsonObjectMapper.addMixInAnnotations(VariantStats.class, VariantStatsJsonMixin.class);
    }

    public URI createStats(VariantDBAdaptor variantDBAdaptor, URI output, QueryOptions options) throws IOException {

        /** Open output streams **/
        OutputStream outputVariantsStream;
        Path fileVariantsPath = Paths.get(output.getPath() + VARIANT_STATS_SUFFIX);
        outputVariantsStream = new FileOutputStream(fileVariantsPath.toFile());
        logger.info("will write stats to {}", fileVariantsPath);
        if(options != null && options.getBoolean("gzip", true)) {
            outputVariantsStream = new GZIPOutputStream(outputVariantsStream);
        }

        OutputStream outputSourceStream;
        Path fileSourcePath = Paths.get(output.getPath() + SOURCE_STATS_SUFFIX);
        outputSourceStream = new FileOutputStream(fileSourcePath.toFile());
        logger.info("will write source stats to {}", fileSourcePath);
        if(options != null && options.getBoolean("gzip", true)) {
            outputSourceStream = new GZIPOutputStream(outputSourceStream);
        }

        /** Initialize Json serializer**/
        ObjectWriter variantsWriter = jsonObjectMapper.writerWithType(VariantStatsWrapper.class);
        ObjectWriter sourceWriter = jsonObjectMapper.writerWithType(VariantSourceStats.class);

        /** Getting iterator from OpenCGA Variant database. **/
        QueryOptions iteratorQueryOptions = new QueryOptions();

//        int batchSize = 100;  // future optimization, threads, etc

        if(options != null) { //Parse query options
            iteratorQueryOptions = options;
//            batchSize = options.getInt(BATCH_SIZE, batchSize);
        }

        // TODO rethink this way to refer to the Variant fields (through DBObjectToVariantConverter)
        List<String> include = Arrays.asList("chromosome", "start", "end", "alternative", "reference", "sourceEntries");
        iteratorQueryOptions.add("include", include);

        VariantSource variantSource = options.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);
        VariantSourceStats variantSourceStats = new VariantSourceStats(variantSource.getFileId(), variantSource.getStudyId());

        logger.info("starting stats calculation");
        long start = System.currentTimeMillis();

        Iterator<Variant> iterator = variantDBAdaptor.iterator(iteratorQueryOptions);
        int skippedFiles = 0;
        while(iterator.hasNext()) {
            Variant variant = iterator.next();
            VariantSourceEntry file = variant.getSourceEntry(variantSource.getFileId(), variantSource.getStudyId());
//            for (VariantSourceEntry file : variant.getSourceEntries().values()) {
            if (file == null) {
                skippedFiles++;
                continue;
            }
            VariantStats variantStats = new VariantStats(variant);
            file.setStats(variantStats.calculate(file.getSamplesData(), file.getAttributes(), null));
            VariantStatsWrapper variantStatsWrapper = new VariantStatsWrapper(variant.getChromosome(), variant.getStart(), variantStats);
            outputVariantsStream.write(variantsWriter.writeValueAsString(variantStatsWrapper).getBytes());


            variantSourceStats.updateFileStats(Collections.singletonList(variant));     //TODO test
            variantSourceStats.updateSampleStats(Collections.singletonList(variant), variantSource.getPedigree());  // TODO test
//            variantSource.setStats(variantSourceStats.getFileStats());

//            }
        }
        logger.info("finishing stats calculation, time: {}ms", System.currentTimeMillis() - start);
        if (skippedFiles != 0) {
            logger.warn("the sources in {} variants were not found, and therefore couldn't run its stats", skippedFiles);
            logger.info("note: maybe the file-id and study-id were not correct?");
        }
        outputSourceStream.write(sourceWriter.writeValueAsString(variantSourceStats).getBytes());
        outputVariantsStream.close();
        outputSourceStream.close();
        return output;
    }

    public void loadStats(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException {

        URI variantStatsUri = Paths.get(uri.getPath() + VARIANT_STATS_SUFFIX).toUri();
        URI sourceStatsUri = Paths.get(uri.getPath() + SOURCE_STATS_SUFFIX).toUri();

        logger.info("starting stats loading from {} and {}", variantStatsUri, sourceStatsUri);
        long start = System.currentTimeMillis();

        loadVariantStats(variantDBAdaptor, variantStatsUri, options);
        loadSourceStats(variantDBAdaptor, sourceStatsUri, options);

        logger.info("finishing stats loading, time: {}ms", System.currentTimeMillis() - start);
    }
    public void loadVariantStats(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException {

        /** Open input streams **/
        Path variantInput = Paths.get(uri.getPath());
        InputStream variantInputStream;
        variantInputStream = new FileInputStream(variantInput.toFile());
        variantInputStream = new GZIPInputStream(variantInputStream);

        /** Initialize Json parse **/
        JsonParser parser = jsonFactory.createParser(variantInputStream);

        int batchSize = options.getInt(VariantStatisticsCalculator.BATCH_SIZE, 1000);
        ArrayList<VariantStatsWrapper> statsBatch = new ArrayList<>(batchSize);
        int writes = 0;
        int variantsNumber = 0;

        while (parser.nextToken() != null) {
            variantsNumber++;
            statsBatch.add(parser.readValueAs(VariantStatsWrapper.class));

            if (statsBatch.size() == batchSize) {
                QueryResult writeResult = variantDBAdaptor.updateStats(statsBatch, options);
                writes += writeResult.getNumResults();
                logger.info("stats loaded up to position {}:{}", statsBatch.get(statsBatch.size()-1).getChromosome(), statsBatch.get(statsBatch.size()-1).getPosition());
                statsBatch.clear();
            }
        }

        if (!statsBatch.isEmpty()) {
            QueryResult writeResult = variantDBAdaptor.updateStats(statsBatch, options);
            writes += writeResult.getNumResults();
            logger.info("stats loaded up to position {}:{}", statsBatch.get(statsBatch.size()-1).getChromosome(), statsBatch.get(statsBatch.size()-1).getPosition());
            statsBatch.clear();
        }

        if (writes < variantsNumber) {
            logger.warn("provided statistics of {} variants, but only {} were updated", variantsNumber, writes);
            logger.info("note: maybe those variants didn't had the proper study?");
        }

    }
    public void loadSourceStats(VariantDBAdaptor variantDBAdaptor, URI uri, QueryOptions options) throws IOException {

        /** Open input streams **/
        Path sourceInput = Paths.get(uri.getPath());
        InputStream sourceInputStream;
        sourceInputStream = new FileInputStream(sourceInput.toFile());
        sourceInputStream = new GZIPInputStream(sourceInputStream);

        /** Initialize Json parse **/
        JsonParser sourceParser = jsonFactory.createParser(sourceInputStream);

        VariantSourceStats variantSourceStats;
//        if (sourceParser.nextToken() != null) {
            variantSourceStats = sourceParser.readValueAs(VariantSourceStats.class);
//        }
        VariantSource variantSource = options.get(VariantStorageManager.VARIANT_SOURCE, VariantSource.class);   // needed?

        // TODO if variantSourceStats doesn't have studyId and fileId, create another with variantSource.getStudyId() and variantSource.getFileId()
        variantDBAdaptor.getVariantSourceDBAdaptor().updateSourceStats(variantSourceStats, options);

//        DBObject studyMongo = sourceConverter.convertToStorageType(source);
//        DBObject query = new BasicDBObject(DBObjectToVariantSourceConverter.FILEID_FIELD, source.getFileName());
//        WriteResult wr = filesCollection.update(query, studyMongo, true, false);
    }
}
